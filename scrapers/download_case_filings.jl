using DataFrames, ProgressMeter, Dates, DotEnv, SQLite, DBInterface

DotEnv.load!()
# SQLite DB location (shared with other tools)
const DB_PATH = normpath(joinpath(@__DIR__, "..", "web", "foreclosures", "foreclosures.sqlite"))
const CASE_LOG_PATH = normpath(joinpath(@__DIR__, "..", "web", "foreclosures", "cases.log"))
db() = SQLite.DB(DB_PATH)

# Optional case number to resume from when no CLI argument is provided.
# Set to a string like "513094/2019" to always resume there by default.
const DEFAULT_RESUME_CASE = nothing

function requested_resume_case()
    if length(ARGS) >= 1
        arg = strip(String(ARGS[1]))
        if !isempty(arg)
            return arg
        end
    end
    return DEFAULT_RESUME_CASE
end

# Parse many common date representations into Date; return missing if unknown.
function todate(x)
    x === missing && return missing
    x isa Date && return x
    x isa DateTime && return Date(x)
    if x isa AbstractString
        # Try common formats and fallbacks
        for f in (nothing, dateformat"yyyy-mm-dd", dateformat"mm/dd/yyyy", dateformat"yyyy/mm/dd",
                  dateformat"y-m-d", dateformat"y/m/d")
            try
                return isnothing(f) ? Date(x) : Date(x, f)
            catch
            end
        end
        # Fallback: try first 10 chars as ISO date (handles "yyyy-mm-dd HH:MM:SS" or "yyyy-mm-ddTHH:MM:SS")
        if length(x) >= 10
            s = String(x[1:10])
            try
                return Date(s, dateformat"yyyy-mm-dd")
            catch
            end
        end
    end
    return missing
end

function discontinued_cases()
    if !isfile(CASE_LOG_PATH)
        return Set{String}()
    end
    discontinued = Set{String}()
    for line in eachline(CASE_LOG_PATH)
        stripped = strip(line)
        isempty(stripped) && continue
        parts = split(stripped; limit=2)
        length(parts) < 2 && continue
        case_number = strip(parts[1])
        status = lowercase(strip(parts[2]))
        if status == "discontinued"
            push!(discontinued, case_number)
        end
    end
    return discontinued
end
# Get filings. If WSS is set then we are running locally, otherwise on git.
function main()
    
    resume_case = requested_resume_case()
    rows = get_data()
    rows = resume_from_case(rows, resume_case)

    # Filter rows where :missing_filings contains FilingType[:NOTICE_OF_SALE]
    urgent_mask = coalesce.(rows.auction_date .>= today(), false)
    urgent_rows = rows[(in.(FilingType[:NOTICE_OF_SALE], rows.missing_filings)) .& urgent_mask, :]
    urgent_row_count = nrow(urgent_rows)

    # Shuffle the remaining rows and select N at random
    sampled_rows = rows[.!in.(FilingType[:NOTICE_OF_SALE], rows.missing_filings), :]
    sampled_row_count = nrow(sampled_rows)
    println("Repo state: $urgent_row_count urgent cases, $sampled_row_count sampled rows outstanding")


    # Combine the filtered rows with the randomly selected rows
    rows = vcat(urgent_rows, sampled_rows)
    println("Task list: $(nrow(urgent_rows)) urgent cases, $(nrow(sampled_rows)) sampled rows, $(nrow(rows)) total tasks")
    process_data(rows)
end

# Define the FilingType as a constant dictionary
const FilingType = Dict(
    :NOTICE_OF_SALE =>  "noticeofsale",
    :SURPLUS_MONEY_FORM => "surplusmoney"
)

# Build expected filename for a given filing directory.
# For Notice of Sale, include the auction_date in the filename so that
# a date change will cause a new download to be required.
function expected_filename(dir::AbstractString, case_number::AbstractString, auction_date)
    base = replace(case_number, "/" => "-")
    if dir == FilingType[:NOTICE_OF_SALE]
        d = todate(auction_date)
        if d === missing
            # No date known; fall back to legacy naming
            return base * ".pdf"
        else
            # Store in date subfolder: YYYY-MM-DD/base.pdf
            return string(Dates.format(d, dateformat"yyyy-mm-dd"), "/", base, ".pdf")
        end
    else
        return base * ".pdf"
    end
end

# Function to find missing filings
function missing_filings(case_number, auction_date)
    auction_date = todate(auction_date)
    if auction_date !== missing && auction_date < today() - Day(30)
        return []
    end

    res = []
    for (key, dir) in FilingType
        filename = expected_filename(dir, case_number, auction_date)
        pdfPath = joinpath("web/saledocs", dir, filename)
        if !isfile(pdfPath)
            if dir == "noticeofsale"
                # if notice of sale, check path wihtout date
                filename = expected_filename(dir, case_number, missing)
                pdfPath = joinpath("web/saledocs", dir, filename)
                if !isfile(pdfPath)
                    push!(res, dir)
                end
            else
                # if surplus money form, always just check the one path
                push!(res, dir)
            end
        else
            
        end
    end
    

    # For auctions in the last 5 days, don't look for a surplus money form
    earliestDayForMoneyForm = today() - Day(1)

    # For auctions more than 35 days in the future, don't look for a notice of sale
    latestDayForNoticeOfSale = today() + Day(35)

    if auction_date !== missing && auction_date > earliestDayForMoneyForm
        # If auction date in the future, only get the notice of sale, otherwise get the surplus money form too
        res = filter(filing -> filing != FilingType[:SURPLUS_MONEY_FORM], res)
    end

    if auction_date !== missing && auction_date > latestDayForNoticeOfSale
        # If auction date too far in the future, don't look for a notice of sale
        res = filter(filing -> filing != FilingType[:NOTICE_OF_SALE], res)
    end

    return res
end

function get_data()
    # Read directly from SQLite cases table
    dbh = db()
    rows = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough, auction_date FROM cases"))
    SQLite.close(dbh)

    skipped_cases = discontinued_cases()
    if !isempty(skipped_cases)
        original = nrow(rows)
        filter!(row -> !(String(row.case_number) in skipped_cases), rows)
        removed = original - nrow(rows)
        removed > 0 && println("Skipping $removed discontinued cases from log")
    end

    # Parse auction_date text to Date where possible
	transform!(rows, :auction_date => ByRow(todate) => :auction_date)

    # Sort by borough priority, then by most recent auction date within each
    BOROUGH_PRIORITY = Dict(
        "Manhattan" => 1,
        "Brooklyn" => 2,
        "Queens" => 3,
        "Bronx" => 4,
        "Staten Island" => 5,
    )
    transform!(rows, :borough => ByRow(b -> get(BOROUGH_PRIORITY, String(b), 99)) => :borough_priority)
    sort!(rows, [order(:borough_priority), order(:auction_date, rev=true)])
    select!(rows, Not(:borough_priority))

    # Identify missing filings
    transform!(rows, [:case_number, :auction_date] => ByRow(missing_filings) => :missing_filings)
    filter!(row -> !isempty(row.missing_filings), rows)

    println(nrow(rows), " cases have missing filings")
    return rows
end

function resume_from_case(rows::DataFrame, resume_case)
    resume_case === nothing && return rows
    resume_case_str = String(resume_case)
    idx = findfirst(row -> String(row.case_number) == resume_case_str, eachrow(rows))
    if isnothing(idx)
        @warn "Resume case $(resume_case_str) not found in current task list; processing all cases"
        return rows
    end
    skipped = idx - 1
    total = nrow(rows)
    println("Resuming from case $(resume_case_str) ($idx of $total); skipping $skipped queued cases")
    return rows[idx:total, :]
end

const DEBUG = true
const MAX_CONCURRENCY = 1
function process_data(rows, max_concurrent_tasks=MAX_CONCURRENCY)
    tasks = Task[]
    # Use large-capacity channel to avoid blocking producers
    channel = Channel{Tuple{String, Int}}(max(1, nrow(rows)))
    failed_jobs = 0
    running_tasks = 0
    finished_tasks = 0
    # Track created docs per type
    nos_downloaded = 0
    smf_downloaded = 0
    # Cache expected filings metadata per case for quick lookups on completion
    meta = Dict{String, Tuple{Any, Vector{String}}}()

    !DEBUG && ( pb = Progress(nrow(rows)))

    total = nrow(rows)
    for (idx, row) in enumerate(eachrow(rows))
        
        !DEBUG && next!(pb; showvalues = [("Case #", row.case_number), ("date: ", row.auction_date), ("#", "$idx/$total"), ("NOS", string(nos_downloaded)), ("SMF", string(smf_downloaded))])
        

        # Respect concurrency limit by waiting for a completion when pool is full
        while running_tasks >= max_concurrent_tasks
            finished_case_number, exitcode = take!(channel)
            if exitcode != 0
                failed_jobs += 1
                @warn "Processing case #$finished_case_number failed!"
            end
            if exitcode == 0
                ad, filings = meta[String(finished_case_number)]
                for dir in filings
                    fname = expected_filename(dir, String(finished_case_number), ad)
                    path = joinpath("web/saledocs", dir, fname)
                    if isfile(path)
                        if dir == FilingType[:NOTICE_OF_SALE]
                            nos_downloaded += 1
                        elseif dir == FilingType[:SURPLUS_MONEY_FORM]
                            smf_downloaded += 1
                        end
                    end
                end
            end
            running_tasks -= 1
            finished_tasks += 1
        end

        task = @async begin
            let row = row
                ad = todate(row.auction_date)
                ad_str = ad === missing ? "" : Dates.format(ad, dateformat"yyyy-mm-dd")
                args = [row.case_number, row.borough, ad_str, row.missing_filings...]
                p = run(pipeline(ignorestatus(`node scrapers/notice_of_sale.js $args`), stdout, stderr), wait=true)
                put!(channel, (row.case_number, p.exitcode))
            end
        end
        push!(tasks, task)
        running_tasks += 1
        # Save metadata for completion-time accounting
        meta[String(row.case_number)] = (row.auction_date, String.(row.missing_filings))

        # Opportunistically drain any finished results to keep channel small
        while isready(channel)
            finished_case_number, exitcode = take!(channel)
            if exitcode != 0
                failed_jobs += 1
                @warn "Processing case #$finished_case_number failed!"
            end
            if exitcode == 0
                ad, filings = meta[String(finished_case_number)]
                for dir in filings
                    fname = expected_filename(dir, String(finished_case_number), ad)
                    path = joinpath("web/saledocs", dir, fname)
                    if isfile(path)
                        if dir == FilingType[:NOTICE_OF_SALE]
                            nos_downloaded += 1
                        elseif dir == FilingType[:SURPLUS_MONEY_FORM]
                            smf_downloaded += 1
                        end
                    end
                end
            end
            running_tasks -= 1
            finished_tasks += 1
        end
    end

    # Wait on tasks
    foreach(wait, tasks)

    # Drain remaining completions
    while finished_tasks < total
        finished_case_number, exitcode = take!(channel)
        if exitcode != 0
            failed_jobs += 1
            @warn "Processing case #$finished_case_number failed!"
        end
        if exitcode == 0
            ad, filings = meta[String(finished_case_number)]
            for dir in filings
                fname = expected_filename(dir, String(finished_case_number), ad)
                path = joinpath("web/saledocs", dir, fname)
                if isfile(path)
                    if dir == FilingType[:NOTICE_OF_SALE]
                        nos_downloaded += 1
                    elseif dir == FilingType[:SURPLUS_MONEY_FORM]
                        smf_downloaded += 1
                    end
                end
            end
        end
        finished_tasks += 1
    end
    println("\nNumber of failed jobs: $failed_jobs")

    !DEBUG && finish!(pb)
end

# Main function
main()
