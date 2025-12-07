using DataFrames, Dates, DotEnv, SQLite, DBInterface

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
    
    for (idx, row) in enumerate(eachrow(rows))
        if idx % 10 == 0
            println("Processing case $idx of $(nrow(rows))")
        end    
        ad_str = Dates.format(row.auction_date, dateformat"yyyy-mm-dd")
        args = [row.case_number, row.borough, ad_str, row.missing_filings...]
        run(pipeline(ignorestatus(`node scrapers/notice_of_sale.js $args`), stdout, stderr), wait=true)
    end
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
        if auction_date === missing
            # No date known; fall back to legacy naming
            return base * ".pdf"
        else
            # Store in date subfolder: YYYY-MM-DD/base.pdf
            return string(Dates.format(auction_date, dateformat"yyyy-mm-dd"), "/", base, ".pdf")
        end
    else
        return base * ".pdf"
    end
end

# Function to find missing filings
function missing_filings(case_number, auction_date)
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
	transform!(rows, :auction_date => ByRow(Date) => :auction_date)

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


# Main function
main()
