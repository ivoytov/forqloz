using CSV, DataFrames, ProgressMeter, Base.Threads, Dates, Random, DotEnv, SQLite, DBInterface

DotEnv.load!()
# SQLite DB location (shared with other tools)
const DB_PATH = normpath(joinpath(@__DIR__, "..", "web", "foreclosures", "foreclosures.sqlite"))
db() = SQLite.DB(DB_PATH)

# Parse many common date representations into Date; return missing if unknown
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
# Get filings. If WSS is set then we are running locally, otherwise on git.
function main()
    # Allow overriding concurrency via env var, else choose by environment
    max_concurrency = tryparse(Int, get(ENV, "MAX_CONCURRENCY", ""))
    is_local = haskey(ENV, "WSS")
    max_concurrency = isnothing(max_concurrency) ? (is_local ? 4 : 8) : max_concurrency

    # First, download any pending PDFs discovered previously (parallelized)
    download_pdf_links(max_concurrency, is_local)

    rows = get_data()
	transform!(rows, :auction_date => ByRow(x -> Date(x, "yyyy-mm-dd")) => :auction_date)
    

    # Filter rows where :missing_filings contains FilingType[:NOTICE_OF_SALE]
    urgent_mask = coalesce.(rows.auction_date .>= today(), false)
    urgent_rows = rows[(in.(FilingType[:NOTICE_OF_SALE], rows.missing_filings)) .& urgent_mask, :]
    urgent_row_count = nrow(urgent_rows)

    # Shuffle the remaining rows and select N at random
    sampled_rows = rows[.!in.(FilingType[:NOTICE_OF_SALE], rows.missing_filings), :]
    sampled_row_count = nrow(sampled_rows)
    println("Repo state: $urgent_row_count urgent cases, $sampled_row_count sampled rows outstanding")

    if !is_local
        max_docs = 100
        urgent_rows = urgent_rows[1:min(nrow(urgent_rows), max_docs), :]
        n = min(max_docs - nrow(urgent_rows), nrow(sampled_rows))
        sampled_rows = sampled_rows[shuffle(1:nrow(sampled_rows))[1:n], :]
    end

    # Combine the filtered rows with the randomly selected rows
    rows = vcat(urgent_rows, sampled_rows)
    println("Task list: $(nrow(urgent_rows)) urgent cases, $(nrow(sampled_rows)) sampled rows, $(nrow(rows)) total tasks")
    process_data(rows, max_concurrency, is_local)
end

# Define the FilingType as a constant dictionary
const FilingType = Dict(
    :NOTICE_OF_SALE =>  "noticeofsale",
    :SURPLUS_MONEY_FORM => "surplusmoney"
)

get_filename(case_number) = replace(case_number, "/" => "-") * ".pdf"


function download_pdf_links(max_concurrent_tasks::Int=4, show_progress_bar=false)
    download_path = "web/foreclosures/download.csv"
    if !isfile(download_path)
        return
    end
    rows = CSV.read(download_path, DataFrame)
    # Only attempt those not on disk yet
    filter!(:filename => filename -> !isfile(joinpath("web/saledocs", filename)), rows)
    # Persist filtered queue back so restarts pick up remaining work
    CSV.write(download_path, rows)

    total = nrow(rows)
    if total == 0
        return
    end

    failed_jobs = 0
    finished_tasks = 0
    running_tasks = 0
    chan = Channel{Tuple{String, Int}}(total)  # unblocking completion queue

    pb = show_progress_bar ? Progress(total) : nothing
    out_stream = show_progress_bar ? devnull : stdout

    tasks = Task[]
    for (idx, row) in enumerate(eachrow(rows))
        show_progress_bar && next!(pb; showvalues=[("PDF", row.filename), ("#", "$idx/$total")])
        !show_progress_bar && println("[pdf] $idx/$total => $(row.filename)")

        # backpressure: wait until slot is free
        while running_tasks >= max_concurrent_tasks
            finished_filename, exitcode = take!(chan)
            if exitcode != 0
                failed_jobs += 1
                @warn "PDF download failed for $finished_filename"
            end
            running_tasks -= 1
            finished_tasks += 1
        end

        task = @async begin
            path = joinpath("web/saledocs", row.filename)
            p = run(pipeline(ignorestatus(`node scrapers/download_pdf.js $(row.url) $path`), out_stream, stderr), wait=true)
            put!(chan, (row.filename, p.exitcode))
        end
        push!(tasks, task)
        running_tasks += 1

        # opportunistically drain completions so the channel does not fill
        while isready(chan)
            finished_filename, exitcode = take!(chan)
            if exitcode != 0
                failed_jobs += 1
                @warn "PDF download failed for $finished_filename"
            end
            running_tasks -= 1
            finished_tasks += 1
        end
    end

    # Wait for all to finish
    foreach(wait, tasks)
    while finished_tasks < total
        finished_filename, exitcode = take!(chan)
        if exitcode != 0
            failed_jobs += 1
            @warn "PDF download failed for $finished_filename"
        end
        finished_tasks += 1
    end
    show_progress_bar && finish!(pb)
    println("PDF downloads complete. Failed: $failed_jobs / $total")
end

# Function to find missing filings
function missing_filings(case_number, auction_date)
    auction_date = todate(auction_date)
    if auction_date !== missing && auction_date < today() - Day(30)
        return []
    end

    filename = get_filename(case_number)

    res = []
    for (key, dir) in FilingType
        pdfPath = joinpath("web/saledocs", dir, filename)
        if !isfile(pdfPath)
            push!(res, dir)
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

    # Parse auction_date text to Date where possible
	transform!(rows, :auction_date => ByRow(todate) => :auction_date)

    # Keep deterministic order (most recent first)
    sort!(rows, order(:auction_date), rev=true)

    # Identify missing filings
    transform!(rows, [:case_number, :auction_date] => ByRow(missing_filings) => :missing_filings)
    filter!(row -> !isempty(row.missing_filings), rows)

    println(nrow(rows), " cases have missing filings")
    return rows
end


function process_data(rows, max_concurrent_tasks, show_progress_bar=false)
    tasks = Task[]
    # Use large-capacity channel to avoid blocking producers
    channel = Channel{Tuple{String, Int}}(max(1, nrow(rows)))
    failed_jobs = 0
    running_tasks = 0
    finished_tasks = 0

    if show_progress_bar
        pb = Progress(nrow(rows))
        out_stream = devnull
    else
        pb = nothing
        out_stream = stdout
    end

    total = nrow(rows)
    for (idx, row) in enumerate(eachrow(rows))
        show_progress_bar && next!(pb; showvalues = [("Case #", row.case_number), ("date: ", row.auction_date),  ("#", "$idx/$total")])
        !show_progress_bar && println("$idx/$total === $(row.case_number) $(row.borough) ===")

        # Respect concurrency limit by waiting for a completion when pool is full
        while running_tasks >= max_concurrent_tasks
            finished_case_number, exitcode = take!(channel)
            if exitcode != 0
                failed_jobs += 1
                @warn "Processing case #$finished_case_number failed!"
            end
            running_tasks -= 1
            finished_tasks += 1
        end

        task = @async begin
            let row = row
                ad = todate(row.auction_date)
                ad_str = ad === missing ? "" : Dates.format(ad, dateformat"yyyy-mm-dd")
                args = [row.case_number, row.borough, ad_str, row.missing_filings...]
                p = run(pipeline(ignorestatus(`node scrapers/notice_of_sale.js $args`), out_stream, stderr), wait=true)
                put!(channel, (row.case_number, p.exitcode))
            end
        end
        push!(tasks, task)
        running_tasks += 1

        # Opportunistically drain any finished results to keep channel small
        while isready(channel)
            finished_case_number, exitcode = take!(channel)
            if exitcode != 0
                failed_jobs += 1
                @warn "Processing case #$finished_case_number failed!"
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
        finished_tasks += 1
    end
    println("\nNumber of failed jobs: $failed_jobs")

    show_progress_bar && finish!(pb)
end

# Main function
main()
