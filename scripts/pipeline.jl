using Dates
using SQLite
using DBInterface
using JSON3
using SHA
using DataFrames

const ROOT = normpath(joinpath(@__DIR__, ".."))
const DB_PATH = normpath(joinpath(ROOT, "web", "foreclosures", "foreclosures.sqlite"))
const DB_WRITE_LOCK = ReentrantLock()

include(joinpath(ROOT, "scrapers", "download_case_filings.jl"))
include(joinpath(ROOT, "scrapers", "extract_info.jl"))
include(joinpath(ROOT, "process_auctions.jl"))

function db()
    conn = SQLite.DB(DB_PATH)
    DBInterface.execute(conn, "PRAGMA journal_mode=WAL;")
    DBInterface.execute(conn, "PRAGMA busy_timeout=5000;")
    return conn
end

now_iso() = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")


function migrate_schema()
    lock(DB_WRITE_LOCK) do
        dbh = db()
        try
            DBInterface.execute(dbh, """
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY,
                    started_at TEXT,
                    finished_at TEXT,
                    status TEXT,
                    args_json TEXT
                );
            """)
            DBInterface.execute(dbh, """
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY,
                    run_id INTEGER,
                    type TEXT,
                    case_number TEXT,
                    borough TEXT,
                    auction_date TEXT,
                    status TEXT,
                    attempts INTEGER,
                    last_error TEXT,
                    next_attempt_at TEXT,
                    updated_at TEXT
                );
            """)
            DBInterface.execute(dbh, """
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY,
                    case_number TEXT,
                    doc_type TEXT,
                    auction_date TEXT,
                    rel_path TEXT,
                    sha256 TEXT,
                    bytes INTEGER,
                    mtime TEXT,
                    created_at TEXT
                );
            """)
            DBInterface.execute(dbh, """
                CREATE TABLE IF NOT EXISTS extractions (
                    id INTEGER PRIMARY KEY,
                    case_number TEXT,
                    type TEXT,
                    status TEXT,
                    source_file_id INTEGER,
                    payload_json TEXT,
                    updated_at TEXT
                );
            """)
            DBInterface.execute(dbh, """
                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY,
                    case_number TEXT,
                    type TEXT,
                    status TEXT,
                    reason TEXT,
                    payload_json TEXT,
                    updated_at TEXT
                );
            """)
            DBInterface.execute(dbh, """
                CREATE TABLE IF NOT EXISTS case_status (
                    case_number TEXT PRIMARY KEY,
                    status TEXT,
                    reason TEXT,
                    updated_at TEXT
                );
            """)

            DBInterface.execute(dbh, "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(type, status, next_attempt_at)")
            DBInterface.execute(dbh, "CREATE INDEX IF NOT EXISTS idx_files_case ON files(case_number, doc_type, auction_date)")
            DBInterface.execute(dbh, "CREATE INDEX IF NOT EXISTS idx_extractions_case ON extractions(case_number, type)")
            DBInterface.execute(dbh, "CREATE INDEX IF NOT EXISTS idx_reviews_status ON reviews(status, type)")
        finally
            SQLite.close(dbh)
        end
    end
end

function start_run()
    lock(DB_WRITE_LOCK) do
        dbh = db()
        try
            args_json = JSON3.write(Dict("args" => ARGS))
            DBInterface.execute(dbh,
                "INSERT INTO runs (started_at, status, args_json) VALUES (?, ?, ?)",
                (now_iso(), "running", args_json))
            id = DBInterface.execute(dbh, "SELECT last_insert_rowid()") |> DataFrame
            return id[1, 1]
        finally
            SQLite.close(dbh)
        end
    end
end

function finish_run(run_id, status)
    lock(DB_WRITE_LOCK) do
        dbh = db()
        try
            DBInterface.execute(dbh,
                "UPDATE runs SET finished_at = ?, status = ? WHERE id = ?",
                (now_iso(), status, run_id))
            # Browser sql.js reads the main SQLite file only (not WAL sidecars),
            # so checkpoint WAL at run end to persist all staged updates.
            # This can fail when another process has an open read lock; keep run
            # finalization successful and treat checkpointing as best effort.
            checkpoint_ok = false
            for attempt in 1:3
                try
                    DBInterface.execute(dbh, "PRAGMA wal_checkpoint(TRUNCATE);")
                    checkpoint_ok = true
                    break
                catch e
                    if !occursin("locked", lowercase(string(e))) || attempt == 3
                        break
                    end
                    sleep(0.2 * attempt)
                end
            end
            if !checkpoint_ok
                println("finish_run: WAL checkpoint skipped (database busy/locked)")
            end
        finally
            SQLite.close(dbh)
        end
    end
end

function enqueue_job(run_id, type, case_number, borough, auction_date)
    lock(DB_WRITE_LOCK) do
        dbh = db()
        try
            existing = DBInterface.execute(dbh,
                "SELECT id, status FROM jobs WHERE type = ? AND case_number = ? AND borough = ? AND auction_date = ? AND status IN ('pending','running','retry','review') LIMIT 1",
                (type, case_number, borough, string(auction_date))) |> DataFrame
            if nrow(existing) > 0
                status = existing[1, :status]
                if status == "running"
                    DBInterface.execute(dbh,
                        "UPDATE jobs SET run_id = ? WHERE id = ?",
                        (run_id, existing[1, :id]))
                else
                    DBInterface.execute(dbh,
                        "UPDATE jobs SET run_id = ?, status = 'pending', next_attempt_at = NULL, updated_at = ? WHERE id = ?",
                        (run_id, now_iso(), existing[1, :id]))
                end
                return
            end
            DBInterface.execute(dbh,
                "INSERT INTO jobs (run_id, type, case_number, borough, auction_date, status, attempts, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, type, case_number, borough, string(auction_date), "pending", 0, now_iso()))
        finally
            SQLite.close(dbh)
        end
    end
end

function list_jobs(type; statuses=["pending", "retry"], prioritize_run_id=nothing)
    dbh = db()
    try
        status_list = join(["'$(s)'" for s in statuses], ",")
        nowstr = now_iso()
        run_priority_sql = prioritize_run_id === nothing ? "" : "CASE WHEN run_id = $(prioritize_run_id) THEN 0 ELSE 1 END,"
        due_filter_sql = prioritize_run_id === nothing ?
            "(next_attempt_at IS NULL OR next_attempt_at <= '$nowstr')" :
            "(run_id = $(prioritize_run_id) OR next_attempt_at IS NULL OR next_attempt_at <= '$nowstr')"
        sql = """
            SELECT id, case_number, borough, auction_date, attempts
            FROM jobs
            WHERE type = '$type'
              AND status IN ($status_list)
              AND $due_filter_sql
            ORDER BY $run_priority_sql auction_date DESC, updated_at ASC
        """
        return DataFrame(DBInterface.execute(dbh, sql))
    finally
        SQLite.close(dbh)
    end
end

function reclaim_run_jobs(run_id, type)
    run_id === nothing && return
    lock(DB_WRITE_LOCK) do
        dbh = db()
        try
            DBInterface.execute(dbh,
                """
                UPDATE jobs
                SET status = 'pending',
                    next_attempt_at = NULL,
                    updated_at = ?
                WHERE run_id = ?
                  AND type = ?
                  AND status IN ('running', 'retry', 'review')
                """,
                (now_iso(), run_id, type))
        finally
            SQLite.close(dbh)
        end
    end
end

function update_job_status(job_id, status; last_error=nothing, next_attempt_at=nothing)
    lock(DB_WRITE_LOCK) do
        dbh = db()
        try
            DBInterface.execute(dbh,
                "UPDATE jobs SET status = ?, last_error = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?",
                (status, last_error, next_attempt_at, now_iso(), job_id))
        finally
            SQLite.close(dbh)
        end
    end
end

function increment_job_attempts(job_id)
    lock(DB_WRITE_LOCK) do
        dbh = db()
        try
            DBInterface.execute(dbh, "UPDATE jobs SET attempts = attempts + 1, updated_at = ? WHERE id = ?", (now_iso(), job_id))
        finally
            SQLite.close(dbh)
        end
    end
end

function retry_at(attempts)
    minutes = attempts <= 1 ? 30 : attempts == 2 ? 120 : attempts == 3 ? 720 : 1440
    return Dates.format(now() + Minute(minutes), dateformat"yyyy-mm-ddTHH:MM:SS")
end

const SURPLUS_SCHEDULE_DAYS = [1, 2, 4, 7, 14, 28]

function next_surplus_attempt(auction_date)
    auction_date === missing && return nothing
    # pick the next scheduled day relative to T (auction date)
    today_date = Date(now())
    for offset in SURPLUS_SCHEDULE_DAYS
        scheduled = auction_date + Day(offset)
        if scheduled >= today_date
            return Dates.format(DateTime(scheduled), dateformat"yyyy-mm-ddTHH:MM:SS")
        end
    end
    return nothing
end

function next_nos_attempt()
    return Dates.format(now() + Day(1), dateformat"yyyy-mm-ddTHH:MM:SS")
end

function rel_path_for_doc(case_number, doc_type, auction_date)
    base = replace(case_number, "/" => "-") * ".pdf"
    if doc_type == "noticeofsale"
        if auction_date === missing
            return joinpath("noticeofsale", base)
        end
        date_str = Dates.format(Date(auction_date), dateformat"yyyy-mm-dd")
        return joinpath("noticeofsale", date_str, base)
    else
        return joinpath("surplusmoney", base)
    end
end

function record_file(doc_type, case_number, auction_date, rel_path)
    full_path = normpath(joinpath(ROOT, "web", "saledocs", rel_path))
    if !isfile(full_path)
        return
    end
    info = Base.stat(full_path)
    bytes = info.size
    mtime_dt = Dates.unix2datetime(info.mtime)
    mtime = Dates.format(mtime_dt, dateformat"yyyy-mm-ddTHH:MM:SS")
    sha = open(full_path, "r") do io
        bytes2hex(sha256(io))
    end
    lock(DB_WRITE_LOCK) do
        dbh = db()
        try
            existing = DBInterface.execute(dbh,
                "SELECT id FROM files WHERE case_number = ? AND doc_type = ? AND auction_date = ? AND rel_path = ? LIMIT 1",
                (case_number, doc_type, string(auction_date), rel_path)) |> DataFrame
            if nrow(existing) > 0
                DBInterface.execute(dbh,
                    "UPDATE files SET sha256 = ?, bytes = ?, mtime = ?, created_at = ? WHERE id = ?",
                    (sha, bytes, mtime, now_iso(), existing[1, 1]))
            else
                DBInterface.execute(dbh,
                    "INSERT INTO files (case_number, doc_type, auction_date, rel_path, sha256, bytes, mtime, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (case_number, doc_type, string(auction_date), rel_path, sha, bytes, mtime, now_iso()))
            end
        finally
            SQLite.close(dbh)
        end
    end
end

function ensure_review(case_number, type, reason; payload=Dict())
    lock(DB_WRITE_LOCK) do
        dbh = db()
        try
            existing = DBInterface.execute(dbh,
                "SELECT 1 FROM reviews WHERE case_number = ? AND type = ? AND status = 'pending' LIMIT 1",
                (case_number, type)) |> DataFrame
            if nrow(existing) > 0
                return
            end
            payload_json = JSON3.write(payload)
            DBInterface.execute(dbh,
                "INSERT INTO reviews (case_number, type, status, reason, payload_json, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (case_number, type, "pending", reason, payload_json, now_iso()))
        finally
            SQLite.close(dbh)
        end
    end
end

function record_extraction(case_number, type; payload=Dict(), source_file_id=nothing)
    lock(DB_WRITE_LOCK) do
        dbh = db()
        try
            payload_json = JSON3.write(payload)
            DBInterface.execute(dbh,
                "INSERT INTO extractions (case_number, type, status, source_file_id, payload_json, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (case_number, type, "done", source_file_id, payload_json, now_iso()))
        finally
            SQLite.close(dbh)
        end
    end
end

function sync_calendar(run_id)
    println("Stage: sync-calendar")
    run(`bun scrapers/calendar.js`)

    rows = build_download_jobs()
    for row in eachrow(rows)
        enqueue_job(run_id, "download_filing", row.case_number, row.borough, row.auction_date)
    end

    dbh = db()
    try
        cases = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough, auction_date FROM cases"))
        lots = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough FROM lots"))
        bids = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough, auction_date FROM bids"))
        cases.auction_date = [ismissing(x) ? missing : Date(x, dateformat"yyyy-mm-dd") for x in cases.auction_date]
        bids.auction_date = [ismissing(x) ? missing : Date(x, dateformat"yyyy-mm-dd") for x in bids.auction_date]

        missing_lots = antijoin(cases, lots, on=[:case_number, :borough])
        for row in eachrow(missing_lots)
            pdf_path = notice_of_sale_path(row.case_number, row.auction_date)
            if isfile(pdf_path)
                enqueue_job(run_id, "extract_nos", row.case_number, row.borough, row.auction_date)
            end
        end

        for row in eachrow(cases)
            filename = replace(row.case_number, "/" => "-") * ".pdf"
            pdf_path = joinpath(ROOT, "web", "saledocs", "surplusmoney", filename)
            if !isfile(pdf_path)
                continue
            end
            existing = filter(r -> r.case_number == row.case_number && r.borough == row.borough && r.auction_date == row.auction_date, eachrow(bids))
            if isempty(existing)
                enqueue_job(run_id, "extract_bids", row.case_number, row.borough, row.auction_date)
            end
        end
    finally
        SQLite.close(dbh)
    end
end

function enqueue_missing_jobs(run_id)
    println("Stage: enqueue-missing-jobs")
    dbh = db()
    try
        cases = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough, auction_date FROM cases"))
        lots = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough FROM lots"))
        bids = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough, auction_date FROM bids"))
        cases.auction_date = [ismissing(x) ? missing : Date(x, dateformat"yyyy-mm-dd") for x in cases.auction_date]
        bids.auction_date = [ismissing(x) ? missing : Date(x, dateformat"yyyy-mm-dd") for x in bids.auction_date]

        missing_lots = antijoin(cases, lots, on=[:case_number, :borough])
        for row in eachrow(missing_lots)
            pdf_path = notice_of_sale_path(row.case_number, row.auction_date)
            if isfile(pdf_path)
                enqueue_job(run_id, "extract_nos", row.case_number, row.borough, row.auction_date)
            end
        end

        for row in eachrow(cases)
            filename = replace(row.case_number, "/" => "-") * ".pdf"
            pdf_path = joinpath(ROOT, "web", "saledocs", "surplusmoney", filename)
            if !isfile(pdf_path)
                continue
            end
            existing = filter(r -> r.case_number == row.case_number && r.borough == row.borough && r.auction_date == row.auction_date, eachrow(bids))
            if isempty(existing)
                enqueue_job(run_id, "extract_bids", row.case_number, row.borough, row.auction_date)
            end
        end
    finally
        SQLite.close(dbh)
    end
end

function sync_filings(run_id=nothing)
    println("Stage: sync-filings")
    reclaim_run_jobs(run_id, "download_filing")
    jobs = list_jobs("download_filing"; prioritize_run_id=run_id)
    if nrow(jobs) == 0
        println("No download_filing jobs pending.")
        return
    end
    total = nrow(jobs)
    println("Processing $total download_filing jobs")
    completed = 0
    for row in eachrow(jobs)
        completed += 1
        if completed % 5 == 0 || completed == 1 || completed == total
            println("sync-filings progress: $completed / $total (case $(row.case_number))")
        end
        update_job_status(row.id, "running")
        increment_job_attempts(row.id)
        case_number = row.case_number
        borough = row.borough
        auction_date = tryparse(Date, row.auction_date, dateformat"yyyy-mm-dd")
        auction_date = auction_date === nothing ? missing : auction_date

        missing = missing_filings(case_number, auction_date)
        if isempty(missing)
            update_job_status(row.id, "done")
            continue
        end
        try
            run_download_job(case_number, borough, auction_date, missing)
        catch e
            update_job_status(row.id, "retry"; last_error=string(e), next_attempt_at=retry_at(row.attempts + 1))
            continue
        end

        if case_number in discontinued_cases()
            dbh = db()
            try
                DBInterface.execute(dbh,
                    "INSERT OR REPLACE INTO case_status (case_number, status, reason, updated_at) VALUES (?, ?, ?, ?)",
                    (case_number, "discontinued", "case log", now_iso()))
            finally
                SQLite.close(dbh)
            end
            update_job_status(row.id, "done")
            continue
        end

        remaining = missing_filings(case_number, auction_date)
        if isempty(remaining)
            for doc_type in ["noticeofsale", "surplusmoney"]
                rel_path = rel_path_for_doc(case_number, doc_type, auction_date)
                record_file(doc_type, case_number, auction_date, rel_path)
            end
            update_job_status(row.id, "done")
            continue
        end

        # Reschedule based on document type cadence
        wants_nos = any(x -> x == "noticeofsale", remaining)
        wants_surplus = any(x -> x == "surplusmoney", remaining)
        next_times = String[]
        if wants_nos
            push!(next_times, next_nos_attempt())
        end
        if wants_surplus
            next_surplus = next_surplus_attempt(auction_date)
            if next_surplus !== nothing
                push!(next_times, next_surplus)
            end
        end

        if isempty(next_times)
            # Surplus schedule exhausted and no NOS pending
            update_job_status(row.id, "done"; last_error="schedule exhausted")
        else
            next_attempt = sort(next_times)[1]
            update_job_status(row.id, "retry"; last_error="missing filings after download", next_attempt_at=next_attempt)
        end
    end
end

function extract_nos()
    println("Stage: extract-nos")
    jobs = list_jobs("extract_nos")
    if nrow(jobs) == 0
        println("No extract_nos jobs pending.")
        return
    end
    println("Processing $(nrow(jobs)) extract_nos jobs")
    dbh = db()
    cases = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough, auction_date FROM cases"))
    cases.auction_date = [ismissing(x) ? missing : Date(x, dateformat"yyyy-mm-dd") for x in cases.auction_date]
    SQLite.close(dbh)

    concurrency = tryparse(Int, get(ENV, "NOS_CONCURRENCY", "4"))
    concurrency = (concurrency === nothing || concurrency < 1) ? 1 : concurrency
    sem = Base.Semaphore(concurrency)
    tasks = Dict{Int, Task}()

    for row in eachrow(jobs)
        tasks[row.id] = Threads.@spawn begin
            Base.acquire(sem)
            try
                update_job_status(row.id, "running")
                increment_job_attempts(row.id)
                job_date = tryparse(Date, row.auction_date, dateformat"yyyy-mm-dd")
                job_date = job_date === nothing ? missing : job_date
                case_row = filter(r -> r.case_number == row.case_number && r.borough == row.borough && r.auction_date == job_date, eachrow(cases))
                if isempty(case_row)
                    update_job_status(row.id, "dead"; last_error="case not found")
                    return
                end
                case_row = first(case_row)
                res = extract_notice_of_sale(case_row; interactive=false)
                if res == :ok
                    record_extraction(row.case_number, "nos"; payload=Dict("status" => "ok"))
                    update_job_status(row.id, "done")
                    println("extract-nos: done $(row.case_number)")
                else
                    ensure_review(row.case_number, "nos", "missing block/lot"; payload=Dict("case_number" => row.case_number))
                    update_job_status(row.id, "review"; last_error="missing block/lot")
                    println("extract-nos: review needed $(row.case_number)")
                end
            catch e
                update_job_status(row.id, "retry"; last_error=string(e), next_attempt_at=retry_at(row.attempts + 1))
                println("extract-nos: failed $(row.case_number) error=$(e)")
            finally
                Base.release(sem)
            end
        end
    end

    completed = 0
    total = nrow(jobs)
    for (job_id, task) in tasks
        try
            fetch(task)
        catch e
            println("extract-nos: task failed job_id=$job_id error=$e")
        finally
            completed += 1
            if completed % 5 == 0 || completed == total
                println("extract-nos progress: $completed / $total completed")
            end
        end
    end
end

function extract_bids_stage()
    println("Stage: extract-bids")
    jobs = list_jobs("extract_bids")
    if nrow(jobs) == 0
        println("No extract_bids jobs pending.")
        return
    end
    println("Processing $(nrow(jobs)) extract_bids jobs")
    dbh = db()
    cases = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough, auction_date FROM cases"))
    cases.auction_date = [ismissing(x) ? missing : Date(x, dateformat"yyyy-mm-dd") for x in cases.auction_date]
    SQLite.close(dbh)

    concurrency = tryparse(Int, get(ENV, "LLM_CONCURRENCY", "4"))
    concurrency = (concurrency === nothing || concurrency < 1) ? 1 : concurrency
    sem = Base.Semaphore(concurrency)
    tasks = Dict{Int, Task}()

    for (idx, row) in enumerate(eachrow(jobs))
        tasks[row.id] = Threads.@spawn begin
            Base.acquire(sem)
            try
                update_job_status(row.id, "running")
                increment_job_attempts(row.id)
                job_date = tryparse(Date, row.auction_date, dateformat"yyyy-mm-dd")
                job_date = job_date === nothing ? missing : job_date
                case_row = filter(r -> r.case_number == row.case_number && r.borough == row.borough && r.auction_date == job_date, eachrow(cases))
                if isempty(case_row)
                    update_job_status(row.id, "dead"; last_error="case not found")
                    return
                end
                case_row = first(case_row)
                res = extract_bids(case_row; use_llm=true, interactive=false)
                if res == :ok
                    record_extraction(row.case_number, "bids"; payload=Dict("status" => "ok"))
                    update_job_status(row.id, "done")
                    println("extract-bids: done $(row.case_number)")
                else
                    ensure_review(row.case_number, "bids", "llm parse failure"; payload=Dict("case_number" => row.case_number))
                    update_job_status(row.id, "review"; last_error="missing values")
                    println("extract-bids: review needed $(row.case_number)")
                end
            catch e
                update_job_status(row.id, "retry"; last_error=string(e), next_attempt_at=retry_at(row.attempts + 1))
                println("extract-bids: failed $(row.case_number) error=$(e)")
            finally
                Base.release(sem)
            end
        end
    end

    completed = 0
    total = nrow(jobs)
    for (job_id, task) in tasks
        try
            fetch(task)
        catch e
            println("extract-bids: task failed job_id=$job_id error=$e")
        finally
            completed += 1
            if completed % 5 == 0 || completed == total
                println("extract-bids progress: $completed / $total completed")
            end
        end
    end
end

function enrich_pluto_stage()
    println("Stage: enrich-pluto")
    enrich_bbls_and_pluto()
end

function build_auction_sales_stage()
    println("Stage: build-auction-sales")
    build_auction_sales()
end

function review_list()
    dbh = db()
    try
        rows = DataFrame(DBInterface.execute(dbh, "SELECT case_number, type, reason, updated_at FROM reviews WHERE status = 'pending' ORDER BY updated_at ASC"))
        if nrow(rows) == 0
            println("No pending reviews.")
            return
        end
        for row in eachrow(rows)
            println("$(row.case_number) $(row.type) $(row.reason) $(row.updated_at)")
        end
    finally
        SQLite.close(dbh)
    end
end

function review_next()
    dbh = db()
    row = DataFrame()
    try
        row = DataFrame(DBInterface.execute(dbh, "SELECT id, case_number, type FROM reviews WHERE status = 'pending' ORDER BY updated_at ASC LIMIT 1"))
    finally
        SQLite.close(dbh)
    end
    if nrow(row) == 0
        println("No pending reviews.")
        return
    end
    rid = row[1, :id]
    case_number = row[1, :case_number]
    type = row[1, :type]
    open_pdf = get(ENV, "PIPELINE_OPEN_PDF", "0") == "1"
    if type == "nos"
        dbh_lookup = db()
        auction_date = missing
        try
            case_row = DataFrame(DBInterface.execute(dbh_lookup, "SELECT auction_date FROM cases WHERE case_number = ? LIMIT 1", (case_number,)))
            if nrow(case_row) > 0 && !ismissing(case_row[1, :auction_date])
                auction_date = tryparse(Date, case_row[1, :auction_date], dateformat"yyyy-mm-dd")
            end
        finally
            SQLite.close(dbh_lookup)
        end
        pdf_path = notice_of_sale_path(case_number, auction_date)
        println("PDF: $(pdf_path)")
        if open_pdf && isfile(pdf_path)
            run(`open "$pdf_path"`)
        end
        print("Enter block: "); block = readline()
        print("Enter lot: "); lot = readline()
        print("Enter address (optional): "); address = readline()
        if isempty(block) || isempty(lot)
            println("Missing block/lot; review not resolved.")
            return
        end
        dbh2 = db()
        try
            case_row = DataFrame(DBInterface.execute(dbh2, "SELECT case_number, borough FROM cases WHERE case_number = ? LIMIT 1", (case_number,)))
            if nrow(case_row) == 0
                println("Case not found; review not resolved.")
                return
            end
            borough = case_row[1, :borough]
            DBInterface.execute(dbh2,
                "INSERT INTO lots (case_number, borough, block, lot, address, BBL, unit) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (case_number, borough, parse(Int, block), parse(Int, lot), isempty(address) ? nothing : address, nothing, nothing))
            DBInterface.execute(dbh2, "UPDATE reviews SET status = 'done', updated_at = ? WHERE id = ?", (now_iso(), rid))
        finally
            SQLite.close(dbh2)
        end
        println("Review resolved for $case_number (nos).")
    elseif type == "bids"
        filename = replace(case_number, "/" => "-") * ".pdf"
        pdf_path = joinpath(ROOT, "web", "saledocs", "surplusmoney", filename)
        println("PDF: $(pdf_path)")
        if open_pdf && isfile(pdf_path)
            run(`open "$pdf_path"`)
        end
        print("Enter judgement: "); judgement = readline()
        print("Enter upset price: "); upset_price = readline()
        print("Enter winning bid: "); winning_bid = readline()
        if isempty(judgement) || isempty(upset_price) || isempty(winning_bid)
            println("Missing values; review not resolved.")
            return
        end
        dbh2 = db()
        try
            case_row = DataFrame(DBInterface.execute(dbh2, "SELECT case_number, borough, auction_date FROM cases WHERE case_number = ? LIMIT 1", (case_number,)))
            if nrow(case_row) == 0
                println("Case not found; review not resolved.")
                return
            end
            borough = case_row[1, :borough]
            auction_date = case_row[1, :auction_date]
            DBInterface.execute(dbh2,
                "INSERT INTO bids (case_number, borough, auction_date, judgement, upset_price, winning_bid) VALUES (?, ?, ?, ?, ?, ?)",
                (case_number, borough, auction_date, parse(Float64, judgement), parse(Float64, upset_price), parse(Float64, winning_bid)))
            DBInterface.execute(dbh2, "UPDATE reviews SET status = 'done', updated_at = ? WHERE id = ?", (now_iso(), rid))
        finally
            SQLite.close(dbh2)
        end
        println("Review resolved for $case_number (bids).")
    else
        println("Unknown review type: $type")
    end
end

function review_resolve(case_number, type)
    dbh = db()
    try
        DBInterface.execute(dbh, "UPDATE reviews SET status = 'done', updated_at = ? WHERE case_number = ? AND type = ? AND status = 'pending'",
            (now_iso(), case_number, type))
    finally
        SQLite.close(dbh)
    end
end

function run_pipeline()
    migrate_schema()
    run_id = start_run()
    status = "done"
    try
        sync_calendar(run_id)
        sync_filings(run_id)
        extract_nos()
        extract_bids_stage()
        enrich_pluto_stage()
        build_auction_sales_stage()
    catch e
        status = "failed"
        println("Pipeline failed: $e")
        rethrow()
    finally
        finish_run(run_id, status)
    end
end

function run_with_run(fn)
    run_id = start_run()
    status = "done"
    try
        fn(run_id)
    catch
        status = "failed"
        rethrow()
    finally
        finish_run(run_id, status)
    end
end

function main()
    if length(ARGS) == 0
        println("Usage: julia --project=. scripts/pipeline.jl <command>")
        return
    end
    cmd = ARGS[1]
    migrate_schema()
    if cmd == "run"
        run_pipeline()
    elseif cmd == "sync-calendar"
        run_with_run(sync_calendar)
    elseif cmd == "enqueue-missing-jobs"
        run_with_run(enqueue_missing_jobs)
    elseif cmd == "sync-filings"
        run_with_run(_ -> sync_filings())
    elseif cmd == "extract-nos"
        run_with_run(run_id -> begin
            enqueue_missing_jobs(run_id)
            extract_nos()
        end)
    elseif cmd == "extract-bids"
        run_with_run(_ -> extract_bids_stage())
    elseif cmd == "enrich-pluto"
        run_with_run(_ -> enrich_pluto_stage())
    elseif cmd == "build-auction-sales"
        run_with_run(_ -> build_auction_sales_stage())
    elseif cmd == "review"
        if length(ARGS) < 2
            println("Usage: review <list|next|resolve>")
            return
        end
        sub = ARGS[2]
        if sub == "list"
            review_list()
        elseif sub == "next"
            review_next()
        elseif sub == "resolve"
            if length(ARGS) < 5
                println("Usage: review resolve --case <case_number> --type <nos|bids>")
                return
            end
            case_number = ""
            type = ""
            i = 3
            while i <= length(ARGS)
                if ARGS[i] == "--case"
                    case_number = ARGS[i + 1]
                    i += 2
                elseif ARGS[i] == "--type"
                    type = ARGS[i + 1]
                    i += 2
                else
                    i += 1
                end
            end
            if isempty(case_number) || isempty(type)
                println("Usage: review resolve --case <case_number> --type <nos|bids>")
                return
            end
            review_resolve(case_number, type)
        else
            println("Unknown review subcommand: $sub")
        end
    else
        println("Unknown command: $cmd")
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
