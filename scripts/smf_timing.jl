using Dates, DataFrames, SQLite, DBInterface, Statistics, CSV

const DB_PATH = normpath(joinpath(@__DIR__, "..", "web", "foreclosures", "foreclosures.sqlite"))
const SMF_DIR = normpath(joinpath(@__DIR__, "..", "web", "saledocs", "surplusmoney"))
const ISO_DATEFORMAT = dateformat"yyyy-mm-dd"

function parse_iso_date(x)
    x isa Date && return x
    (ismissing(x) || x === nothing) && return missing
    str = strip(string(x))
    isempty(str) && return missing
    return Date(str, ISO_DATEFORMAT)
end

function parse_mtime_date(x)
    x isa Date && return x
    (ismissing(x) || x === nothing) && return missing
    str = strip(string(x))
    isempty(str) && return missing
    try
        return Date(DateTime(str))
    catch
        try
            # fallback: mtime may have only date part
            return Date(str, ISO_DATEFORMAT)
        catch
            return missing
        end
    end
end

normalize_case_number(x) = uppercase(replace(strip(string(x)), r"[^A-Za-z0-9]" => ""))

function date_from_file_creation(path)
    # Julia's stat struct in this environment does not expose birthtime,
    # so on macOS we shell out to `stat -f %B` (birth time, unix seconds).
    if Sys.isapple()
        try
            birth_raw = read(`stat -f %B $path`, String)
            birth = tryparse(Int, strip(birth_raw))
            if birth !== nothing && birth > 0
                return Date(Dates.unix2datetime(birth))
            end
        catch
            # fall through to mtime
        end
    end

    # Fallback for files/systems where creation time is unavailable.
    ts = stat(path).mtime
    return Date(Dates.unix2datetime(round(Int, ts)))
end

function smf_timing_by_county(; thresholds=[3, 7, 10, 14, 21, 30])
    thresholds = sort!(unique(Int.(thresholds)))
    df = DataFrame()

    dbh = SQLite.DB(DB_PATH)
    try
        cases = DataFrame(DBInterface.execute(dbh, """
            SELECT borough AS county, case_number, auction_date
            FROM cases
            WHERE auction_date IS NOT NULL AND auction_date <> ''
            """))

        if nrow(cases) == 0
            println("No case rows found in DB")
            return DataFrame()
        end

        case_lookup = Dict{String, NamedTuple{(:county, :case_number, :auction_date), Tuple{String, String, Date}}}()
        for row in eachrow(cases)
            auction_date = parse_iso_date(row.auction_date)
            ismissing(auction_date) && continue
            key = normalize_case_number(row.case_number)
            case_lookup[key] = (county=row.county, case_number=string(row.case_number), auction_date=auction_date)
        end

        if !isdir(SMF_DIR)
            println("SMF directory does not exist: $SMF_DIR")
            return DataFrame()
        end

        first_smf_by_case = Dict{Tuple{String, String, Date}, Date}()
        matched_files = 0
        unmatched_files = 0
        total_files = 0

        for (root, _, files) in walkdir(SMF_DIR)
            for file in files
                endswith(lowercase(file), ".pdf") || continue
                total_files += 1
                stem = splitext(file)[1]
                key = normalize_case_number(stem)
                if !haskey(case_lookup, key)
                    unmatched_files += 1
                    continue
                end
                case_row = case_lookup[key]
                smf_date = date_from_file_creation(joinpath(root, file))
                case_key = (case_row.county, case_row.case_number, case_row.auction_date)
                if !haskey(first_smf_by_case, case_key) || smf_date < first_smf_by_case[case_key]
                    first_smf_by_case[case_key] = smf_date
                end
                matched_files += 1
            end
        end

        println("SMF files scanned: $total_files | matched to cases: $matched_files | unmatched: $unmatched_files")

        if isempty(first_smf_by_case)
            println("No matched SMF records found")
            return DataFrame()
        end

        df = DataFrame(
            county=String[],
            case_number=String[],
            auction_date=Date[],
            smf_date=Date[],
            delay_days=Int[]
        )

        for ((county, case_number, auction_date), smf_date) in first_smf_by_case
            delay_days = Int((smf_date - auction_date).value)
            delay_days < 0 && continue
            push!(df, (county, case_number, auction_date, smf_date, delay_days))
        end
    finally
        SQLite.close(dbh)
    end

    if nrow(df) == 0
        println("No post-auction SMF records found")
        return DataFrame()
    end

    group = groupby(df, :county)
    rows = DataFrame(county=String[], total_cases=Int[])
    for t in thresholds
        rows[!, Symbol("within_$(t)_days")] = Float64[]
    end

    for g in group
        total = nrow(g)
        row = Any[g.county[1], total]
        for t in thresholds
            push!(row, 100.0 * sum(g.delay_days .<= t) / total)
        end
        push!(rows, row)
    end

    sort!(rows, :county)
    return rows
end

function print_smf_timing_table(df)
    if nrow(df) == 0
        return
    end
    # Round to one decimal for readability
    for c in names(df)[3:end]
        df[!, c] = round.(df[!, c]; digits=1)
    end
    println("SMF timing percentiles by county")
    println("(percent of surplus money forms filed within N calendar days after auction date)")
    show(df, allcols=true, allrows=true)
    println()
end

function main(args)
    out_csv = nothing
    for arg in args
        if startswith(arg, "--csv=")
            out_csv = split(arg, "=", limit=2)[2]
        end
    end

    df = smf_timing_by_county(thresholds=[3, 7, 10, 14, 21, 30])
    print_smf_timing_table(df)
    if out_csv !== nothing && !isempty(strip(out_csv))
        CSV.write(out_csv, df)
        println("Wrote CSV: $out_csv")
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
