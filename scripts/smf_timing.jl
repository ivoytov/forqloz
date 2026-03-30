using Dates, DataFrames, SQLite, DBInterface, Statistics

const DB_PATH = normpath(joinpath(@__DIR__, "..", "web", "foreclosures", "foreclosures.sqlite"))
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

function smf_timing_by_county(; thresholds=[3, 7, 10, 14, 21, 30])
    dbh = SQLite.DB(DB_PATH)
    try
        df = DataFrame(DBInterface.execute(dbh, """
            SELECT
                c.borough AS county,
                c.case_number,
                c.auction_date,
                MIN(f.mtime) AS smf_mtime
            FROM files AS f
            JOIN cases AS c
                ON f.case_number = c.case_number
                AND f.auction_date = c.auction_date
            WHERE f.doc_type = 'surplusmoney'
            GROUP BY c.borough, c.case_number, c.auction_date
            """))
    finally
        SQLite.close(dbh)
    end

    if nrow(df) == 0
        println("No surplus money form records found in DB")
        return DataFrame()
    end

    df[!, :auction_date] = parse_iso_date.(df[!, :auction_date])
    df[!, :smf_date] = parse_mtime_date.(df[!, :smf_mtime])
    df[!, :delay_days] = [ (!ismissing(a) && !ismissing(m)) ? Int((m - a).value) : missing for (a,m) in zip(df.auction_date, df.smf_date) ]

    # only post-auction filings (0 or more days)
    df = filter(:delay_days => x -> !ismissing(x) && x >= 0, df)

    group = groupby(df, :county)

    rows = DataFrame(
        county = String[],
        total_cases = Int[],
        within_3_days = Float64[],
        within_7_days = Float64[],
        within_10_days = Float64[],
        within_14_days = Float64[],
        within_21_days = Float64[],
        within_30_days = Float64[]
    )

    for g in group
        total = nrow(g)
        push!(rows, (
            g.county[1],
            total,
            100.0 * sum(g.delay_days .<= 3) / total,
            100.0 * sum(g.delay_days .<= 7) / total,
            100.0 * sum(g.delay_days .<= 10) / total,
            100.0 * sum(g.delay_days .<= 14) / total,
            100.0 * sum(g.delay_days .<= 21) / total,
            100.0 * sum(g.delay_days .<= 30) / total,
        ))
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
end

if abspath(PROGRAM_FILE) == @__FILE__
    df = smf_timing_by_county()
    print_smf_timing_table(df)
end
