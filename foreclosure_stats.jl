using Dates, DataFrames, AlgebraOfGraphics, CairoMakie, Statistics, SQLite, DBInterface
set_aog_theme!()

const DB_PATH = normpath(joinpath(@__DIR__, "web", "foreclosures", "foreclosures.sqlite"))
const ISO_DATEFORMAT = dateformat"yyyy-mm-dd"
const WEEKLY_BAR_OUTPUT = "manhattan_weekly_foreclosures.png"
const WEEKLY_CALENDAR_HEATMAP_OUTPUT = "manhattan_weekly_foreclosures_calendar_heatmap.png"
const CALENDAR_HEATMAP_ARG = "--calendar-heatmap"
const SCATTER_BLDG_CLASSES = Set([
    "CONDOMINIUMS",
    "ONE FAMILY DWELLINGS",
    "TWO FAMILY DWELLINGS",
    "VACANT LAND",
])

const WEEKLY_START_DATE = Date("2024-01-01")
const HILTON_TIMESHARE_BLOCK = 1006
const HILTON_TIMESHARE_LOT = 1302
const HILTON_TIMESHARE_BBL = 1010067502

function parse_iso_date(x)
    x isa Date && return x
    (ismissing(x) || x === nothing) && return missing
    str = strip(string(x))
    isempty(str) && return missing
    return Date(str, ISO_DATEFORMAT)
end

function strip_bldgclass_label(x)
    (ismissing(x) || x === nothing) && return missing
    str = strip(string(x))
    isempty(str) && return str
    idx = findfirst(==(':'), str)
    return isnothing(idx) ? str : (idx == firstindex(str) ? "" : strip(str[firstindex(str):prevind(str, idx)]))
end

function is_hilton_timeshare(block, lot, bbl)
    by_block_lot = !ismissing(block) && !ismissing(lot) &&
        block == HILTON_TIMESHARE_BLOCK && lot == HILTON_TIMESHARE_LOT
    by_bbl = !ismissing(bbl) && bbl == HILTON_TIMESHARE_BBL
    return by_block_lot || by_bbl
end

function week_bucket_start(d::Date)
    return WEEKLY_START_DATE + Day(7 * fld(Dates.value(d - WEEKLY_START_DATE), 7))
end

function month_ticks(week_starts)
    positions = Int[]
    labels = String[]
    last_month = nothing
    for (idx, week_start) in enumerate(week_starts)
        month_key = (year(week_start), month(week_start))
        if month_key != last_month
            push!(positions, idx)
            push!(labels, Dates.format(week_start, dateformat"m/yy"))
            last_month = month_key
        end
    end
    return positions, labels
end

hasarg(flag) = any(==(flag), ARGS)

function getarg(flag)
    idx = findfirst(==(flag), ARGS)
    isnothing(idx) && return nothing
    idx == length(ARGS) && return nothing
    return ARGS[idx + 1]
end

function week_of_year_position(d::Date)
    return fld(dayofyear(d) - 1, 7) + 1
end

function weekly_sales_counts(auctions, borough=nothing)
    filter_borough = isnothing(borough) ? (x -> true) : (x -> x == borough)
    weekly_sales = subset(
        copy(auctions),
        :borough => ByRow(filter_borough),
        :auction_date => ByRow(x -> !ismissing(x) && x >= WEEKLY_START_DATE),
        :BldgClass => ByRow(x -> x ∈ BLDG_CLASSES),
        [:block, :lot, :BBL] => ByRow((block, lot, bbl) -> !is_hilton_timeshare(block, lot, bbl)),
    )

    if nrow(weekly_sales) == 0
        series_end = week_bucket_start(today())
        weekly_counts = DataFrame(week_start = collect(WEEKLY_START_DATE:Week(1):series_end))
        weekly_counts[!, :count] = zeros(Int, nrow(weekly_counts))
        return weekly_counts
    end

    weekly_sales[!, :week_start] = week_bucket_start.(weekly_sales[!, :auction_date])
    weekly_counts = combine(groupby(weekly_sales, :week_start), nrow => :count)
    sort!(weekly_counts, :week_start)

    series_end = max(week_bucket_start(today()), maximum(weekly_counts[!, :week_start]))
    all_weeks = DataFrame(week_start = collect(WEEKLY_START_DATE:Week(1):series_end))
    weekly_counts = leftjoin(all_weeks, weekly_counts, on = :week_start)
    weekly_counts[!, :count] = Int.(coalesce.(weekly_counts[!, :count], 0))
    return weekly_counts
end

function render_weekly_bar_chart(weekly_counts; borough=nothing, output_path = WEEKLY_BAR_OUTPUT)
    current_week = week_bucket_start(today())
    weekly_counts = copy(weekly_counts)
    weekly_counts[!, :x] = collect(1:nrow(weekly_counts))
    weekly_counts[!, :color] = [
        week_start == current_week ? colorant"#E45C35" : colorant"#2E5BFF"
        for week_start in weekly_counts[!, :week_start]
    ]

    tick_positions, tick_labels = month_ticks(weekly_counts[!, :week_start])

    title_text = isnothing(borough) ? "NYC foreclosure auctions by week" : "$borough foreclosure auctions by week"
    weekly_fig = Figure(size = (1400, 600))
    weekly_ax = Axis(
        weekly_fig[1, 1],
        title = title_text,
        subtitle = "Condominiums, single family homes, and vacant land only; excludes Block 1006 Lot 1302 (Hilton timeshares)",
        xlabel = "Week starting",
        ylabel = "Auctions",
        xticks = (tick_positions, tick_labels),
        xticklabelrotation = pi / 4,
    )
    barplot!(
        weekly_ax,
        weekly_counts[!, :x],
        weekly_counts[!, :count],
        color = weekly_counts[!, :color],
        gap = 0.15,
    )
    ymax = max(1, maximum(weekly_counts[!, :count]))
    ylims!(weekly_ax, 0, ymax + 1)

    save(output_path, weekly_fig)
end

function render_weekly_calendar_heatmap(weekly_counts; borough=nothing, output_path = WEEKLY_CALENDAR_HEATMAP_OUTPUT)
    weekly_counts = copy(weekly_counts)
    weekly_counts[!, :year] = year.(weekly_counts[!, :week_start])
    weekly_counts[!, :x] = week_of_year_position.(weekly_counts[!, :week_start])
    years = sort(unique(weekly_counts[!, :year]))
    max_count = max(1, maximum(weekly_counts[!, :count]))

    counts_grid = fill(0, 53, length(years))
    year_to_row = Dict(y => idx for (idx, y) in enumerate(years))
    for row in eachrow(weekly_counts)
        counts_grid[row.x, year_to_row[row.year]] = row.count
    end

    month_starts = [Date(2025, month, 1) for month in 1:12]
    tick_positions = week_of_year_position.(month_starts)
    tick_labels = Dates.format.(month_starts, dateformat"u")

    # Use a portrait 4:5 canvas so the output fits social feeds cleanly.
    weekly_fig = Figure(size = (1080, 1350), fontsize = 28)
    title_text = isnothing(borough) ? "NYC foreclosure auctions calendar heatmap" : "$borough foreclosure auctions calendar heatmap"
    weekly_ax = Axis(
        weekly_fig[1, 1],
        title = title_text,
        subtitle = "Weekly totals for condominiums/townhomes excluding timeshares",
        xlabel = "Week starting",
        xticks = (tick_positions, tick_labels),
        yticks = (collect(1:length(years)), string.(years)),
        yreversed = true,
        titlesize = 42,
        subtitlesize = 24,
        xlabelsize = 28,
        xticklabelsize = 24,
        yticklabelsize = 28,
    )

    heatmap!(
        weekly_ax,
        1:53,
        1:length(years),
        counts_grid,
        colormap = cgrad(["#ebedf0", "#9be9a8", "#40c463", "#30a14e", "#216e39"]),
        colorrange = (0, max_count),
        interpolate = false,
    )

    xlims!(weekly_ax, 0.5, 53.5)
    ylims!(weekly_ax, 0.5, length(years) + 0.5)
    hidespines!(weekly_ax, :r, :t)
    rowgap!(weekly_fig.layout, 40)
    rowsize!(weekly_fig.layout, 1, Relative(0.76))
    # rowsize!(weekly_fig.layout, 2, Relative(0.08))
    Colorbar(
        weekly_fig[2, 1],
        limits = (0, max_count),
        colormap = cgrad(["#ebedf0", "#9be9a8", "#40c463", "#30a14e", "#216e39"]),
        vertical = false,
        label = "Auction lots per week",
        labelsize = 28,
        ticklabelsize = 24,
    )

    save(output_path, weekly_fig)
end

auction_window_end = today() + Month(2)
auction_window_start = auction_window_end - Month(12)
query_window_start = min(auction_window_start, WEEKLY_START_DATE)

fmt_date(d) = Dates.format(d, ISO_DATEFORMAT)

auction_qry = """
SELECT
    lots.case_number,
    lots.borough,
    boroughs.id as borough_id,
    boroughs.code as borough_code,
    lots.block,
    lots.lot,
    lots.address AS lot_address,
    lots.BBL,
    lots.unit,
    cases.auction_date,
    cases.case_name,
    bids.judgement,
    bids.upset_price,
    bids.winning_bid,
    CASE WHEN bids.winning_bid > 100 THEN bids.winning_bid - bids.upset_price END AS over_bid,
    pluto.Address AS pluto_address,
    pluto.ZipCode,
    substr(building_class.name, 0, instr(building_class.name, ':')) as LandUse,
    building_class.name as BldgClass,
    pluto.OwnerName,
    pluto.YearBuilt,
    pluto.YearAlter1,
    pluto.YearAlter2,
    pluto.LotArea,
    pluto.BldgArea
FROM lots
JOIN cases ON cases.case_number = lots.case_number AND cases.borough = lots.borough
LEFT JOIN bids ON bids.case_number = lots.case_number
    AND bids.auction_date = cases.auction_date
    AND bids.borough = lots.borough
JOIN pluto ON pluto.BBL = lots.BBL
JOIN building_class ON pluto.BldgClass = building_class.id
JOIN boroughs on lots.borough = boroughs.name
WHERE cases.auction_date > :start_date AND cases.auction_date <= :end_date;
"""


dbh = SQLite.DB(DB_PATH)

auction_stmt = SQLite.Stmt(dbh, auction_qry)
auctions = DataFrame(DBInterface.execute(
    auction_stmt;
    start_date = fmt_date(query_window_start),
    end_date = fmt_date(auction_window_end),
))

auctions[!, :BldgClass] = strip_bldgclass_label.(auctions[!, :BldgClass])
auctions[!, :auction_date] = parse_iso_date.(auctions[!, :auction_date])
SQLite.close(dbh)

scatter_auctions = subset(
    copy(auctions),
    :BldgClass => ByRow(x -> x ∈ SCATTER_BLDG_CLASSES),
    :auction_date => ByRow(x -> !ismissing(x) && x >= auction_window_start),
)

boro_auctions = combine(groupby(scatter_auctions, :borough), :case_number => length => :count)

completed_auctions = dropmissing(scatter_auctions, :winning_bid)
sold_auctions = filter(:winning_bid => >(100), completed_auctions)
sold_narrow = stack(
    sold_auctions,
    [:judgement, :winning_bid, :upset_price],
    [:case_number, :borough],
    variable_name = "result_type",
    value_name = "amount",
)

boro_sales = combine(groupby(sold_auctions, :borough),
    :winning_bid => length => :sold_count,
    :judgement => mean,
    :upset_price => mean,
    :winning_bid => mean,
    [:upset_price, :judgement] => ((u, j) -> mean(u ./ j)) => :avg_upset_to_judgement,
    [:upset_price, :winning_bid] => ((u, w) -> mean(filter(isfinite, w ./ u))) => :avg_overbid,
)

boro_completes = combine(groupby(completed_auctions, :borough), nrow => :completed)

df = outerjoin(boro_auctions, boro_completes, boro_sales, on = :borough)
df[:, r"mean|count"] = coalesce.(df[:, r"mean|count"], 0)

scatter_sales = subset(copy(sold_auctions), :winning_bid => ByRow(<=(4.0e6)))
maxy = ceil(max(scatter_sales.winning_bid...) / 5e5) * 500
axis = (
    width = 225,
    height = 225,
    xlabel = "Opening Bid (\$000s)",
    ylabel = "Winning Bid (\$000s)",
    limits = ((0, maxy), (0, maxy)),
)
result_overbid = data(scatter_sales) * mapping(
    :upset_price => (t -> t / 1000) => "Opening Bid (\$000s)",
    :winning_bid => (t -> t / 1000) => "Winning Bid (\$000s)",
)
plt = result_overbid * mapping(layout = :borough, marker = :BldgClass)

line_data = DataFrame(x = [0, maxy])
line_45 = data(line_data) * mapping(:x => identity, :x => identity) * visual(Lines,
    linestyle = :dash,
    linewidth = 1,
)

res = AlgebraOfGraphics.draw(plt + line_45; axis = axis)
save("stats.png", res)

borough = getarg("--borough")
weekly_counts = weekly_sales_counts(auctions, borough)
render_weekly_bar_chart(weekly_counts; borough=borough)

if hasarg(CALENDAR_HEATMAP_ARG)
    render_weekly_calendar_heatmap(weekly_counts; borough=borough)
end
