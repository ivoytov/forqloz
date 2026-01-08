using Dates, DataFrames, AlgebraOfGraphics, CairoMakie, Statistics, SQLite, DBInterface
set_aog_theme!()

const DB_PATH = normpath(joinpath(@__DIR__, "web", "foreclosures", "foreclosures.sqlite"))
const ISO_DATEFORMAT = dateformat"yyyy-mm-dd"
const INCLUDED_BLDG_CLASSES = Set([
    "CONDOMINIUMS",
    "ONE FAMILY DWELLINGS",
    "TWO FAMILY DWELLINGS",
    "VACANT LAND",
])

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


auction_window_end = Date("2025-12-31")
auction_window_start = auction_window_end - Month(12)

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
JOIN bids ON bids.case_number = lots.case_number
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
    start_date = fmt_date(auction_window_start),
    end_date = fmt_date(auction_window_end),
))

auctions[!, :BldgClass] = strip_bldgclass_label.(auctions[!, :BldgClass])
subset!(auctions,
    :BldgClass => ByRow(x -> x ∈ INCLUDED_BLDG_CLASSES)
)

SQLite.close(dbh)


auctions[!, :auction_date] = parse_iso_date.(auctions[!, :auction_date])

boro_auctions = combine(groupby(auctions, :borough), :case_number => length => :count)

completed_auctions = dropmissing(auctions, :winning_bid)
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

filter!(:winning_bid => <=(4.0e6), sold_auctions)
maxy = ceil(max(sold_auctions.winning_bid...) / 5e5) * 500
axis = (
    width = 225,
    height = 225,
    xlabel = "Opening Bid (\$000s)",
    ylabel = "Winning Bid (\$000s)",
    limits = ((0, maxy), (0, maxy)),
)
result_overbid = data(sold_auctions) * mapping(
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