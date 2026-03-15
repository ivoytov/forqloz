using DataFrames, Dates, CSV, GLM, Statistics, JSON3, HTTP, Printf, SQLite, DBInterface, ProgressMeter

const DB_PATH = normpath(joinpath(@__DIR__, "web", "foreclosures", "foreclosures.sqlite"))


# Function to read CSV file into DataFrame
read_csv(file) = CSV.read(file, DataFrame)

borough_dict = Dict(1 => "Manhattan", 2 => "Bronx", 3 => "Brooklyn", 4 => "Queens", 5 => "Staten Island")
borough_id_dict = Dict("Manhattan" => "1", "Bronx"=>"2", "Brooklyn"=>"3", "Queens" =>"4", "Staten Island"=>"5")

const PROGRESS_BAR_WIDTH = 40
const ESRI_BATCH_SIZE = 200

standard_bbl(borough, block, lot) = parse(Int, @sprintf("%s%05d%04d", borough_id_dict[borough], block, lot))

to_int(x::Integer) = Int(x)
to_int(x::AbstractString) = parse(Int, x)
to_int(x) = Int(x)

function chunk_indices(n, size)
    out = Vector{UnitRange{Int}}()
    i = 1
    while i <= n
        j = min(i + size - 1, n)
        push!(out, i:j)
        i = j + 1
    end
    return out
end

# Initialize and preprocess datasets
function initialize_data()
    base_df = read_csv("../manhattan/transactions/nyc_sales_2018-2022.csv")
    archives = [read_csv("../manhattan/transactions/nyc_sales_$(year).csv") for year in 2003:2017]
    rolling_sales = reduce(vcat, [read_csv("../manhattan/transactions/$borough.csv") for borough in ["manhattan", "bronx", "brooklyn", "queens", "statenisland"]])
    rolling_sales = filter(["SALE DATE"] => >(Date(2022, 12, 31)), rolling_sales)

    df = vcat(base_df, vcat(archives...), rolling_sales, cols=:intersect)
    df.BOROUGH = [borough_dict[id] for id in df.BOROUGH]
    df
end

function spna_building_census()
    # Available fields at https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/ArcGIS/rest/services/MAPPLUTO/FeatureServer/0
    fields = ["BoroCode", "Block", "Lot", "BBL", "Address", "OwnerName", "OwnerType", "UnitsRes", "ResArea",
          "BldgClass", "YearBuilt", "YearAlter1", "YearAlter2", "Landmark", "CondoNo"]
    blocks = (901, 900, 899, 898, 897, 896, 469, 468, 467, 453, 454, 455, 921, 922, 923, 924, 925, 926)
    # building classes https://www.nyc.gov/assets/finance/jump/hlpbldgcode.html
    qry = "BoroCode = 1 and (Block in $blocks and (UnitsRes >= 6 and BldgClass not in ('C6', 'D4')))"
    ans = esri_query(url, fields, qry);

    # Extract attributes from features
    attrs = [feat["attributes"] for feat in ans]

    function nothing_to_missing!(df,col)
         for i in eachindex(df[!,col])
           if isnothing(df[i, col])
             df[i, col] = missing
           end
         end
    end

    # Build DataFrame with stable column order
    df = DataFrame(attrs)
    for c in names(df)
        df[!, c] = replace(df[!, c], nothing => missing)
    end

    # registrations from https://data.cityofnewyork.us/Housing-Development/Multiple-Dwelling-Registrations/tesw-yqqr/about_data
    reg_df = read_csv("registrations.csv")
    # registration contacts from https://data.cityofnewyork.us/Housing-Development/Registration-Contacts/feu5-w2e2/about_data
    contacts_df = read_csv("contacts.csv")
    df2 = innerjoin(df, reg_df, on=[:BoroCode => :BoroID, :Block => :Block, :Lot => :Lot]);
    innerjoin(df2, contacts_df, on=:RegistrationID)
    CSV.write("building_census_contacts.csv", df)
end



function enrich_bbls_and_pluto()
    # Load lots from DB
    dbh = SQLite.DB(DB_PATH)
    auctions = DataFrame(DBInterface.execute(dbh, "SELECT * FROM lots"))

    # Fill in missing BBL / unit information
    need_update = filter(row -> ismissing(row.BBL) || ((row.lot > 1000) && ismissing(row.unit)), auctions)
    if nrow(need_update) > 0
        condo_requests = [(r.borough, r.block, r.lot) for r in eachrow(need_update) if r.lot > 1000]
        unique!(condo_requests)
        condo_map = condo_base_bbl_keys(condo_requests)
        base_keys = unique([v[1] for v in values(condo_map)])
        billing_map = condo_billing_bbls(base_keys)
        DBInterface.execute(dbh, "BEGIN")
        try
            @showprogress dt=1 desc="Updating lots" barlen=PROGRESS_BAR_WIDTH for r in eachrow(need_update)
                new_bbl = standard_bbl(r.borough, r.block, r.lot)
                new_unit = missing
                if r.lot > 1000
                    key = (borough_id_dict[r.borough], r.block, r.lot)
                    if haskey(condo_map, key)
                        base_key, unit = condo_map[key]
                        billing = get(billing_map, base_key, missing)
                        if !ismissing(billing)
                            new_bbl = billing
                            new_unit = unit
                        end
                    end
                end
                DBInterface.execute(dbh,
                    "UPDATE lots SET BBL = ?, unit = ? WHERE case_number = ? AND borough = ?",
                    (new_bbl, new_unit, r.case_number, r.borough))
            end
            DBInterface.execute(dbh, "COMMIT")
        catch
            DBInterface.execute(dbh, "ROLLBACK")
            rethrow()
        end
        # Reload auctions after updates
        auctions = DataFrame(DBInterface.execute(dbh, "SELECT * FROM lots"))
    end

    # update pluto file
    pluto_data = DataFrame(DBInterface.execute(dbh, "SELECT * FROM pluto"))

    # 1. Calculate missing BBLs
    new_lots = antijoin(dropmissing(auctions, :BBL), pluto_data, on=:BBL)
    subset!(new_lots, :BBL => ByRow(!=("")))
    
    # 2. Deduplicate: Ensure we only have one row per BBL to prevent duplicate inserts
    unique!(new_lots, :BBL)

    # Iterate over each BBL in `auctions` and call the `pluto` function, storing the results in the DataFrame
    columns = ["Address", "Borough", "Block", "Lot", "ZipCode", "BldgClass", "LandUse", "BBL", "YearBuilt", "YearAlter1", "YearAlter2", "OwnerName", "LotArea", "BldgArea"]

    DBInterface.execute(dbh, "BEGIN")
    try
        bbls = [to_int(b) for b in new_lots.BBL if !ismissing(b)]
        attrs_map = pluto_bbls(bbls)
        @showprogress dt=1 desc="Fetching pluto" barlen=PROGRESS_BAR_WIDTH for bbl in new_lots.BBL
            attributes = get(attrs_map, bbl, missing)
            if attributes !== missing
                row = []
                for col in columns
                    if col == "LandUse" && !isnothing(attributes[col])
                        push!(row, parse(Int, attributes[col]))
                        continue
                    end
                    push!(row, (attributes[col]))
                end
                
                # Insert into pluto table
                sql = "INSERT INTO pluto (Address, Borough, Block, Lot, ZipCode, BldgClass, LandUse, BBL, YearBuilt, YearAlter1, YearAlter2, OwnerName, LotArea, BldgArea) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                DBInterface.execute(dbh, sql, Tuple(row))
            end
        end
        DBInterface.execute(dbh, "COMMIT")
    catch
        DBInterface.execute(dbh, "ROLLBACK")
        rethrow()
    end


    SQLite.close(dbh)
end

# Main function to calculate and export home price indices
function build_auction_sales()
    sales = initialize_data()
    # Load lots from DB
    dbh = SQLite.DB(DB_PATH)
    auctions = DataFrame(DBInterface.execute(dbh, "SELECT * FROM lots"))

    # Merge auctions and sales DataFrames
    dropmissing!(auctions, [:block, :lot])
    merged_df = innerjoin(sales, auctions, on = [:BOROUGH => :borough, :BLOCK => :block, :LOT => :lot])
    # Select only columns from sales DataFrame
    select!(merged_df, names(sales))

    # drop timeshares (condo hotels)
    exclude_prefixes = ["45", "25", "26", "28"]
    filter!(row -> !ismissing(row."BUILDING CLASS CATEGORY") &&
        all(prefix -> !startswith(row."BUILDING CLASS CATEGORY", prefix), exclude_prefixes), merged_df)
    # Ensure "SALE DATE" is stored as ISO text not BLOB
    transform!(merged_df, "SALE DATE" => ByRow(d -> Dates.format(d, dateformat"yyyy-mm-dd")) => "SALE DATE")

    # Replace auction_sales table with explicit schema to avoid BLOB types
    DBInterface.execute(dbh, "DROP TABLE IF EXISTS auction_sales")
    DBInterface.execute(dbh, join([
        "CREATE TABLE auction_sales (",
        "\"BOROUGH\" TEXT,",
        "\"NEIGHBORHOOD\" TEXT,",
        "\"BUILDING CLASS CATEGORY\" TEXT,",
        "\"TAX CLASS AT PRESENT\" TEXT,",
        "\"BLOCK\" INTEGER,",
        "\"LOT\" INTEGER,",
        "\"EASEMENT\" INTEGER,",
        "\"BUILDING CLASS AT PRESENT\" TEXT,",
        "\"ADDRESS\" TEXT,",
        "\"APARTMENT NUMBER\" TEXT,",
        "\"ZIP CODE\" INTEGER,",
        "\"RESIDENTIAL UNITS\" INTEGER,",
        "\"COMMERCIAL UNITS\" INTEGER,",
        "\"TOTAL UNITS\" INTEGER,",
        "\"LAND SQUARE FEET\" INTEGER,",
        "\"GROSS SQUARE FEET\" INTEGER,",
        "\"YEAR BUILT\" INTEGER,",
        "\"TAX CLASS AT TIME OF SALE\" INTEGER,",
        "\"BUILDING CLASS AT TIME OF SALE\" TEXT,",
        "\"SALE PRICE\" REAL,",
        "\"SALE DATE\" TEXT",
        ")"
    ]))
    # Insert the data
    SQLite.load!(merged_df, dbh, "auction_sales")
    # Recreate helpful indexes
    DBInterface.execute(dbh, "CREATE INDEX IF NOT EXISTS idx_sales_geo ON auction_sales(\"BOROUGH\", \"BLOCK\", \"LOT\")")
    DBInterface.execute(dbh, "CREATE INDEX IF NOT EXISTS idx_sales_date ON auction_sales(\"SALE DATE\")")
    SQLite.close(dbh)
end

function main()
    enrich_bbls_and_pluto()
    build_auction_sales()
end

function condo_base_bbl_key(borough, block, lot)
    lot < 1000 && throw(ArgumentError("Lot must be over 1000"))
    outfields = "UNIT_BORO, UNIT_BLOCK, UNIT_LOT, CONDO_BASE_BBL_KEY, UNIT_DESIGNATION"
    url = "https://services6.arcgis.com/yG5s3afENB5iO9fj/arcgis/rest/services/DTM_ETL_DAILY_view/FeatureServer/4"
    # query = "UNIT_BORO = 1 and UNIT_BLOCK = 459 and UNIT_LOT = 1113"
    query = "UNIT_BORO = '$(borough_id_dict[borough])' and UNIT_BLOCK=$(block) and UNIT_LOT=$(lot)"
    result = esri_query(url, outfields, query)
    isempty(result) && return missing, missing

    (
        result[1]["attributes"]["CONDO_BASE_BBL_KEY"],
        something(result[1]["attributes"]["UNIT_DESIGNATION"], missing)
    )
end

function condo_base_bbl_keys(condo_requests::AbstractVector)
    isempty(condo_requests) && return Dict{Tuple{String, Int, Int}, Tuple{Int, Union{Missing, String}}}()
    outfields = "UNIT_BORO, UNIT_BLOCK, UNIT_LOT, CONDO_BASE_BBL_KEY, UNIT_DESIGNATION"
    url = "https://services6.arcgis.com/yG5s3afENB5iO9fj/arcgis/rest/services/DTM_ETL_DAILY_view/FeatureServer/4"
    out = Dict{Tuple{String, Int, Int}, Tuple{Int, Union{Missing, String}}}()
    ranges = chunk_indices(length(condo_requests), ESRI_BATCH_SIZE)
    for r in ranges
        chunk = condo_requests[r]
        clauses = String[]
        for (borough, block, lot) in chunk
            boro_id = haskey(borough_id_dict, borough) ? borough_id_dict[borough] : string(borough)
            push!(clauses, "UNIT_BORO = '$boro_id' and UNIT_BLOCK = $block and UNIT_LOT = $lot")
        end
        query = "(" * join(clauses, ") OR (") * ")"
        result = esri_query(url, outfields, query)
        for feat in result
            attrs = feat["attributes"]
            key = (string(attrs["UNIT_BORO"]), to_int(attrs["UNIT_BLOCK"]), to_int(attrs["UNIT_LOT"]))
            base_key = to_int(attrs["CONDO_BASE_BBL_KEY"])
            unit = something(attrs["UNIT_DESIGNATION"], missing)
            out[key] = (base_key, unit)
        end
    end
    return out
end

function esri_query(url, outfields, query; format="JSON")
    params = Dict("f"=>format, "outfields"=>outfields, "where"=>query, "returnGeometry" => false)
    r = HTTP.request("POST", "$(url)/query",
                 ["Content-Type" => "application/x-www-form-urlencoded", "accept"=>"application/json"],
                 HTTP.URIs.escapeuri(params))
    json = JSON3.read(String(r.body))

    if haskey(json, :error)
        err = json[:error]
        msg = haskey(err, :message) ? err[:message] : String(r.body)
        throw(ErrorException("ESRI query failed: $msg params: $params"))
    end

    return json[:features]
end

function condo_billing_bbl(condo_base_bbl_key)
    outfields = "CONDO_BASE_BBL_KEY, CONDO_BILLING_BBL"
    url = "https://services6.arcgis.com/yG5s3afENB5iO9fj/arcgis/rest/services/DTM_ETL_DAILY_view/FeatureServer/3"
    query = "CONDO_BASE_BBL_KEY = $condo_base_bbl_key"
    result = esri_query(url, outfields, query)
    isempty(result) && return missing
    return to_int(result[1]["attributes"]["CONDO_BILLING_BBL"])
end

function condo_billing_bbls(condo_base_bbl_keys::AbstractVector{<:Integer})
    isempty(condo_base_bbl_keys) && return Dict{Int, Int}()
    outfields = "CONDO_BASE_BBL_KEY, CONDO_BILLING_BBL"
    url = "https://services6.arcgis.com/yG5s3afENB5iO9fj/arcgis/rest/services/DTM_ETL_DAILY_view/FeatureServer/3"
    key_list = join(condo_base_bbl_keys, ", ")
    query = "CONDO_BASE_BBL_KEY IN ($key_list)"
    result = esri_query(url, outfields, query)
    out = Dict{Int, Int}()
    for feat in result
        attrs = feat["attributes"]
        key = to_int(attrs["CONDO_BASE_BBL_KEY"])
        billing = to_int(attrs["CONDO_BILLING_BBL"])
        out[key] = billing
    end
    return out
end

function pluto(bbl)
    outfields = ["Address", "Borough", "Block", "Lot", "ZipCode", "BldgClass", "LandUse", "BBL", "YearBuilt", "YearAlter1", "YearAlter2", "OwnerName", "LotArea", "BldgArea"]
    url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/ArcGIS/rest/services/MAPPLUTO/FeatureServer/0"
    query = "BBL = $bbl"
    result = esri_query(url, outfields, query)
    isempty(result) && return missing
    result[1]["attributes"]
end

function pluto_bbls(bbls::AbstractVector)
    isempty(bbls) && return Dict{Int, Any}()
    outfields = ["Address", "Borough", "Block", "Lot", "ZipCode", "BldgClass", "LandUse", "BBL", "YearBuilt", "YearAlter1", "YearAlter2", "OwnerName", "LotArea", "BldgArea"]
    url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/ArcGIS/rest/services/MAPPLUTO/FeatureServer/0"
    out = Dict{Int, Any}()
    ranges = chunk_indices(length(bbls), ESRI_BATCH_SIZE)
    for r in ranges
        chunk = bbls[r]
        key_list = join(chunk, ", ")
        query = "BBL IN ($key_list)"
        result = esri_query(url, outfields, query)
        for feat in result
            attrs = feat["attributes"]
            key = to_int(attrs["BBL"])
            out[key] = attrs
        end
    end
    return out
end

function bbl(borough, block, lot)
    if lot > 1000
        key, unit_name = condo_base_bbl_key(borough, block, lot)
        !ismissing(key) && return (condo_billing_bbl(key), unit_name)
    end 
    
    (standard_bbl(borough, block, lot), missing)
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
