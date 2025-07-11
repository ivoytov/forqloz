using DataFrames, Dates, CSV, GLM, Statistics, JSON3, HTTP, Printf


# Function to read CSV file into DataFrame
read_csv(file) = CSV.read(file, DataFrame)

borough_dict = Dict(1 => "Manhattan", 2 => "Bronx", 3 => "Brooklyn", 4 => "Queens", 5 => "Staten Island")
borough_id_dict = Dict("Manhattan" => "1", "Bronx"=>"2", "Brooklyn"=>"3", "Queens" =>"4", "Staten Island"=>"5")


# Initialize and preprocess datasets
function initialize_data()
    base_df = read_csv("../manhattan/transactions/nyc_2018-2022.csv")
    archives = [read_csv("../manhattan/transactions/nyc_sales_$(year).csv") for year in 2003:2017]
    rolling_sales = reduce(vcat, [read_csv("../manhattan/transactions/$borough.csv") for borough in ["manhattan", "bronx", "brooklyn", "queens", "statenisland"]])
    rolling_sales = filter(["SALE DATE"] => >(Date(2022, 12, 31)), rolling_sales)

    df = vcat(base_df, vcat(archives...), rolling_sales, cols=:intersect)
    df.BOROUGH = [borough_dict[id] for id in df.BOROUGH]
    df
end

# Main function to calculate and export home price indices
function main()
    sales = initialize_data()
    lot_path = "web/foreclosures/lots.csv"
    auctions = read_csv(lot_path)

    updated_auctions = filter(row -> ismissing(row.BBL) || ((row.lot > 1000) && ismissing(row.unit)), auctions)
    transform!(updated_auctions, [:borough, :block, :lot] => ByRow(bbl) => [:BBL, :unit])
    filter!(row -> !(ismissing(row.BBL) || ((row.lot > 1000) && ismissing(row.unit))), auctions)
    auctions = vcat(auctions, updated_auctions)
    CSV.write(lot_path, auctions)

    # update pluto file
    pluto_path = "web/foreclosures/pluto.csv"
    pluto_data = read_csv(pluto_path)
    new_lots = antijoin(dropmissing(auctions, :BBL), pluto_data, on=:BBL)

    # Iterate over each BBL in `auctions` and call the `pluto` function, storing the results in the DataFrame
    columns = ["Address", "Borough", "Block", "Lot", "ZipCode", "BldgClass", "LandUse", "BBL", "YearBuilt", "YearAlter1", "YearAlter2", "OwnerName", "LotArea", "BldgArea"]

    for bbl in new_lots.BBL
        attributes = pluto(bbl)
        if attributes !== missing
            row = []
            for col in columns
                if col == "LandUse" && !isnothing(attributes[col])
                    push!(row, parse(Int, attributes[col]))
                    continue
                end
                push!(row, (attributes[col]))
            end
            
            push!(pluto_data, row; promote=true)
        end
    end
    CSV.write(pluto_path, pluto_data; transform=(col, val) -> something(val, missing))


    # Merge auctions and sales DataFrames
    dropmissing!(auctions, [:block, :lot])    
    merged_df = innerjoin(sales, auctions, on = [:BOROUGH => :borough, :BLOCK => :block, :LOT => :lot])
    # Select only columns from sales DataFrame
    select!(merged_df, names(sales))

    # drop timeshares (condo hotels)
    exclude_prefixes = ["45", "25", "26", "28"]
    filter!(row -> !ismissing(row."BUILDING CLASS CATEGORY") &&
        all(prefix -> !startswith(row."BUILDING CLASS CATEGORY", prefix), exclude_prefixes), merged_df)    
    CSV.write("web/foreclosures/auction_sales.csv", merged_df)
end

function condo_base_bbl_key(borough, block, lot)
    lot < 1000 && throw(ArgumentError("Lot must be over 1000"))
    outfields = "CONDO_BASE_BBL_KEY, UNIT_DESIGNATION"
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

function esri_query(url, outfields, query; format="JSON")
    params = Dict("f"=>format, "outfields"=>outfields, "where"=>query, "returnGeometry" => false)
    r = HTTP.request("POST", "$(url)/query",
                 ["Content-Type" => "application/x-www-form-urlencoded", "accept"=>"application/json"],
                 HTTP.URIs.escapeuri(params))
    json = JSON3.read(String(r.body))
    return json.features
end

function condo_billing_bbl(condo_base_bbl_key)
    outfields = "CONDO_BILLING_BBL"
    url = "https://services6.arcgis.com/yG5s3afENB5iO9fj/arcgis/rest/services/DTM_ETL_DAILY_view/FeatureServer/3"
    query = "CONDO_BASE_BBL_KEY = $condo_base_bbl_key"
    result = esri_query(url, outfields, query)
    isempty(result) && return missing
    return parse(Int,result[1]["attributes"]["CONDO_BILLING_BBL"])
end

function pluto(bbl)
    outfields = ["Address", "Borough", "Block", "Lot", "ZipCode", "BldgClass", "LandUse", "BBL", "YearBuilt", "YearAlter1", "YearAlter2", "OwnerName", "LotArea", "BldgArea"]
    url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/ArcGIS/rest/services/MAPPLUTO/FeatureServer/0"
    query = "BBL = $bbl"
    result = esri_query(url, outfields, query)
    isempty(result) && return missing
    result[1]["attributes"]
end

function bbl(borough, block, lot)
    if lot > 1000
        key, unit_name = condo_base_bbl_key(borough, block, lot)
        !ismissing(key) && return (condo_billing_bbl(key), unit_name)
    end 
    
    (parse(Int, @sprintf("%s%05d%04d", borough_id_dict[borough], block, lot)), missing)
end

main()



