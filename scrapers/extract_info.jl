using CSV, DataFrames, ProgressMeter, OCReract, Dates, Printf, OpenAI, Base64, JSON3, DotEnv

DotEnv.load!()

# Function to prompt with a default answer
function prompt(question, default_answer)
    print("$question [$default_answer]: ")
    input = readline()
    input == "q" && return nothing
    if input == ""
        if isnothing(default_answer)
            return nothing
        end
        return string(default_answer)
    end
    return string(input)
end

# Function to extract matches based on a pattern
function extract_pattern(text, patterns)
    for pattern in patterns
        m = match(pattern, text)
        if m !== nothing
            return length(m.captures) > 0 ? m.captures[1] : m
        end
    end
    return nothing
end

# Function to extract address
function extract_address(text)
    patterns = [
        r"(?:premises known(?:\sas)?|(?:building|property) located at)\s((.+?)(?:,?\s+(N\.?Y\.?|New\s?York(?! Avenue)))(\s+\d{5})?)"i,
        r"(?:building located at|property located at|(?<!formerly )\bknown(?:\sas)?|described as follows:?(?!\s*See|\s*beginning|\s*All that)|(?:prem\.|premises)\s*k\/a|lying and being at|street address of)\s((.+?)(?:,?\s+(N\.?Y\.?|New\s?York(?! Avenue)))(\s+\d{5})?)"i,
    ]
    return extract_pattern(text, patterns)
end


# Function to extract block
function extract_block(text)
    patterns = [
        r"\bBlock[:\s]+(\d{1,5})\b"i, 
        r"SBL\.?:?\s*(\d{3,5})-\d{1,4}"i,
        r"(?<!\(\d{3}\))\s(\d{3,5})-(\d{1,4})[.)]"
    ]
    return extract_pattern(text, patterns)
end

# Function to extract lot
function extract_lot(text)
    patterns = [
        r"\bLot(?:\(?s?\)?| No\.?)[:\s]+(\d{1,4})"i, 
        r"SBL\.?:?\s*(\d{3,5})-\d{1,4}"i,
        r"(?<!\(\d{3}\))\s\d{3,5}-(\d{1,4})[.)]"
    ]
    return extract_pattern(text, patterns)
end

function detect_multiple_lots(text)
    patterns = [
        r"\b\d{1,4}\s?(?:&|and)\s?\d{1,4}"i,
        r"\b(lot:? )(\d{1,4}).+?\b(lot:? )(?!(\2))\d{1,4}"i,
        r"\blots?:?\s\d{1,4},\s\d{1,4}"i,
    ]
    return extract_pattern(text, patterns)
end


function detect_time_share(text)
    patterns = [
        r"\bHNY CLUB SUITES\b"i,
        r"\bVACATION SUITES\b"i,
    ]
    return extract_pattern(text, patterns)
end

# Extract text from PDF
function extract_text_from_pdf(pdf_path)
    case_number = basename(pdf_path)[1:end-4]
    image_path = case_number * ".png"
    text_path = case_number # .txt gets appended automatically

    # Call the GraphicsMagick command
    run(pipeline(`gm convert -append -density 330 $pdf_path $image_path`, stdout=devnull, stderr=devnull))

    
    run_tesseract(image_path, text_path, lang="eng", user_defined_dpi=330)
    text = read(text_path, String)
    rm(image_path)
    rm(text_path)
    return text
end

function extract_llm_values(pdf_path)
    case_number = basename(pdf_path)[1:end-4]
    image_path = case_number * ".png"

    # Call the GraphicsMagick command
    try
        run(pipeline(`gm convert -append -density 330 $pdf_path $image_path`, stdout=devnull, stderr=devnull))
    catch e
        println("Error running gm convert: $e")
        return nothing
    end
    
    # Read the image and encode it to Base64
    image_data = read(image_path)
    rm(image_path)
    base64_image = base64encode(image_data)
    
    provider = OpenAI.OpenAIProvider(
        api_key=ENV["OPENAI_API_KEY"],
    )

    
    r = create_chat(
        provider,
        "gpt-4o-mini",
        [Dict("role" => "user", "content" => [
            Dict("type" => "text", "text" => "I need help extracting the judgment, upset price, and the sale price (winning bid) from this document. Return the answer in JSON format like this: { \"judgement\": 100000, \"upset_price\": 200000, \"winning_bid\": 300000 }"),
            Dict("type" => "image_url", "image_url" => Dict("url" => "data:image/png;base64," * base64_image))
        ])];
        max_tokens = 300,
        response_format = Dict("type" => "json_object")
    )

    parsed_response = r.response[:choices][1][:message][:content] |> JSON3.read

    # Access structured data
    judgement = prompt("Enter judgement:", parsed_response["judgement"])
    upset_price = prompt("Enter upset price:", parsed_response["upset_price"])
    winning_bid = prompt("Enter winning bid:", parsed_response["winning_bid"])

    if isnothing(judgement) || judgement == "" || isnothing(winning_bid) || winning_bid == "" || isnothing(upset_price) || upset_price == ""
        println("Error: Missing values in the response.")
        return nothing
    end

    return (
        judgement=parse(Float64, judgement), 
        upset_price=parse(Float64,upset_price), 
        winning_bid=parse(Float64, winning_bid)
    )
end

# Prompt for winning bid
function prompt_for_winning_bid(foreclosure_case)
    case_number = foreclosure_case.case_number

    # Extract text from PDF manually
    filename = replace(case_number, "/" => "-") * ".pdf"
    pdf_path = joinpath("web/saledocs/surplusmoney", filename)
    run(`open "$pdf_path"`)
        
    prices = extract_llm_values(pdf_path)

    if isnothing(prices)
        println("Error extracting values from $pdf_path")
        return
    end

    run(`osascript -e 'tell application "Preview" to close window 1'`)

    row = (
        case_number=case_number, 
        borough=foreclosure_case.borough, 
        auction_date=foreclosure_case.auction_date,
        judgement=prices.judgement,
        upset_price=prices.upset_price,
        winning_bid=prices.winning_bid
    )

    # append the row to the bids CSV file
    CSV.write("web/foreclosures/bids.csv", DataFrame([row]); append=true, header=false)
end

# Get auction results
function get_auction_results()
    # Read the cases file
    cases = CSV.read("web/foreclosures/cases.csv", DataFrame)
    bids_path = "web/foreclosures/bids.csv"
    bids = CSV.read(bids_path, DataFrame)

    # Read in which files exist
    files = readdir("web/saledocs/surplusmoney") .|> x -> replace(x[1:end-4], "-" => "/")

    filter!(row -> row.auction_date < today(), cases)
    filter!(row -> row.case_number in files, cases)

    cases = antijoin(cases, bids, on=[:case_number, :borough])
    sort!(cases, order(:auction_date, rev=true))


    prompt_for_winning_bid.(eachrow(cases))


    println("CSV file bids.csv has been updated with missing bid results values.")
end

notice_of_sale_path(case_number) = joinpath("web/saledocs/noticeofsale", replace(case_number, "/" => "-") * ".pdf")

# Extract block/lot/address from file
function parse_notice_of_sale(pdf_path)
    # Extract text from PDF
    text = try
        extract_text_from_pdf(pdf_path)
    catch e
        println("Error extracting text from $pdf_path: $e")
        return nothing
    end
    text = replace(text, "\n" => " ")

    if !isnothing(detect_time_share(text))
        return (
            block=1006, 
            lot=1302,
            address= missing
        )
    end

    if !isnothing(detect_multiple_lots(text))
        println("Multiple lots detected in $pdf_path")
    end

    block, lot = extract_block(text), extract_lot(text)

    if ("-i" ∈ ARGS || haskey(ENV, "WSS")) && (isnothing(block) || isnothing(lot))
        # Open the PDF file with the default application on macOS
        run(`open "$pdf_path"`)

        block = prompt("Enter block:", block)
        lot = prompt("Enter lot:", lot)

        run(`osascript -e 'tell application "Preview" to close window 1'`)
    end
    

    if isnothing(block) || isnothing(lot)
        println("Error: Missing block or lot in $pdf_path")
        return nothing
    end

    return (
        block=parse(Int, block),
        lot=parse(Int, lot),
        address=extract_address(text),
    )
end

# Get block and lot
function get_block_and_lot()
    # Read the cases file
    cases = CSV.read("web/foreclosures/cases.csv", DataFrame)
    lots_path = "web/foreclosures/lots.csv"
    lots = CSV.read(lots_path, DataFrame)

    # Read in which files exist
    files = readdir("web/saledocs/noticeofsale") .|> x -> replace(x[1:end-4], "-" => "/")

    filter!(row -> row.case_number in files, cases)

    sort!(cases, order(:auction_date, rev=true))

    new_cases = antijoin(cases, lots, on=[:case_number, :borough])

    for case in eachrow(new_cases)
        pdf_path = notice_of_sale_path(case.case_number)
        values = parse_notice_of_sale(pdf_path)
        if isnothing(values)
            continue
        end

        row = (
            case_number=case.case_number, 
            borough=case.borough, 
            block=values.block, 
            lot=values.lot, 
            address=isnothing(values.address) ? missing : values.address,
            bbl=missing,
            unit=missing,
        )
        printstyled(@sprintf("%12s block %6d lot %5d address %s\n", row.case_number, row.block, row.lot, row.address), color=:light_green)

        # append the row to the bids CSV file
        CSV.write("web/foreclosures/lots.csv", DataFrame([row]); append=true, header=false)
    end

    # Convert updated rows back to CSV
    println("CSV file has been updated with missing block and lot values.")
end


# Main function
function main()
    get_block_and_lot()
    if "-i" ∈ ARGS || haskey(ENV, "WSS")
        get_auction_results()
    end
end

main()
