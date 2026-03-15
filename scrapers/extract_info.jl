using CSV, DataFrames, ProgressMeter, OCReract, Dates, Printf, OpenAI, Base64, JSON3, DotEnv, SQLite, DBInterface

DotEnv.load!()

const DB_PATH = normpath(joinpath(@__DIR__, "..", "web", "foreclosures", "foreclosures.sqlite"))
function db()
    conn = SQLite.DB(DB_PATH)
    DBInterface.execute(conn, "PRAGMA journal_mode=DELETE;")
    return conn
end

function is_interactive()
    return ("-i" ∈ ARGS || haskey(ENV, "WSS")) && !("--no-interaction" ∈ ARGS)
end

# Function to prompt with a default answer
function prompt(question, default_answer; prefix=nothing)
    label = isnothing(prefix) ? "" : string(prefix, " ")
    print("$label$question [$default_answer]: ")
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
        r"(?<!\(\d{3}\))\s(\d{3,5})-(\d{1,4})[.)]",
        r"tax map identification,?\s-?(\d{3,5})-\d{1,4}"i,
        
    ]
    return extract_pattern(text, patterns)
end

# Function to extract lot
function extract_lot(text)
    patterns = [
        r"\bLot(?:\(?s?\)?| No\.?)[:\s]+(\d{1,4})"i, 
        r"SBL\.?:?\s*(\d{3,5})-\d{1,4}"i,
        r"(?<!\(\d{3}\))\s\d{3,5}-(\d{1,4})[.)]",
        r"tax map identification,?\s-?\d{3,5}-(\d{1,4})"i,

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
    try
        run(pipeline(`gm convert -append -density 330 $pdf_path $image_path`, stdout=devnull, stderr=devnull))
    catch e
        println("Error running gm convert: $e")
        return :gm_failed
    end

    
    run_tesseract(image_path, text_path, lang="eng", user_defined_dpi=330)
    text = read(text_path, String)
    rm(image_path)
    rm(text_path)
    return text
end

function llm_extract_values(pdf_path)
    case_number = basename(pdf_path)[1:end-4]
    image_path = case_number * ".png"

    # Call the GraphicsMagick command
    try
        run(pipeline(`gm convert -append -density 330 $pdf_path $image_path`, stdout=devnull, stderr=devnull))
    catch e
        println("Error running gm convert: $e")
        return :gm_failed
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
        "gpt-5-mini",
        [Dict("role" => "user", "content" => [
            Dict("type" => "text", "text" => "Extracting the amount of final judgement of foreclosure, upset price, and the sale price of property (winning bid) from this document. If the purchaser is specified as Plaintiff, then return the sale price as \$100 exactly (even if a different amount is listed on the form). Return the answer in JSON format like this: { \"judgement\": 100000, \"upset_price\": 200000, \"winning_bid\": 300000 }"),
            Dict("type" => "image_url", "image_url" => Dict("url" => "data:image/png;base64," * base64_image))
        ])];
        response_format = Dict("type" => "json_object")
    )

    parsed_response = r.response[:choices][1][:message][:content] |> JSON3.read
    return (
        judgement=parsed_response["judgement"],
        upset_price=parsed_response["upset_price"],
        winning_bid=parsed_response["winning_bid"]
    )
end

# Prompt for winning bid
function prompt_for_winning_bid(foreclosure_case; llm_values=nothing)
    case_number = foreclosure_case.case_number

    filename = replace(case_number, "/" => "-") * ".pdf"
    pdf_path = joinpath("web/saledocs/surplusmoney", filename)
        
    case_label = "[Case $(replace(case_number, "/" => "-"))]"
    defaults = isnothing(llm_values) ? (judgement=nothing, upset_price=nothing, winning_bid=nothing) : llm_values
    if is_interactive()
        run(`open "$pdf_path"`)
    end
    judgement = is_interactive() ? prompt("Enter judgement:", defaults.judgement; prefix=case_label) : defaults.judgement
    upset_price = is_interactive() ? prompt("Enter upset price:", defaults.upset_price; prefix=case_label) : defaults.upset_price
    winning_bid = is_interactive() ? prompt("Enter winning bid:", defaults.winning_bid; prefix=case_label) : defaults.winning_bid

    if isnothing(judgement) || judgement == "" || isnothing(winning_bid) || winning_bid == "" || isnothing(upset_price) || upset_price == ""
        println("Error: Missing values in the response.")
        return
    end

    if is_interactive()
        run(`osascript -e 'tell application "Preview" to close window 1'`)
    end

    row = (
        case_number=case_number, 
        borough=foreclosure_case.borough, 
        auction_date=foreclosure_case.auction_date,
        judgement=parse(Float64, judgement),
        upset_price=parse(Float64, upset_price),
        winning_bid=parse(Float64, winning_bid)
    )

    # insert the row into the bids table
    dbh = db()
    try
        sql = "INSERT INTO bids (case_number, borough, auction_date, judgement, upset_price, winning_bid) VALUES (?, ?, ?, ?, ?, ?)"
        DBInterface.execute(dbh, sql, (row.case_number, row.borough, string(row.auction_date), row.judgement, row.upset_price, row.winning_bid))
    finally
        SQLite.close(dbh)
    end
end

# Get auction results
function get_auction_results()
    # Read cases and bids from SQLite
    dbh = db()
    cases, bids = DataFrame(), DataFrame()
    try
        cases = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough, auction_date FROM cases"))
        # Parse auction_date strings to Date for filtering/sorting
        cases.auction_date = [ismissing(x) ? missing : Date(x, dateformat"yyyy-mm-dd") for x in cases.auction_date]
        bids = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough FROM bids"))
    finally
        SQLite.close(dbh)
    end

    # Read in which files exist
    files = readdir("web/saledocs/surplusmoney") .|> x -> replace(x[1:end-4], "-" => "/")

    filter!(row -> row.auction_date < today(), cases)
    filter!(row -> row.case_number in files, cases)

    cases = antijoin(cases, bids, on=[:case_number, :borough])
    sort!(cases, order(:auction_date, rev=true))


    concurrency = tryparse(Int, get(ENV, "LLM_CONCURRENCY", "4"))
    concurrency = (concurrency === nothing || concurrency < 1) ? 1 : concurrency
    sem = Base.Semaphore(concurrency)
    tasks = Dict{String, Task}()

    for foreclosure_case in eachrow(cases)
        filename = replace(foreclosure_case.case_number, "/" => "-") * ".pdf"
        pdf_path = joinpath("web/saledocs/surplusmoney", filename)
        tasks[foreclosure_case.case_number] = Threads.@spawn begin
            Base.acquire(sem)
            try
                return llm_extract_values(pdf_path)
            finally
                Base.release(sem)
            end
        end
    end

    for foreclosure_case in eachrow(cases)
        task = tasks[foreclosure_case.case_number]
        llm_values = try
            fetch(task)
        catch e
            println("Error extracting values for $(foreclosure_case.case_number): $e")
            nothing
        end
        if llm_values === :gm_failed
            if is_interactive()
                println("Falling back to manual entry for $(foreclosure_case.case_number) due to gm convert failure.")
                prompt_for_winning_bid(foreclosure_case; llm_values=nothing)
            end
            continue
        end
        prompt_for_winning_bid(foreclosure_case; llm_values=llm_values)
    end


    println("Database table 'bids' has been updated with missing bid results values.")
end

# Build path to Notice of Sale PDF for a given case number and auction date.
# Files now live at web/saledocs/noticeofsale/YYYY-MM-DD/<case>.pdf, but we keep
# backward-compatible fallbacks to older flat naming schemes.
function notice_of_sale_path(case_number::AbstractString, auction_date)
    base = replace(case_number, "/" => "-")
    file_name = base * ".pdf"
    root = joinpath("web", "saledocs", "noticeofsale")

    if !isdir(root)
        return joinpath(root, file_name)
    end

    if auction_date !== missing
        dated_dir = joinpath(root, Dates.format(auction_date, dateformat"yyyy-mm-dd"))
        dated_path = joinpath(dated_dir, file_name)
        if isfile(dated_path)
            return dated_path
        end
    end

    # Search dated subdirectories newest-first for a matching file.
    dated_dirs = readdir(root; join=true)
    candidates = Tuple{Date,String}[]
    for dir_path in dated_dirs
        isdir(dir_path) || continue
        folder_name = basename(dir_path)
        dir_date = tryparse(Date, folder_name, dateformat"yyyy-mm-dd")
        dir_date === nothing && continue
        candidate = joinpath(dir_path, file_name)
        if isfile(candidate)
            push!(candidates, (dir_date, candidate))
        end
    end
    if !isempty(candidates)
        sort!(candidates; by=first, rev=true)
        _, path = first(candidates)
        return path
    end

    # If no dated directory had the file, look in any remaining subdirectories.
    for dir_path in dated_dirs
        isdir(dir_path) || continue
        candidate = joinpath(dir_path, file_name)
        if isfile(candidate)
            return candidate
        end
    end

    # Legacy structure: web/saledocs/noticeofsale/base-YYYY-MM-DD.pdf
    if auction_date !== missing
        legacy_dated = joinpath(root, string(base, "-", Dates.format(auction_date, dateformat"yyyy-mm-dd"), ".pdf"))
        if isfile(legacy_dated)
            return legacy_dated
        end
    end

    # Fallback: legacy undated name in root
    return joinpath(root, file_name)
end

# Extract block/lot/address from file
function parse_notice_of_sale(pdf_path; prompt_prefix=nothing)
    # Extract text from PDF
    text = try
        extract_text_from_pdf(pdf_path)
    catch e
        println("Error extracting text from $pdf_path: $e")
        return nothing
    end
    if text === :gm_failed
        if is_interactive()
            run(`open "$pdf_path"`)
            block = prompt("Enter block:", nothing; prefix=prompt_prefix)
            lot = prompt("Enter lot:", nothing; prefix=prompt_prefix)
            run(`osascript -e 'tell application "Preview" to close window 1'`)
            if isnothing(block) || isnothing(lot)
                println("Error: Missing block or lot in $pdf_path")
                return nothing
            end
            return (
                block=parse(Int, block),
                lot=parse(Int, lot),
                address=missing,
            )
        end
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

    if is_interactive() && (isnothing(block) || isnothing(lot))
        # Open the PDF file with the default application on macOS
        run(`open "$pdf_path"`)

        block = prompt("Enter block:", block; prefix=prompt_prefix)
        lot = prompt("Enter lot:", lot; prefix=prompt_prefix)

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
    # Read the cases and lots tables
    dbh = db()
    cases, lots = nothing, nothing
    try
        cases = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough, auction_date FROM cases"))
        cases.auction_date = [ismissing(x) ? missing : Date(x, dateformat"yyyy-mm-dd") for x in cases.auction_date]
        lots = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough FROM lots"))
    finally
        SQLite.close(dbh)
    end

    # Read which case_numbers have any NOS file present (support both legacy and new dated folder names)
    files = String[]
    for (root, _, filelist) in walkdir("web/saledocs/noticeofsale")
        for f in filelist
            if endswith(lowercase(f), ".pdf")
                stem = f[1:end-4]
                # Legacy dated filename: base-YYYY-MM-DD.pdf -> strip suffix
                if occursin(r"-\d{4}-\d{2}-\d{2}$", stem)
                    stem = stem[1:end-11]
                end
                push!(files, replace(stem, "-" => "/"))
            end
        end
    end

    filter!(row -> row.case_number in files, cases)

    sort!(cases, order(:auction_date, rev=true))

    new_cases = antijoin(cases, lots, on=[:case_number, :borough])

    for case in eachrow(new_cases)
        pdf_path = notice_of_sale_path(case.case_number, case.auction_date)
        case_label = "[Case $(replace(case.case_number, "/" => "-"))]"
        values = parse_notice_of_sale(pdf_path; prompt_prefix=case_label)
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

        # insert the row into the lots table
        dbh2 = db()
        try
            sql = "INSERT INTO lots (case_number, borough, block, lot, address, BBL, unit) VALUES (?, ?, ?, ?, ?, ?, ?)"
            addr = ismissing(row.address) ? nothing : row.address
            DBInterface.execute(dbh2, sql, (row.case_number, row.borough, row.block, row.lot, addr, nothing, nothing))
        finally
            SQLite.close(dbh2)
        end
    end

    # Convert updated rows back to CSV
    println("Database table 'lots' has been updated with missing block and lot values.")
end


# Main function
function main()
    get_block_and_lot()
    if is_interactive() || "--no-interaction" ∈ ARGS
        get_auction_results()
    end
end

main()
