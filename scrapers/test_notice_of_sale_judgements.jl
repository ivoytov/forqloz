using CSV, DataFrames, Dates, Printf, SQLite, DBInterface

include(joinpath(@__DIR__, "extract_info.jl"))

function test_notice_of_sale_judgements(; limit::Int=100)
    dbh = db()
    cases = DataFrame()
    try
        cases = DataFrame(DBInterface.execute(dbh, "SELECT case_number, borough, auction_date FROM cases"))
        cases.auction_date = [ismissing(x) ? missing : Date(x, dateformat"yyyy-mm-dd") for x in cases.auction_date]
    finally
        SQLite.close(dbh)
    end

    filter!(row -> !ismissing(row.auction_date), cases)
    sort!(cases, order(:auction_date, rev=true))

    checked = 0
    missing = 0

    for case in eachrow(cases)
        checked >= limit && break
        pdf_path = notice_of_sale_path(case.case_number, case.auction_date)
        isfile(pdf_path) || continue

        checked += 1
        case_label = "[Case $(replace(case.case_number, "/" => "-"))]"
        printstyled(@sprintf("%3d/%3d %s %s\n", checked, limit, case_label, pdf_path), color=:light_blue)

        text = try
            extract_text_from_pdf(pdf_path)
        catch e
            println("Error extracting text from $pdf_path: $e")
            continue
        end

        if text === :gm_failed
            if is_interactive()
                run(`open "$pdf_path"`)
                resp = prompt("GM convert failed. Press Enter to continue or q to stop:", ""; prefix=case_label)
                run(`osascript -e 'tell application "Preview" to close window 1'`)
                resp === nothing && break
            end
            continue
        end

        text = replace(text, "\n" => " ")
        judgement = extract_judgement_amount(text)

        if ismissing(judgement)
            missing += 1
            println("Missing judgement amount.")
            if is_interactive()
                run(`open "$pdf_path"`)
                resp = prompt("Judgement missing. Press Enter to continue or q to stop:", ""; prefix=case_label)
                run(`osascript -e 'tell application "Preview" to close window 1'`)
                resp === nothing && break
            end
        else
            println("Found judgement: $judgement")
        end
    end

    println("Checked $checked notices of sale. Missing judgement in $missing.")
end

test_notice_of_sale_judgements()
