import { connect } from 'puppeteer-core';
import { download_pdf } from './download_pdf.js';
import path from 'path';
import { existsSync, appendFile } from 'fs';


const SBR_WS_ENDPOINT = `wss://${process.env.BRIGHTDATA_AUTH}@brd.superproxy.io:9222`;
const url = "https://iapps.courts.state.ny.us/nyscef/CaseSearch"


const county_map = {
    "Manhattan": "31",
    "Queens": "41",
    "Bronx": "62",
    "Brooklyn": "24",
    "Staten Island": "43",
}

export const FilingType = Object.freeze({
    // JUDGEMENT: { id: "1310", dir: "judgement" },
    NOTICE_OF_SALE: { id: "1163", dir: "noticeofsale" },
    SURPLUS_MONEY_FORM: { id: "1741", dir: "surplusmoney" }
})

function sleep(s) {
    return new Promise(resolve => setTimeout(resolve, s * 1000));
}

function missing_filings(index_number) {
    const out = []
    for (const f in FilingType) {
        const { dir } = FilingType[f]

        const filename = index_number.replace('/', '-') + ".pdf"
        const pdfPath = path.resolve(`web/saledocs/${dir}/${filename}`);
        if (!existsSync(pdfPath)) {
            out.push(FilingType[f])
        }
    }
    return out
}


export async function download_filing(index_number, county, auction_date, missingFilings, endpoint = SBR_WS_ENDPOINT,) {
    const browser = await connect({
        browserWSEndpoint: endpoint,
    });

    const page = await browser.newPage();
    // const client = await page.createCDPSession();
    await page.setRequestInterception(true);

    page.on('request', (req) => {
        if (req.resourceType() == 'stylesheet' || req.resourceType() == 'font' || req.resourceType() == 'image') {
            req.abort();
        } else {
            req.continue();
        }
    });

    await page.goto(url, { waitUntil: 'networkidle0', timeout: 2 * 60 * 1000 });

    // await inspect(client);

    try {
        await page.locator('#txtCaseIdentifierNumber').fill(index_number);
        await page.select('select#txtCounty', county_map[county]);
    } catch (e) {
        return { error: 'Failed to fill case number in search form' };
    }

    await Promise.all([
        page.locator("button[name='btnSubmit']").click(),
        page.waitForNavigation({
            waitUntil: 'networkidle2',
        }),
    ]);

    // Check for CAPTCHA before proceeding
    let maxWaitTime = 60; // seconds
    while (maxWaitTime > 0) {
        const isCaptcha = await page.evaluate(() => {
            const bodyText = document.body.innerText.toLowerCase();
            return bodyText.includes("having captcha trouble?");
        });

        if (!isCaptcha) break;

        console.log("Captcha detected. Waiting for manual solve...");
        await sleep(5); // your existing sleep function
        maxWaitTime -= 5;
    }

    if (maxWaitTime <= 0) {
        console.warn(`${index_number} captcha not solved in time`);
        return { error: "captcha timeout" };
    }


    const tableExists = await page.$("table.NewSearchResults");
    if (!tableExists) {
        console.warn(`\n\n${index_number} couldn't find a valid case with this index (table missing)`);
        return { error: 'No case found' };
    }


    try {
        await Promise.all([
            page.locator('table.NewSearchResults > tbody > tr > td > a').click(),
            page.waitForNavigation({
                waitUntil: 'networkidle0',
            })
        ])
    } catch (e) {
        console.warn(`\n\n${index_number} couldn't find a valid case with this index`)
        return { error: 'Failed to find case in CEF' };
    }

    if (endpoint == SBR_WS_ENDPOINT) {
        const client = await page.createCDPSession(page);
        const { status } = await client.send('Captcha.waitForSolve', {
            detectTimeout: 10 * 1000,
        });
        console.log(`Captcha status: ${status}`);
        if (status === 'solve_failed') {
            return { error: "captcha solve failed" }
        }
    }


    const availableFilings = await page.$$eval("select#selDocumentType > option", options => {
        return options.map(el => el.value)
    })
    // check for motion to discontinue
    if (availableFilings.includes("3664")) {
        appendFile("web/foreclosures/cases.log", `${index_number} Discontinued\n`, (err) => {
            if (err) {
                console.error('Failed to append to the file:', err);
            } else {
                console.log(`Case ${index_number} Motion to Discontinue detected`)
            }
        });
        return { error: "case discontinued" }
    }

    const filename = index_number.replace('/', '-') + ".pdf"
    for (const filing of missingFilings) {
        const { dir, id } = filing
        const pdfPath = path.resolve(`web/saledocs/${dir}/${filename}`);
        if (!existsSync(pdfPath) && availableFilings.includes(id)) {
            await page.select('select#selDocumentType', id);

            await Promise.all([
                await page.locator("input[name='btnNarrow']").click(),
                await page.waitForNavigation({
                    waitUntil: 'networkidle0',
                })
            ])

            let docs = await page.$$eval("table.NewSearchResults > tbody > tr", rows => {
                const out = []
                for (const row of rows) {
                    const link = row.querySelector('td:nth-child(2) a');
                    const received = row.querySelector('td:nth-child(3) span');
                    const subtitle = row.querySelector('td:nth-child(2) span');

                    if (link && received) {
                        out.push({
                            downloadUrl: link.href,
                            receivedDate: received.innerText.split(" ")[1],
                            subtitle: subtitle ? subtitle.innerText : null,
                        })
                    }
                }
                return out
            })
            docs = docs.reverse()


            if (docs.length == 0) {
                return { error: 'No valid document links available' };
            }

            const receivedDate = new Date(docs[0].receivedDate)
            const subtitle = docs[0].subtitle
            const downloadUrl = docs[0].downloadUrl

            // if received date is before auction date, this is not the right surplus money form
            if (auction_date && filing == FilingType.SURPLUS_MONEY_FORM && receivedDate < auction_date) {
                console.log(index_number, `Found SMF with received date ${receivedDate.toISOString().split('T')[0]}, before ${auction_date.toISOString().split('T')[0]} auction date; SKIPPING`)
                continue
            }

            // if received date is >90 days before the auction date, this is not the right notice of sale form
            const earliestDayForNoticeOfSale = new Date(auction_date)
            earliestDayForNoticeOfSale.setDate(earliestDayForNoticeOfSale.getDate() - 90)
            if (auction_date && filing == FilingType.NOTICE_OF_SALE && (receivedDate < earliestDayForNoticeOfSale || receivedDate > auction_date)) {
                console.log(index_number, `Found NOS with received date ${receivedDate.toISOString().split('T')[0]}, either after or more than 90 days before ${auction_date.toISOString().split('T')[0]} auction date; SKIPPING`)
                continue
            }

            if (filing == FilingType.NOTICE_OF_SALE && subtitle && subtitle.toLowerCase().includes('cancellation')) {
                console.log(index_number, `Found NOS with received date ${receivedDate.toISOString().split('T')[0]}, but subtitle indicates cancellation; SKIPPING`)
                continue
            }

            appendFile("web/foreclosures/download.csv", `${dir}/${filename},${downloadUrl}\n`, (err) => {
                if (err) {
                    console.error('Failed to append to the file:', err);
                }
            })
            
            download_pdf(downloadUrl, pdfPath);
            await Promise.all([
                await page.locator("input[name='btnClear']").click(),
                await page.waitForNavigation({
                    waitUntil: 'networkidle0',
                })
            ])
        }
    }

    // finish up
    await page.close()
    browser.disconnect();

}

if (import.meta.url === `file://${process.argv[1]}`) {
    const endpoint = process.env.WSS ?? SBR_WS_ENDPOINT;
    const auction_date = new Date(process.argv[4])

    const args = process.argv.slice(2, process.argv.length).join(" ")
    const county = process.argv[3] == 'Staten' ? `${process.argv[3]} ${process.argv[4]}` : process.argv[3]
    console.log(args, "Starting...")
    const missingFilings = []
    if (process.argv.includes('surplusmoney')) {
        missingFilings.push(FilingType.SURPLUS_MONEY_FORM)
    }
    if (process.argv.includes('noticeofsale')) {
        missingFilings.push(FilingType.NOTICE_OF_SALE)
    }
    await download_filing(process.argv[2], county, auction_date, missingFilings, endpoint).catch(err => {
        console.error(args, "Error processing", err);
    })
    console.log(args, "...Completed")
    process.exit()

}

async function inspect(client) {
    const { frameTree: { frame } } = await client.send('Page.getFrameTree');
    const { url: inspectUrl } = await client.send('Page.inspect', {
        frameId: frame.id,
    });
    console.log(`You can inspect this session at: ${inspectUrl}.`);
    console.log(`Scraping will continue in 10 seconds...`);
    await sleep(10);
}
