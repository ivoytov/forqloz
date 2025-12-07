import { connect } from 'puppeteer-core';
import Database from 'bun:sqlite';
import path from 'path';
import { fileURLToPath } from 'url';

function sleep(s) {
    return new Promise(resolve => setTimeout(resolve, s * 1000));
}

const SBR_WS_ENDPOINT = `wss://${process.env.BRIGHTDATA_AUTH}@brd.superproxy.io:9222`;
const endpoint = process.argv.includes('--browser') ? process.argv[process.argv.indexOf('--browser') + 1] : process.env.WSS ?? SBR_WS_ENDPOINT;

console.log('Connecting to Scraping Browser...');

const boroughConfigDict = {
    "Queens": {
        courtId: "80",
        calendarId: "38968",
    },
    "Manhattan": {
        courtId: "60",
        calendarId: "38272",
    },
    "Bronx": {
        courtId: "124",
        calendarId: "38936",
    },
    "Brooklyn": {
        courtId: "46",
        calendarId: "26915",
    },
    "Staten Island": {
        courtId: "84",
        calendarId: "45221",
    },
}

let auctionLots = []
let maxDate = new Date()
maxDate.setDate(maxDate.getDate() + 21)
maxDate = maxDate.toISOString().split('T')[0]
for (const borough in boroughConfigDict) {
    try {
        const newLots = await getAuctionLots(borough, boroughConfigDict[borough], maxDate)
        if (newLots === null) {
            console.log(`Scraper for ${borough} returned null`)
            continue
        }
        console.log(`Scraped ${newLots.length} total foreclosure cases for ${borough}`)
        auctionLots = [...auctionLots, ...newLots]
    } catch (e) {
        console.warn(`${borough} scraper failed.`, e)
    }
}

// Update SQLite cases table instead of CSV
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DB_PATH = 'web/foreclosures/foreclosures.sqlite'

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL;");

// Ensure unique constraint for UPSERT
db.prepare('CREATE UNIQUE INDEX IF NOT EXISTS uq_cases_case_boro ON cases(case_number, borough)').run();

const upsertStmt = db.prepare(`
    INSERT INTO cases (case_number, borough, auction_date, case_name)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(case_number, borough) DO UPDATE SET
      auction_date = excluded.auction_date,
      case_name = excluded.case_name
`);

let upserts = 0;
db.transaction(() => {
    for (const lot of auctionLots) {
        upsertStmt.run(lot.case_number, lot.borough, lot.auction_date, lot.case_name);
        upserts++;
    }
})();

console.log(`Upserted ${upserts} foreclosure cases before ${maxDate} across all boroughs.`);
console.log('Database has been updated with new foreclosure cases via UPSERT.');
process.exit();


async function getAuctionLots(borough, { courtId, calendarId }, maxDate) {
    const browser = await connect({
        browserWSEndpoint: endpoint,
    });

    const page = await browser.newPage();

    console.log('Connected! Navigating...');
    const url = 'https://iapps.courts.state.ny.us/webcivil/FCASCalendarSearch';
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 2 * 60 * 1000 });

    await Promise.all([
        page.select('select#cboCourt', courtId),  //QUEENS Superior Court
        page.waitForNavigation({ waitUntil: 'networkidle2' })
    ])

    await page.select('select#cboCourtPart', calendarId); // FORECLOSURE AUCTION PART

    await Promise.all([
        page.waitForNavigation({ waitUntil: 'networkidle2' }),
        page.locator("input#btnFindCalendar").click(),
    ])
    if (courtId == boroughConfigDict['Queens'].courtId && endpoint != SBR_WS_ENDPOINT) {
        console.log("Waiting for captcha")
        await sleep(10)
    }
    


    if (endpoint == SBR_WS_ENDPOINT) {
        const client = await page.createCDPSession(page);
        const { status } = await client.send('Captcha.waitForSolve', {
            detectTimeout: 10 * 1000,
        });
        console.log(`Captcha status: ${status}`);
        if (status === 'solve_failed'){
            return { error: "captcha solve failed"}
        }
        // await inspect(client)
    }

    // check if there is an option to select on page
    if (await page.$("input#btnApply")) {
        page.locator("#showForm > tbody > tr:nth-child(6) > td > input:nth-child(1)").click()

        await Promise.all([
            page.locator("input#btnApply").click(),
            page.waitForNavigation({ waitUntil: 'networkidle2' })
        ])

    }

    // extract auction info
    const auctionLots = await page.evaluate(() => {
        const lots = Array.from(document.querySelectorAll('dt'));
        const parseDate = (dateString) => {
            // Step 1: Remove the single quotes
            dateString = dateString.replace(/'/g, "").trim();

            // Step 2: Remove the day of the week (Friday)
            let dateWithoutDay = dateString.replace(/^\w+ /, ""); // Removes the first word (the day)

            // Step 3: Create a Date object
            return new Date(dateWithoutDay);
        };
        res = lots.map(dt => {
            const onclickValue = dt.children[0].getAttribute('onclick');
            const rawDateStr = onclickValue.split(',').slice(6, 8).join(',');
            const newDate = parseDate(rawDateStr);
            const date = newDate.toISOString().split('T')[0];
            return {
                case_number: dt.childNodes[0].wholeText.split(' ')[2],
                auction_date: date,
                case_name: dt.children[0].text
            };
        });
        return res;

    });
    browser.disconnect();

    console.log(`Scraped ${auctionLots.length} total foreclosure cases.`)
    const filteredLots = auctionLots.filter(({ auction_date }) => auction_date < maxDate)
        .map(lot => ({ borough: borough, ...lot }))
    return filteredLots
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
