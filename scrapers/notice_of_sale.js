import { connect, TimeoutError } from 'puppeteer-core';
import { download_pdf } from './download_pdf.js';
import path from 'path';
import { existsSync, appendFile } from 'fs';


const LOCAL_CHROME_URL = "http://localhost:9222";
const url = "https://iapps.courts.state.ny.us/nyscef/CaseSearch"
const BLOCKED_RESOURCE_TYPES = new Set();
const DOM_TIMEOUT = 15_000;
const NAVIGATION_TIMEOUT = 15_000;


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

class CloudflareBlockError extends Error {
    constructor() {
        super('Cloudflare bot blocker detected; terminating scraper');
        this.name = 'CloudflareBlockError';
    }
}

async function waitForDomNavigation(page, timeout = NAVIGATION_TIMEOUT) {
    try {
        await page.waitForNavigation({
            waitUntil: 'domcontentloaded',
            timeout,
        });
        return true;
    } catch (err) {
        if (err instanceof TimeoutError) {
            return false;
        }
        throw err;
    }
}

async function throwIfCloudflareBlocked(page) {
    const blocked = await page.evaluate(() => {
        const title = document.title.toLowerCase();
        const text = (document.body?.innerText || '').toLowerCase();
        const hasCloudflareMarker = title.includes('cloudflare') || text.includes('cloudflare');
        const hardBlock = [
            'sorry, you have been blocked',
            'you are unable to access',
            'error 1012',
            'error 1015',
            'you are being rate limited',
        ].some(phrase => text.includes(phrase));
        const challenge = [
            'just a moment',
            'attention required',
            'verify you are human',
            'checking your browser',
            'performing security verification',
        ].some(phrase => title.includes(phrase) || text.includes(phrase));

        return hardBlock || (hasCloudflareMarker && challenge);
    });

    if (blocked) {
        throw new CloudflareBlockError();
    }
}

async function waitForAnyKey(message) {
    if (!process.stdin.isTTY) {
        console.warn('stdin is not a TTY; cannot pause for manual captcha solve');
        return;
    }

    console.log(message);
    process.stdin.resume();
    process.stdin.setEncoding('utf8');

    const previousRawMode = process.stdin.isRaw;
    if (typeof process.stdin.setRawMode === 'function') {
        process.stdin.setRawMode(true);
    }

    await new Promise((resolve) => {
        const onData = (chunk) => {
            process.stdin.off('data', onData);
            if (chunk === '\u0003') {
                if (typeof process.stdin.setRawMode === 'function') {
                    process.stdin.setRawMode(Boolean(previousRawMode));
                }
                process.stdin.pause();
                process.kill(process.pid, 'SIGINT');
                return;
            }
            resolve();
        };
        process.stdin.on('data', onData);
    });

    if (typeof process.stdin.setRawMode === 'function') {
        process.stdin.setRawMode(Boolean(previousRawMode));
    }
    process.stdin.pause();
    console.log('Continuing...');
}

async function getSearchGateState(page) {
    return page.evaluate(() => {
        const bodyText = document.body.innerText.toLowerCase();
        const includesAny = (phrases) => phrases.some((phrase) => bodyText.includes(phrase));

        return {
            hasCaptcha: includesAny([
                'having captcha trouble?',
                'verify you are human',
                'security check',
                'please complete the security check',
                'press and hold',
            ]),
            hasResultsTable: !!document.querySelector('table.NewSearchResults'),
            hasNoResults: includesAny([
                'no cases were found',
                'search returned no results',
                'no matches were found',
                'no records found',
            ]),
        };
    });
}

async function waitForSearchGateState(page, timeoutSeconds = 10) {
    const startedAt = Date.now();
    while ((Date.now() - startedAt) < timeoutSeconds * 1000) {
        await throwIfCloudflareBlocked(page);
        const state = await getSearchGateState(page);
        if (state.hasResultsTable || state.hasNoResults || state.hasCaptcha) {
            return state;
        }
        await sleep(1);
    }
    return getSearchGateState(page);
}

function missing_filings(index_number, auction_date) {
    const out = []
    for (const f in FilingType) {
        const { dir } = FilingType[f]

        const dateStr = (dir === FilingType.NOTICE_OF_SALE.dir && auction_date)
            ? new Date(auction_date).toISOString().split('T')[0]
            : null;
        const baseName = `${index_number.replace('/', '-')}.pdf`;
        const relPath = (dir === FilingType.NOTICE_OF_SALE.dir && dateStr)
            ? `${dir}/${dateStr}/${baseName}`
            : `${dir}/${baseName}`;
        const pdfPath = path.resolve(`web/saledocs/${relPath}`);
        if (!existsSync(pdfPath)) {
            out.push(FilingType[f])
        }
    }
    return out
}


export async function download_filing(index_number, county, auction_date, missingFilings, endpoint = LOCAL_CHROME_URL,) {
    const browser = await connect({
        browserURL: endpoint,
    });

    const pages = await browser.pages();
    const page = pages[0] ?? await browser.newPage();
    const reusedExistingPage = pages.length > 0;
    page.setDefaultNavigationTimeout(60_000);
    page.setDefaultTimeout(DOM_TIMEOUT);
    await page.setRequestInterception(true);
    page.on('request', request => {
        const requestUrl = request.url();
        const isFavicon = /favicon/i.test(requestUrl);
        const shouldBlock = isFavicon || BLOCKED_RESOURCE_TYPES.has(request.resourceType());
        const action = shouldBlock ? request.abort() : request.continue();
        action.catch(() => {});
    });

    let cleanedUp = false;
    const cleanup = async () => {
        if (cleanedUp) return;
        cleanedUp = true;
        try {
            if (!reusedExistingPage) {
                await page.close();
            }
        } catch (err) {
            console.warn(index_number, 'Failed to close case page', err);
        }
        try {
            browser.disconnect();
        } catch (err) {
            console.warn(index_number, 'Failed to disconnect browser session', err);
        }
    };

    const finish = async (result) => {
        await cleanup();
        return result;
    };
    // const client = await page.createCDPSession();

    try {
        // The court site can leave its favicon request open for tens of seconds.
        // The page is usable once its DOM is loaded; waiting for zero network
        // connections makes every navigation pay for that unrelated request.
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAVIGATION_TIMEOUT });
        await throwIfCloudflareBlocked(page);

        // await inspect(client);

        try {
            await page.locator('#txtCaseIdentifierNumber').fill(index_number);
            await page.select('select#txtCounty', county_map[county]);
        } catch (e) {
            return finish({ error: 'Failed to fill case number in search form' });
        }

        await Promise.all([
            page.locator("button[name='btnSubmit']").click(),
            waitForDomNavigation(page),
        ]);
        await throwIfCloudflareBlocked(page);

        let gateState = await waitForSearchGateState(page);
        if (gateState.hasCaptcha) {
            await waitForAnyKey(`Solve captcha/search for ${index_number}, then press any key to continue.`);
            gateState = await waitForSearchGateState(page);
        }
        if (!gateState.hasResultsTable) {
            // console.warn(`\n\n${index_number} couldn't find a valid case with this index (table missing)`);
            return finish({ error: 'No case found' });
        }


        try {
            await Promise.all([
                page.locator('table.NewSearchResults > tbody > tr > td > a').click(),
                waitForDomNavigation(page),
            ])
            await throwIfCloudflareBlocked(page);
            await page.waitForSelector('select#selDocumentType', { timeout: DOM_TIMEOUT });
        } catch (e) {
            if (e instanceof CloudflareBlockError) {
                throw e;
            }
            // console.warn(`\n\n${index_number} couldn't find a valid case with this index`)
            return finish({ error: 'Failed to find case in CEF' });
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
            return finish({ ok: true })
        }

        for (const filing of missingFilings) {
            const { dir, id } = filing
            const dateStr = (filing === FilingType.NOTICE_OF_SALE && auction_date)
                ? auction_date.toISOString().split('T')[0]
                : null;
            const baseName = `${index_number.replace('/', '-')}.pdf`;
            const relPath = (filing === FilingType.NOTICE_OF_SALE && dateStr)
                ? `${dir}/${dateStr}/${baseName}`
                : `${dir}/${baseName}`;
            const pdfPath = path.resolve(`web/saledocs/${relPath}`);
            if (!existsSync(pdfPath) && availableFilings.includes(id)) {
                await page.waitForSelector('select#selDocumentType', { timeout: DOM_TIMEOUT });
                await page.select('select#selDocumentType', id);
                await page.waitForSelector("input[name='btnNarrow']", { timeout: DOM_TIMEOUT });

                const narrowNavigation = (async () => {
                    const navigationCompleted = await waitForDomNavigation(page);
                    if (navigationCompleted) {
                        await throwIfCloudflareBlocked(page);
                    }
                    return navigationCompleted;
                })();

                const [navigationCompleted] = await Promise.all([
                    narrowNavigation,
                    page.locator("input[name='btnNarrow']").click(),
                ]);
                if (!navigationCompleted) {
                    console.warn(index_number, 'Timeout waiting for document filter navigation; checking current page');
                }
                try {
                    await page.waitForSelector("table.NewSearchResults", { timeout: DOM_TIMEOUT });
                    await throwIfCloudflareBlocked(page);
                } catch (err) {
                    if (err instanceof CloudflareBlockError) {
                        throw err;
                    }
                    console.warn(index_number, 'Results table did not refresh after document filter');
                }

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
                    return finish({ ok: true });
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

                await download_pdf(downloadUrl, pdfPath, { page });

                let clearButton;
                try {
                    clearButton = await page.waitForSelector("input[name='btnClear']", { timeout: 15_000 });
                } catch (err) {
                    console.warn(index_number, 'Clear button not available after download; aborting remaining filings');
                    break;
                }

                if (clearButton) {
                    try {
                        const [navigationCompleted] = await Promise.all([
                            waitForDomNavigation(page, 5_000),
                            clearButton.evaluate(btn => btn.click()),
                        ]);
                        if (!navigationCompleted) {
                            await page.waitForSelector('select#selDocumentType', { timeout: 5_000 });
                        }
                        await throwIfCloudflareBlocked(page);
                    } catch (err) {
                        if (err instanceof CloudflareBlockError) {
                            throw err;
                        }
                        console.warn(index_number, 'Failed to reset document filter after download', err);
                        break;
                    } finally {
                        await clearButton.dispose();
                    }
                }
            }
        }

        return finish({ ok: true });
    } catch (err) {
        await cleanup();
        throw err;
    }
}

if (import.meta.url === `file://${process.argv[1]}`) {
    const endpoint = LOCAL_CHROME_URL;
    let auction_date = new Date(process.argv[4]);
    if (isNaN(auction_date)) auction_date = null;

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
    const benignErrors = new Set([
        "case discontinued",
        "no valid document links available",
    ]);
    let exitCode = 0;
    try {
        const result = await download_filing(process.argv[2], county, auction_date, missingFilings, endpoint);
        if (result?.error) {
            const normalized = result.error.toLowerCase();
            if (benignErrors.has(normalized)) {
                console.log(args, `Benign error encountered: ${result.error}`);
            } else {
                console.error(args, "Finished with error:", result.error);
                exitCode = 1;
            }
        }
    } catch (err) {
        console.error(args, "Error processing", err);
        exitCode = 1;
    }
    // console.log(args, "...Completed")
    process.exit(exitCode)

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
