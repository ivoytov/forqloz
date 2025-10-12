#!/usr/bin/env node
import { connect } from 'puppeteer-core';
import { open, mkdir } from 'fs/promises'
import path from 'path'

const SBR_WS_ENDPOINT = `wss://${process.env.BRIGHTDATA_AUTH}@brd.superproxy.io:9222`;
const endpoint = process.env.WSS ?? SBR_WS_ENDPOINT;

async function ensureFileHandle(fileName) {
    fileName = fileName ?? "download.pdf";
    try {
        await mkdir(path.dirname(fileName), { recursive: true });
    } catch (e) {
        // Directory already exists or cannot be created; subsequent open() will surface fatal errors
    }
    return open(fileName, 'w');
}

async function download_via_page(page, url, fileName) {
    const fileHandle = await ensureFileHandle(fileName);
    try {
        const bytes = await page.evaluate(async (downloadUrl) => {
            const res = await fetch(downloadUrl, { credentials: 'include' });
            if (!res.ok) {
                throw new Error(`Failed to fetch PDF: ${res.status} ${res.statusText}`);
            }
            const buffer = await res.arrayBuffer();
            return Array.from(new Uint8Array(buffer));
        }, url);

        const data = Buffer.from(bytes);
        if (data.length < 1_000) {
            throw new Error('PDF content seems too small, might be corrupted.');
        }
        await fileHandle.write(data);
        console.log(`PDF downloaded successfully (${data.length} bytes)`);
    } finally {
        await fileHandle.close();
    }
}

export async function download_pdf(url, fileName, { page: existingPage } = {}) {
    console.log(`In download_pdf with url: ${url} ${fileName ? fileName.split('/').pop() : "no filename"}`); 
    if (existingPage) {
        await download_via_page(existingPage, url, fileName);
        return;
    }

    const browser = await connect({ 
        browserWSEndpoint: endpoint, 
    }); 

    let page;
    let client;
    let fileHandle;

    try {
        page = await browser.newPage(); 
        page.setDefaultNavigationTimeout(3 * 60 * 1000); 

        client = await page.createCDPSession(); 
        await client.send('Fetch.enable', { 
            patterns: [{ 
                requestStage: 'Response', 
                resourceType: 'Document', 
            }], 
        }); 

        const requestId = await new Promise((resolve, reject) => { 
            let resolved; 
            client.on('Fetch.requestPaused', async ({ requestId, responseHeaders }) => { 
                const contentTypeHeader = responseHeaders.find(header => header.name.toLowerCase() === 'content-type');
                if (contentTypeHeader && contentTypeHeader.value.includes('text/html')) {
                    reject(new Error('HTML response received instead of PDF'));
                } else {
                    if (resolved) { 
                        client.send('Fetch.continueRequest', { requestId }); 
                    } else { 
                        resolved = true; 
                        resolve(requestId); 
                    } 
                }

                const contentDisposition = responseHeaders.find(header => header.name.toLowerCase() === 'content-disposition');
                if (contentDisposition) {
                    const matchFilename = contentDisposition["value"].match(/filename="(.*)"/);
                    if (matchFilename) {
                        fileName = fileName ?? matchFilename[1];
                    }
                }
            }); 
            page.goto(url, {timeout: 2*60*1000}).catch(e => { 
                if (!resolved) { 
                    reject(e); 
                } 
            }); 
        }); 

        fileHandle = await ensureFileHandle(fileName ?? "test.pdf");

        console.log('Saving pdf stream to file...'); 
        const { stream } = await client.send('Fetch.takeResponseBodyAsStream', { requestId }); 

        let totalBytes = 0; 
        while (true) { 
            const { data, base64Encoded, eof } = await client.send('IO.read', { handle: stream }); 
            const chunk = Buffer.from(data, base64Encoded ? 'base64' : 'utf8'); 
            await fileHandle.write(chunk); 
            totalBytes += chunk.length; 
            console.log(`Got chunk: ${chunk.length} bytes, Total: ${totalBytes} bytes, EOF: ${eof}`); 
            if (eof) break; 
        } 
        await client.send('IO.close', { handle: stream }); 
        await client.send('Fetch.fulfillRequest', { 
            requestId: requestId, 
            responseCode: 200, 
            body: '' 
        }); 

        if (totalBytes < 1000) { 
            throw new Error("PDF content seems too small, might be corrupted."); 
        } 

        console.log('PDF downloaded successfully'); 
    } finally {
        if (fileHandle) {
            try {
                await fileHandle.close();
            } catch (err) {
                console.warn('Failed to close PDF file handle', err);
            }
        }
        if (page) {
            try {
                await page.close();
            } catch (err) {
                console.warn('Failed to close PDF page', err);
            }
        }
        try {
            browser.disconnect();
        } catch (err) {
            console.warn('Failed to disconnect PDF browser session', err);
        }
    }
} 

if (import.meta.url === `file://${process.argv[1]}`) {
    try {
        await download_pdf(process.argv[2], process.argv.length >= 3 ? process.argv[3] : null)
    } catch (err) {
        console.error(err.stack || err);
        process.exit(1);
    } finally {
        setTimeout(() => {
            console.log('Active handles:', process._getActiveHandles());
            console.log('Active requests:', process._getActiveRequests());
          process.exit(0);
        }, 1000); // Give it a second to complete all I/O operations
    }
}
