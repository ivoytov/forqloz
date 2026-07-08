# NYC Foreclosure tracker

## Workflow 

1. Start Chrome in WS mode `chrome`

2. Run calendar scrapers

```
WSS="{URL}" bun run scrapers/calendar.js
```

3. Scrape notice of sales

```
julia --project=. scrapers/download_case_filings.jl
```

## R2 PDF hosting

Public PDF links can be served from Cloudflare R2 while keeping `web/saledocs` as the local OCR/scrape working directory.

1. Create an R2 bucket and connect it to your custom domain, for example `https://pdfs.example.com`.
2. Configure `web/config.js` with that public base URL.
3. Configure `rclone` for your R2 bucket and set:

```
export PIPELINE_R2_PUBLISH=1
export R2_REMOTE_NAME=forqloz-r2
export R2_BUCKET=forqloz-pdfs
```

Routine upload after filing sync:

```
julia --project=. scripts/pipeline.jl sync-filings
```

One-time backfill or repair:

```
julia --project=. scripts/pipeline.jl sync-r2 --dry-run
julia --project=. scripts/pipeline.jl sync-r2
```

One-way publish without remote deletions:

```
julia --project=. scripts/pipeline.jl publish-r2
```

## Testing an individual case

You can test a single case scrape like this:

```
node scrapers/notice_of_sale.js 850044/2025 Manhattan 2026-01-28 noticeofsale
```
