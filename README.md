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

## Testing an individual case

You can test a single case scrape like this:

```
node scrapers/notice_of_sale.js 850044/2025 Manhattan 2026-01-28 noticeofsale
```

