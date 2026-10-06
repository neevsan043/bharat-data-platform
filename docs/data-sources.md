# Data Source Feasibility Check (Phase 0, Task 1)

**Verification dates:** 2026-10-05 (evening) through 2026-10-06 (early hours, IST).
**Method:** every URL below was fetched live during verification using `curl` with a browser
User-Agent (plus `WebFetch` cross-checks where noted). Large files were sampled with HTTP
range requests (`curl -r 0-65535`) and the saved bytes were checked with `file`. The NPCI
site was additionally opened in a real browser session (its pages are invisible to curl).
No URL in this document is quoted from memory or from a search snippet without an observed
HTTP response; where that was impossible the source is marked FAIL or the URL is labelled
"not verified".

**Status rubric:**

| Status | Meaning |
|---|---|
| PASS | Machine-readable file or API downloadable right now, no account or key. |
| NEEDS-KEY | Works, but requires a registered API key/token. |
| MANUAL | Data exists and is public, but only reachable via human/browser steps or PDF-only. |
| FAIL | Endpoint dead, replaced, or could not be verified at all. |

**Summary (9 entries across the 7 requested source families):**

| # | Source | Status | Format | Key? | Update cadence | Machine-readable? |
|---|---|---|---|---|---|---|
| 1 | NPCI — UPI product statistics | MANUAL | xlsx/JSON via browser | no | monthly | browser only (bot-blocked) |
| 2 | World Bank open data API | PASS | JSON | no | indicator-level | yes |
| 3 | RBI — CIMS / DBIE portals | MANUAL | xlsx via browser UI | no | varies by series | no (SPA) |
| 4 | Grid-India (formerly POSOCO) | PASS | PDF (some xlsx/csv), JSON listing | no | daily/monthly | yes |
| 5 | Open-Meteo | PASS | JSON | no | continuous (hourly models) | yes |
| 6 | data.gov.in | FAIL | — | untestable | — | no (CKAN API retired) |
| 7 | TRAI publications | MANUAL | PDF only | no | monthly/quarterly/yearly | no |
| 8 | OpenCelliD | NEEDS-KEY | CSV (+XML API) | yes — free account | daily changes / snapshots | yes, with token |
| 9 | PLFS (MoSPI) | MANUAL | unit-level files behind login | no (free registration) | quarterly/annual | no (login wall) |

Verified sample files are kept in `data/samples/` (git-ignored) and under
`/tmp/bdp_phase0/` for reference.

---

## 1. NPCI — UPI product statistics — **MANUAL**

- **Page (verified in browser):** `https://www.npci.org.in/product/upi/product-statistics`
  — page title "Unified Payments Interface (UPI) Product Statistics | NPCI".
  The older path `https://www.npci.org.in/what-we-do/upi/product-statistics` returns a 404
  page inside the site.
- **Live data observed in browser:** monthly table rows, e.g. `September-2026: 756 banks
  live, 24,068.38 Mn volume, 29,37,396.67 Cr value`. Tabs exist for Daily Statistics,
  Monthly Statistics, Uptime, Downtime/Incidents, IPO Mandate statistics; a financial-year
  selector (2026-27) and a Download button are present.
- **Data endpoints discovered in the page's XHR traffic** (both keyless, both confirmed
  as the calls the SPA makes):
  - table data: `https://www.npci.org.in/api/product-statistic/tab/detail?product_name=upi&tab_name=product-statistics-upi&year_range=2026-27&excel_type=monthly&page_no=1&page_size=10&locale=en`
  - Excel export (triggered by the Download button):
    `https://www.npci.org.in/api/products-statistics-files?product_name=upi&tab_name=product-statistics-upi&year_range=2026-27&excel_type=monthly&locale=en`
- **Why MANUAL:** Akamai bot protection blocks every non-browser client. `curl` on the
  pages *and* on both API endpoints returns `403 text/html` (~430-byte block page:
  "You don't have permission to access … on this server"), even with a browser UA,
  `Accept` and `Referer` headers. The same URLs succeed inside a real browser session.
- **Format:** xlsx (via the export endpoint); JSON also available through the table API.
- **API key:** none used by the site; the barrier is bot protection, not authentication.
- **Update frequency:** monthly rows observed on the page; no frequency sentence found to quote.
- **Approx size:** unverified (download is browser-only); the in-browser export was not
  intercepted. Expect a small file (single spreadsheet per product/year).
- **Licence:** no licence statement found on the NPCI pages fetched.
- **Ingestion implication:** treat as a **monthly manual file drop** (browser download into
  `data/landing/npci/`), or later automate via headless-browser fetch if the allowed
  toolset is ever extended.
- **Landing data received (2026-10-06):** six per-FY monthly exports (2021-22 … 2026-27,
  downloaded from the site by the user) placed in `data/landing/npci/`, ~17–18 KB each,
  single sheet "Product Statistics" with columns: Month, Volume (In Mn.), Avg. Daily
  Volume (In Mn.), Value (In Cr.), Avg. Daily Value (In Cr.). Verified coverage:
  **June-2021 → October-2026 (57 monthly rows), but April and May are missing for each of
  2022, 2023, 2024, 2025** — every per-FY file starts at June — and **October-2026 is a
  partial month** (4 days at download time: 3,377.80 Mn = 844.45 × 4). The September-2026
  row matches the live site (value differs in the last decimal: 29,37,396.69 vs the
  site's 29,37,396.67 Cr). Silver layer must treat Apr/May 2022-25 as source gaps and
  flag partial current-month rows.
- **Re-verify (expect 403 from terminal):**
  `curl -A 'Mozilla/5.0 …Chrome/126…' 'https://www.npci.org.in/api/product-statistic/tab/detail?product_name=upi&tab_name=product-statistics-upi&year_range=2026-27&excel_type=monthly&page_no=1&page_size=10&locale=en'`

## 2. World Bank open data API — **PASS**

- **Working URLs (200, JSON, keyless):**
  - `https://api.worldbank.org/v2/country/IND/indicator/NY.GDP.MKTP.CD?format=json&per_page=3`
    → India GDP (current US$): 2025 = 3,956,067,115,771.63; series `total: 66` observations;
    page metadata carries `"lastupdated":"2026-07-13"`.
  - `https://api.worldbank.org/v2/country/IND/indicator/FP.CPI.TOTL.ZG?format=json&per_page=3`
    → CPI inflation, same shape.
  - Indicator metadata also keyless: `https://api.worldbank.org/v2/indicator/NY.GDP.MKTP.CD?format=json`
- **Format:** JSON; standard pagination (`page`, `per_page`, `total`).
- **API key:** not required.
- **Update frequency:** each response carries a `lastupdated` timestamp (2026-07-13 for the
  GDP series); no per-indicator frequency string is present in the data responses.
- **Approx size:** ~0.7 KB per 2-3 observations; full India series = 66 rows (a few KB).
  Yearly full-history pulls for a handful of indicators stay well under 1 MB.
- **Licence (quoted from `https://data.worldbank.org/summary-terms-of-use`, fetched):**
  "you are free to copy, distribute, adapt, display or include the data … in other products
  for commercial or noncommercial purposes at no cost under a Creative Commons Attribution
  4.0 International License." Attribution required.
- **Problems:** none.
- **Ingestion implication:** clean REST batch ingestion; annual/quarterly refresh.
- **Re-verify:** `curl -sS 'https://api.worldbank.org/v2/country/IND/indicator/NY.GDP.MKTP.CD?format=json&per_page=2'`

## 3. RBI — CIMS / DBIE — **MANUAL**

- **Portals (both 200 but JS-only shells):**
  - `https://cims.rbi.org.in/` → 200, 3,254-byte Angular shell; login-first ("CIMSLOGIN").
  - `https://data.rbi.org.in` → 200, 50,732-byte Angular shell (Database on Indian
    Economy, DBIE). Routes visible in its JS bundle: `/dbie/home`, `/dbie/reports`,
    `/dbie/popular-data`, `/dbie/searchresult`.
- **Why MANUAL:** no direct xlsx/CSV URL is reachable without an interactive session. The
  one API path found in the bundle (`/api/services/queryInterface`) 404s on plain GET and
  is used by the app with POST payloads that cannot be discovered without a rendered
  browser. No guessed endpoints were tested.
- **Format:** xlsx served through the DBIE UI (per the UI's design); nothing machine-fetchable verified.
- **API key:** no key system; interactive browser required.
- **Update frequency / size:** not stated on any fetched page; unknown.
- **Licence:** no licence statement found on either portal.
- **Manual path (documented for a human):** open `https://data.rbi.org.in` → DBIE portal →
  Reports or Popular Data → pick table → download xlsx.
- **Ingestion implication:** occasional manual file drop; treat as optional supplement to
  World Bank for macro-financial series (policy rates, banking aggregates).

## 4. Grid-India / POSOCO — electricity grid reports — **PASS**

- **Old domain dead:** `https://posoco.in/reports/` → **502 Bad Gateway** (3 attempts).
  The organisation (renamed Grid Controller of India) lives at:
- **Portal:** `https://grid-india.in/reports/` → 200, but a JS SPA (empty `<div id="root">`
  shell; links invisible to curl/WebFetch).
- **Keyless JSON file-listing API** (discovered in the site's JS bundle; verified 200):
  `POST https://webapi.grid-india.in/api/v1/file` with JSON body
  `{"_source":"GRDW","_type":"DAILY_FREQUENCY_PROFILE","_fileDate":"","_month":"00"}`
  → `200 application/json`, ~2.3 MB listing. Entries carry `Title_`, `MimeType`,
  `FilePath`, `SizeInKb`, `CreatedOn` — e.g. first entry: "Frequency 04.10.26",
  `application/pdf`, 985 KB, created 05-10-2026 00:39.
- **Direct CDN files (verified):** `https://webcdn.grid-india.in/files/grdw/2026/10/Frequency%2004.10.26_897.pdf`
  → `206` on a range request, full size 1,008,218 bytes, `PDF document, version 1.5,
  1 pages`. Files are served from Grid-India's own CDN — **not** Google Drive.
- **Report taxonomy observed in the API:** `DAILY_FREQUENCY_PROFILE`, `DAILY_VRE_REPORT`,
  `DAILY_AS_REPORT`, `DAILY_PSP_REPORT`, `DAILY_PSM_REPORTS`, `MONTHLY_GDAM_DATA`,
  `MONTHLY_GRID_DISTURBANCE`, `REPORTS_MONTHLY_REPORT`. The `MimeType` field shows some
  report types are xlsx/csv rather than PDF.
- **API key:** none required.
- **Update frequency:** daily reports observed (the 04.10 file appeared at 00:39 next day);
  monthly report families also present. No frequency sentence is printed on the site.
- **Approx size:** daily frequency PDF ≈ 1 MB; listing JSON ≈ 2.3 MB per report type.
- **Licence:** no licence statement found on grid-india.in.
- **Problems:** the SPA hides everything from plain HTTP; the listing API is undocumented
  (reverse-engineered from the app bundle — use gently, low request volume).
- **Ingestion implication:** scheduled batch pull — query the listing API for a report type,
  download the newest file(s) from `webcdn.grid-india.in`. Prefer the monthly xlsx/csv
  report types for tabular data; daily PDFs where needed.

## 5. Open-Meteo — weather history & forecast — **PASS**

- **Working URLs (200, JSON, keyless):**
  - Forecast: `https://api.open-meteo.com/v1/forecast?latitude=28.6139&longitude=77.2090&hourly=temperature_2m&past_days=2`
    (Delhi hourly, ~5.5 KB per 3 days).
  - Historical archive: `https://archive-api.open-meteo.com/v1/archive?latitude=28.6139&longitude=77.2090&start_date=2024-06-01&end_date=2024-06-02&daily=temperature_2m_max,temperature_2m_min&timezone=Asia%2FKolkata`
    → returned 2024-06-01 max 44.0 °C / min 31.3 °C, timezone Asia/Kolkata.
- **Format:** JSON.
- **API key:** not required. Docs state the `apikey` parameter is "Only required to
  commercial use to access reserved API resources for customers."
- **Update frequency:** not stated on fetched pages; the forecast API serves hourly model
  data and the archive updates day by day (observed).
- **Rate limits (quoted from `https://open-meteo.com/en/terms`):** "Less than 10'000 API
  calls per day, 5'000 per hour and 600 per minute" for the free non-commercial API;
  "You may only use the free API services for non-commercial purposes."
- **Licence (quoted from the same terms page):** "The data obtained through the API is
  provided under the terms of the CC-BY 4.0 licence."
- **Problems:** none for this project's scale (a few cities × daily pulls ≪ limits).
- **Ingestion implication:** micro-batch — daily incremental refresh per city plus a
  one-time historical backfill per city. This is the platform's "streaming-adjacent" feed
  (Kafka is banned by constraint; scheduled micro-batches fill that role).

## 6. data.gov.in — Government open data portal — **FAIL** (keyless machine access)

- **What still works:** the portal serves HTML (`https://data.gov.in/` → 200, ~1.2 MB),
  a keyless RSS feed of new catalogs (`https://data.gov.in/backend/dms/v1/rss.xml` → 200,
  `application/rss+xml`), individual catalog pages
  (e.g. `https://data.gov.in/catalog/mahatma-gandhi-national-rural-employment-guarantee-act-mgnrega`
  → 200), and the licence PDF
  (`https://data.gov.in/sites/default/files/Gazette_Notification_OGDL.pdf` → 200, 1,179,227 B,
  scanned, no extractable text).
- **Decisive test that failed:** the classic CKAN catalog API is retired.
  `https://data.gov.in/api/3/action/package_search?q=telecom&rows=3` → 301 to www, then
  either the site's `page_not_found` HTML (1 MB) or `403` with body "Request is currently
  forbidden due to authorization failed." — never JSON without a key.
- **Key-gated path untestable:** the new Nuxt front-end calls its JSON APIs
  (`/ogdp/catalog/…`, `/api-export/catalog/…`) with bearer tokens (`fetchWithBearerToken`
  in the bundle); no live resource ID or key-issuance page could be verified this session,
  so even NEEDS-KEY status cannot be honestly granted.
- **Portal self-description (quoted from the footer of fetched pages):** "This is a sandbox
  environment created for testing and demonstration purposes only".
- **Licence:** footer states content "is owned by the respective Ministry/State/Department/
  Organization and licensed under the Government Open Data License - India" (GODL). The
  full GODL text page is JS-rendered; the Gazette PDF is a scan, so no licence clause
  sentence is quotable.
- **Why FAIL:** no verifiable machine-readable download path exists right now — keyless or
  keyed — and the portal itself declares it is a sandbox. Re-check in a later phase; if it
  matures, telecom/employment datasets may return through it.
- **Note:** TRAI-mirrored or PLFS datasets could **not** be located on the portal this
  session (search is JS-only; catalog API dead).

## 7. TRAI — telecom regulator publications — **MANUAL**

- **Working pages (200):**
  - `https://www.trai.gov.in/release-publication/reports/performance-indicators-reports`
    (note: plural "indicators"; the singular path 404s)
  - `https://www.trai.gov.in/release-publication/reports/telecom-subscriptions-reports`
  - Latest releases verified by range-request download:
    - `https://www.trai.gov.in/sites/default/files/2026-10/YIR_05102026.pdf` → 206, PDF
      v1.7 (Yearly Performance Indicators 2025-26, 3.77 MB per the listing, released 2026-10)
    - `https://www.trai.gov.in/sites/default/files/2026-06/QPIR_22062026.pdf` → 206, PDF
      v1.7, 171 pages (quarterly report, 4.6 MB per the listing)
- **Why MANUAL:** every listing on both pages is a "Download PDF" link; no xlsx/csv/zip is
  offered anywhere checked. Monthly "Telecom Subscription Data" releases (~6.12 MB PDFs)
  are likewise PDF-only.
- **Format:** PDF only.
- **API key:** none needed.
- **Update cadence (observed from the listings):** monthly subscription data; quarterly
  performance indicator reports; yearly performance indicator report.
- **Approx size:** 1.2–13.1 MB per PDF (per the pages' own size labels); samples taken at
  64 KB.
- **Licence:** no licence statement found on the TRAI publication pages.
- **Ingestion implication:** quarterly manual file drop of 1–2 PDFs; tables extracted
  page-wise (chunked PDF parsing) into bronze Parquet.

## 8. OpenCelliD — cell-tower database — **NEEDS-KEY**

- **Working pages (200):** `https://opencellid.org/`, `https://opencellid.org/downloads`,
  `https://docs.opencellid.org/docs/attribution`, `https://docs.opencellid.org/docs/api/overview`,
  `https://docs.opencellid.org/docs/downloads/overview`, `https://my.opencellid.org/register`
  (read only — no account was created during verification).
- **Token gate (quoted from the downloads page):** "Enter your API access token to see
  download links." and "Country exports, the worldwide database and daily changes are
  available with an API access token. Create an account" (→ `my.opencellid.org/register`,
  which says "Sign-up to contribute & download from the database.").
- **Gate proven without an account:** documented endpoint
  `GET https://opencellid.org/cell/get` without a token →
  `401 text/xml`, body `<rsp stat="fail"><err info="API Key not known: " code="2"/></rsp>`.
- **Format:** CSV for downloads ("CSV exports only include cells observed in the last 18
  months" — downloads page); XML/KML for the lookup API.
- **API key:** yes — free account at `my.opencellid.org/register`. Whether it is free for
  **non-commercial** use is *not stated* on any fetched page (the separate commercial
  Unwired Labs API is linked elsewhere); this remains to be confirmed at sign-up time.
- **Update frequency:** not stated as an explicit sentence; the site advertises "daily
  changes" exports alongside country/worldwide snapshots.
- **Approx size:** unknown — download links are hidden behind the token.
- **Licence (quoted from `https://docs.opencellid.org/docs/attribution`):** "OpenCellID
  data is licensed under CC BY-SA 4.0" — attribution + ShareAlike; "Products or services
  using data derived from OpenCelliD need to visibly credit OpenCelliD."
- **Token configured (2026-10-06):** stored as `OPENCELLID_TOKEN` in `.env` (git-ignored)
  and verified live — `GET /cell/getInArea?key=…&BBOX=28.6250,77.2100,28.6350,77.2200&limit=3&format=json`
  → `200` with 3 Delhi cells (UMTS/LTE, mcc 404). Sample:
  `data/samples/opencellid_getinarea_delhi.json`. A keyless call returns `401 "API Key
  not known"` while a bad-but-present key format returns `400 "Invalid input data"`, so
  the two failure modes are distinguishable.
- **Area-query limit (verified):** `BBOX` may cover at most 4,000,000 sq. m — larger
  boxes return `400 "BBOX too big - Limit to 4,000,000 sq.mts."`. Area-based ingestion
  must tile queries; full snapshots via the tokened downloads page avoid this.
- **Ingestion implication:** batch snapshot download (with token from `.env`) — e.g.
  quarterly full snapshot or India country export; feed GeoPandas for the telecom
  flagship spatial analytics.

## 9. PLFS — Periodic Labour Force Survey (MoSPI) — **MANUAL**

- **Working pages (200):**
  - `https://microdata.gov.in/nada/index.php/catalog` and `.../catalog?q=PLFS`
  - PLFS catalog entries found: `/catalog/292` ("unit-level data of PLFS for quarterly
    (Apr-June 2025 to Oct-Dec 2025)"), `/catalog/291` (monthly unit-level Apr–Dec 2025),
    `/catalog/254` (key employment/unemployment indicators 2024)
  - `/catalog/292/get-microdata` → 200 and states exactly: **"To access data for this
    study, user must be logged in. Click on the links below to login or register for a
    free account."**
- **Why MANUAL:** unit-level data sits behind a free registration + login; report PDFs on
  `https://www.mospi.gov.in/` could not be reached — the site is a JS-only shell (2.6 KB
  HTML, no links via curl or WebFetch).
- **TLS warning:** `microdata.gov.in` serves a broken/self-signed chain — `curl` fails
  certificate verification (exit 60) unless run with `-k`. Any automated client must
  consciously handle this; a browser will warn.
- **Format:** unit-level files behind the login (format not visible pre-login).
- **API key:** none; free account required.
- **Update cadence:** not stated on fetched pages; catalog shows quarterly and monthly
  unit-level releases for 2025 plus annual key-indicator sets.
- **Approx size:** unknown (links gated); PLFS unit-level data is historically large
  (hundreds of MB) — a memory-budget concern if ever ingested.
- **Licence:** no licence statement found on the NADA pages.
- **Ingestion implication:** excluded from the Phase 0 source list; revisit only if a
  labour deep-dive phase is added (manual download after registration, then chunked
  parsing).

---

## Bottom line for Task 2

The strict PASS/NEEDS-KEY pool is **4 sources**: World Bank, Grid-India, Open-Meteo,
OpenCelliD. Because the platform's ingestion design explicitly includes a **file-drop
pattern**, the strongest MANUAL sources (NPCI UPI monthly Excel via browser export, TRAI
PDFs) enter the final list as documented manual drops. See `docs/source-selection.md` for
the final selection and rationale.
