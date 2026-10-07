# Current-term coverage

Checked against local files on **2026-10-07**.

Current term means **2025–26**, except **AL: 2023–26** and **NJ/VA: 2026–27**.

**Legend:** ✓ done/present · ◐ partial or ported but unverified · ✗ not done/missing

- **Scraper — ✓:** a successful current-generation run is recorded and the retained details/history row counts match. 
- **SS — ✓:** saved HTML and parsed CSVs are available for every year of the term. Available years are listed below the table.
- **Commem — ✓:** state rules are ported into `commem/states.py` and verified on current-term inputs. Older commemorative CSVs do not establish that the current command works.
- **Estimator implemented — ✓:** a current state estimation module exists (25 R modules and Colorado’s Python estimator). This does not mean it has been validated for the current term or accepts current scraper outputs; see [scraper compatibility](ESTIMATE.md#scraper-to-estimator-compatibility). Legacy scripts do not count.
- **Roster — ✓:** nonempty LegiScan `people.csv` files with the expected ID/name/chamber fields exist for sessions starting within the term. This does not certify that every session or legislator is covered.

**Totals:** 40 scrapers done; 0 complete SS sets (5 partial); 0 commemorative
implementations verified for the current term (2 ported); roster files present
for 46 states; 26 current estimators implemented.

Scrape strategies below describe the current code. Scrape success and input
availability refer to the current term; estimator implementation is tracked
separately from term validation.

<table>
<thead>
<tr><th>State</th><th>Term</th><th>Scraper worked</th><th>SS saved + parsed</th><th>Commem ported + works</th><th>Roster files</th><th>Estimator implemented</th></tr>
</thead>
<tbody>
<tr><td>AK</td><td>2025–2026</td><td>✓</td><td>◐</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Scrape the All Measures Introduced list and each bill page at <a href="https://www.akleg.gov/basis/Home/BillsandLaws">https://www.akleg.gov/basis/Home/BillsandLaws</a>.</td></tr>
<tr><td>AL</td><td>2023–2026</td><td>◐</td><td>◐</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Fetch bills and paginated histories from the public API at <a href="https://alison.legislature.state.al.us/graphql">https://alison.legislature.state.al.us/graphql</a>.</td></tr>
<tr><td>AR</td><td>2025–2026</td><td>✓</td><td>◐</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Scrape paginated bill lists and each bill page at <a href="https://www.arkleg.state.ar.us/Bills/ViewBills">https://www.arkleg.state.ar.us/Bills/ViewBills</a>.</td></tr>
<tr><td>AZ</td><td>2025–2026</td><td>✓</td><td>◐</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Use a headless browser to read bill lists at <a href="https://www.azleg.gov/bills/">https://www.azleg.gov/bills/</a>; fetch details and actions from <a href="https://apps.azleg.gov/api/Bill/">https://apps.azleg.gov/api/Bill/</a>.</td></tr>
<tr><td>CA</td><td>2025–2026</td><td>✗</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">No current scraper implemented; the bulk-download workflow is deferred.</td></tr>
<tr><td>CO</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Scrape paginated bill lists and each bill page at <a href="https://leg.colorado.gov/bills/bill-search">https://leg.colorado.gov/bills/bill-search</a>; use headless Chrome when ordinary requests are rejected.</td></tr>
<tr><td>CT</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Scrape bill lists and status pages at <a href="https://cga.ct.gov/asp/CGABillInfo/CGABillInfoDisplay.asp">https://cga.ct.gov/asp/CGABillInfo/CGABillInfoDisplay.asp</a>; download introduced-bill PDFs for bill type and introducers.</td></tr>
<tr><td>DE</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Use headless Chrome to fetch paginated search results, bill pages, actions, and votes at <a href="https://legis.delaware.gov/AllLegislation">https://legis.delaware.gov/AllLegislation</a>.</td></tr>
<tr><td>FL</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Scrape paginated bill lists and each bill page at <a href="https://www.flsenate.gov/Session/Bills">https://www.flsenate.gov/Session/Bills</a>.</td></tr>
<tr><td>GA</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Use headless Chrome at <a href="https://www.legis.ga.gov/search">https://www.legis.ga.gov/search</a> to initialize access, then fetch paginated bill lists and details through the site’s API.</td></tr>
<tr><td>HI</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Read annual bill directories at <a href="https://data.capitol.hawaii.gov">https://data.capitol.hawaii.gov</a> and scrape linked status pages, plus special-session lists.</td></tr>
<tr><td>IA</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Scrape the Bill Book list, bill pages, and related-bill pages at <a href="https://www.legis.iowa.gov/legislation/BillBook">https://www.legis.iowa.gov/legislation/BillBook</a>.</td></tr>
<tr><td>ID</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Scrape session bill lists and each bill page at <a href="https://legislature.idaho.gov/sessioninfo/">https://legislature.idaho.gov/sessioninfo/</a>.</td></tr>
<tr><td>IL</td><td>2025–2026</td><td>✓</td><td>◐</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Download individual bill-status XML files from the assembly directories at <a href="https://ftp.ilga.gov/Legislation/">https://ftp.ilga.gov/Legislation/</a>.</td></tr>
<tr><td>IN</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Fetch session bill lists, details, and actions from the public API at <a href="https://iga.in.gov/api/">https://iga.in.gov/api/</a>.</td></tr>
<tr><td>KS</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Scrape paginated measure lists, bill pages, and paginated histories at <a href="https://www.kslegislature.gov/li/">https://www.kslegislature.gov/li/</a>.</td></tr>
<tr><td>KY</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Follow session links at <a href="https://legislature.ky.gov/Legislation/Pages/default.aspx">https://legislature.ky.gov/Legislation/Pages/default.aspx</a>, then scrape bill lists and pages at <a href="https://apps.legislature.ky.gov/record/">https://apps.legislature.ky.gov/record/</a>.</td></tr>
<tr><td>LA</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✗</td><td>✗</td></tr>
<tr><td colspan="7">Submit bill-range searches, follow paginated results, and scrape bill and author pages at <a href="https://www.legis.la.gov/Legis/BillSearch.aspx">https://www.legis.la.gov/Legis/BillSearch.aspx</a>.</td></tr>
<tr><td>MA</td><td>2025–2026</td><td>✗</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Scrape paginated search results, bill histories, and sponsor tabs at <a href="https://malegislature.gov/Bills/Search">https://malegislature.gov/Bills/Search</a>.</td></tr>
<tr><td>MD</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Download the session’s bulk legislation JSON and scrape bill pages for histories at <a href="https://mgaleg.maryland.gov/mgawebsite/search/legislation">https://mgaleg.maryland.gov/mgawebsite/search/legislation</a>.</td></tr>
<tr><td>ME</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Submit a session search, follow paginated results, and scrape summary, sponsor, and docket pages at <a href="https://legislature.maine.gov/LawMakerWeb/advancedsearch.asp">https://legislature.maine.gov/LawMakerWeb/advancedsearch.asp</a>.</td></tr>
<tr><td>MI</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Search by session and bill type, then scrape each bill page at <a href="https://www.legislature.mi.gov/Bills">https://www.legislature.mi.gov/Bills</a>.</td></tr>
<tr><td>MN</td><td>2025–2026</td><td>✗</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Scrape paginated searches, bill-status pages, and introduced bill text at <a href="https://www.revisor.mn.gov/bills/status_search.php?search=advanced">https://www.revisor.mn.gov/bills/status_search.php?search=advanced</a>.</td></tr>
<tr><td>MO</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Download House XML files or archived ZIPs from <a href="https://documents.house.mo.gov">https://documents.house.mo.gov</a>; scrape Senate bill lists, pages, and histories at <a href="https://www.senate.mo.gov/BillTracking/LegislativeInformation">https://www.senate.mo.gov/BillTracking/LegislativeInformation</a>.</td></tr>
<tr><td>MS</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✗</td><td>✗</td></tr>
<tr><td colspan="7">Follow session XML indexes at <a href="https://billstatus.ls.state.ms.us/sessions.htm">https://billstatus.ls.state.ms.us/sessions.htm</a>; download bill XML and check introduced text when primary authors are missing.</td></tr>
<tr><td>MT</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Fetch paginated bill records, actions, and sponsor-name lookups from the public API at <a href="https://bearbeta.legmt.gov">https://bearbeta.legmt.gov</a>.</td></tr>
<tr><td>NC</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Scrape session bill lists and each bill page at <a href="https://www.ncleg.gov">https://www.ncleg.gov</a>.</td></tr>
<tr><td>ND</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Follow assembly bill indexes at <a href="https://ndlegis.gov/assembly">https://ndlegis.gov/assembly</a>, then scrape bill-overview and action pages.</td></tr>
<tr><td>NE</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Read annual bill lists and their CSV exports at <a href="https://nebraskalegislature.gov/bills/search_by_date.php">https://nebraskalegislature.gov/bills/search_by_date.php</a>, then scrape each bill page.</td></tr>
<tr><td>NH</td><td>2025–2026</td><td>✗</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Submit annual searches and scrape status, docket, and bill-text pages at <a href="https://gc.nh.gov/bill_status/legacy/bs2016/">https://gc.nh.gov/bill_status/legacy/bs2016/</a>.</td></tr>
<tr><td>NJ</td><td>2026–2027</td><td>✗</td><td>✗</td><td>◐</td><td>✗</td><td>✓</td></tr>
<tr><td colspan="7">Manually download the term’s bill-tracking ZIP from <a href="https://www.njleg.state.nj.us/legislative-downloads?downloadType=Bill_Tracking">https://www.njleg.state.nj.us/legislative-downloads?downloadType=Bill_Tracking</a>; the command parses MAINBILL.TXT and BILLHIST.TXT.</td></tr>
<tr><td>NM</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Submit session searches, follow paginated results, and scrape each bill page at <a href="https://www.nmlegis.gov/Legislation/Legislation_List">https://www.nmlegis.gov/Legislation/Legislation_List</a>.</td></tr>
<tr><td>NV</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Scrape paginated session bill lists and bill-overview tabs at <a href="https://www.leg.state.nv.us/App/NELIS/REL">https://www.leg.state.nv.us/App/NELIS/REL</a>.</td></tr>
<tr><td>NY</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Search both chambers for the term, then scrape each bill’s summary and actions at <a href="https://nyassembly.gov/leg/?sh=advanced">https://nyassembly.gov/leg/?sh=advanced</a>.</td></tr>
<tr><td>OH</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Scrape paginated searches, bill summaries, and status tables at <a href="https://www.legislature.ohio.gov/legislation/search">https://www.legislature.ohio.gov/legislation/search</a>.</td></tr>
<tr><td>OK</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Submit session and chamber searches at <a href="https://www.oklegislature.gov/TextOfMeasures.aspx">https://www.oklegislature.gov/TextOfMeasures.aspx</a>, then scrape each bill’s history page.</td></tr>
<tr><td>OR</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Fetch paginated bill records with sponsors, documents, and histories from <a href="https://api.oregonlegislature.gov/odata/odataservice.svc/">https://api.oregonlegislature.gov/odata/odataservice.svc/</a>.</td></tr>
<tr><td>PA</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Scrape session and chamber bill indexes, then each bill page at <a href="https://www.palegis.us/legislation/bills">https://www.palegis.us/legislation/bills</a>.</td></tr>
<tr><td>RI</td><td>2025–2026</td><td>✗</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Scrape annual bill-text indexes and linked bill text at <a href="https://webserver.rilegislature.gov">https://webserver.rilegislature.gov</a>; fetch history reports at <a href="https://status.rilegislature.gov/bill_history_report.aspx">https://status.rilegislature.gov/bill_history_report.aspx</a>.</td></tr>
<tr><td>SC</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Search by primary sponsor, including committees, and parse bill details and actions from results at <a href="https://www.scstatehouse.gov/sponsorsearch.php">https://www.scstatehouse.gov/sponsorsearch.php</a>.</td></tr>
<tr><td>SD</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Fetch session bill lists, details, and action logs from <a href="https://sdlegislature.gov/api/">https://sdlegislature.gov/api/</a>.</td></tr>
<tr><td>TN</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Scrape bill-range and special-session indexes, then each bill page at <a href="https://wapp.capitol.tn.gov/apps/Indexes/BillsByIndex">https://wapp.capitol.tn.gov/apps/Indexes/BillsByIndex</a>.</td></tr>
<tr><td>TX</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">List bills through <a href="ftp://ftp.legis.state.tx.us/bills/">ftp://ftp.legis.state.tx.us/bills/</a>, then scrape <a href="https://capitol.texas.gov/BillLookup/History.aspx">https://capitol.texas.gov/BillLookup/History.aspx</a>; download XML when a history page is unavailable.</td></tr>
<tr><td>UT</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Follow session bill indexes at <a href="https://le.utah.gov/bills/bills_By_Session.jsp">https://le.utah.gov/bills/bills_By_Session.jsp</a>, then download each bill’s details and actions as JSON.</td></tr>
<tr><td>VA</td><td>2026–2027</td><td>✗</td><td>✗</td><td>◐</td><td>✗</td><td>✓</td></tr>
<tr><td colspan="7">Use a headless browser at <a href="https://lis.virginia.gov/bill-search">https://lis.virginia.gov/bill-search</a> to capture the bill list, then visit each bill page to collect its data.</td></tr>
<tr><td>VT</td><td>2025–2026</td><td>✗</td><td>✗</td><td>✗</td><td>✓</td><td>✗</td></tr>
<tr><td colspan="7">Fetch bill lists and action feeds as JSON, and scrape bill pages for details at <a href="https://legislature.vermont.gov/">https://legislature.vermont.gov/</a>.</td></tr>
<tr><td>WA</td><td>2025–2026</td><td>✗</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Fetch bill and sponsor XML from <a href="https://wslwebservices.leg.wa.gov/legislationservice.asmx">https://wslwebservices.leg.wa.gov/legislationservice.asmx</a>; scrape histories at <a href="https://app.leg.wa.gov/billsummary">https://app.leg.wa.gov/billsummary</a>.</td></tr>
<tr><td>WI</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Follow session, chamber, and bill-type folders under <a href="https://docs.legis.wisconsin.gov/2025/proposals">https://docs.legis.wisconsin.gov/2025/proposals</a>, then scrape each bill page.</td></tr>
<tr><td>WV</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Scrape annual bill and resolution lists, then each bill’s action table at <a href="https://www.wvlegislature.gov/Bill_Status/bill_status.cfm">https://www.wvlegislature.gov/Bill_Status/bill_status.cfm</a>.</td></tr>
<tr><td>WY</td><td>2025–2026</td><td>✓</td><td>✗</td><td>✗</td><td>✓</td><td>✓</td></tr>
<tr><td colspan="7">Fetch annual bill lists and each bill’s details and actions from <a href="https://web.wyoleg.gov/LsoService/api/BillInformation">https://web.wyoleg.gov/LsoService/api/BillInformation</a>.</td></tr>
</tbody>
</table>

## Remaining work

- **AL:** the successful scrape covers 2025–26 only. The updated scraper accepts 2023–26, but a complete four-year output pair is not present.
- **CA:** bulk-download workflow was deferred; no current-term bill output pair is present.
- **MA:** last run stopped on a people-tab mismatch.
- **MN:** last run stopped on a bill-text identity mismatch.
- **NH:** last run stopped on inconsistent docket metadata.
- **RI:** last run stopped on an unexpected date language/format.
- **VT:** last run stopped on a detail/index title mismatch.
- **WA:** last run stopped on a missing primary sponsor.
- **NJ:** bulk-file cleaner exists, but no successful 2026–27 run is recorded.
- **VA:** the browser scraper is prepared, but 2026–27 session IDs still need verification.

**SS:** AK, AR, and AZ have saved HTML and matching parsed CSVs for 2025.
AL also has matching 2025 HTML/CSV plus imported 2023 and 2024 CSVs; those earlier
years do not have saved HTML here. IL has an imported 2025 CSV without saved
HTML. No 2026 or 2027 SS CSVs are present. Existing CSVs can still be used as
inputs even where the original HTML is unavailable.

**Commemorative coding:** only NJ and VA are configured to work in the current module.

**Rosters:** LA and MS have only 2023/2024 session files here; NJ and VA have no
2026–27 roster files. Present rosters elsewhere still need checks for missing
special sessions, replacements, and incorrect chamber labels.

## Scrape run manifests

A run manifest is a JSON summary written by a scraper after a successful run.
It lives beside the bill CSVs at
`.data/<STATE>/bill/.<STATE>_scrape_<TERM>.json`, for example
`.data/CT/bill/.CT_scrape_2025_2026.json`. The leading dot makes it a hidden file
in many file browsers.

Depending on the state, it records the term, sessions processed, completion time,
bill and action counts, and source exceptions encountered. Those exceptions can
include bills without sponsors or histories, empty source records, excluded
instruments, or use of an alternative source such as XML. They explain what the
scraper found and how it handled it; they are not necessarily scraper errors.

Review these exceptions alongside the CSVs before treating a scrape as ready for
estimation.

All 49 current scrapers now use `scrape/reporting.py` to write these summaries.
New manifests share run IDs, start/completion times, bill CSV filenames and sizes,
and an `exceptions` list; each state's original fields are also retained.
Existing manifests keep their old format until that state is scraped again.

The latest attempt is recorded separately at
`.data/<STATE>/bill/.<STATE>_scrape_<TERM>.attempt.json`. Its status is `running`,
`succeeded`, `skipped`, `failed`, or `interrupted`; failures include the exception
type and message. A failed attempt may leave an older successful manifest, so
check both files. A process killed abruptly can leave the attempt marked `running`.
Reporting also depends on the output directory remaining writable.

The shared code records outcomes; state parsers still identify source-specific
exceptions. Failed attempts do not collect every finding encountered before the
failure, and an empty exceptions list does not establish estimation readiness.
This reporting covers current full scrapes, including NJ bulk-file parsing;
legacy scrapers and separate retry/preview commands are outside its scope.
