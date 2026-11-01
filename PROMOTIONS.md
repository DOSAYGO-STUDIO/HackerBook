# Product promotion operations

Owner authorization: 2026-11-01. Promote TDF, Grip, and BrowserBox; test first,
repair the failed deployment, dispatch one full archive rebuild, and review and
adjust promotion performance autonomously. Do not dispatch additional full
rebuilds as part of promotion optimization.

## Pre-change checklist

Read HackerBook AGENTS.md and the requesting workspace's AI Lint index,
philosophy, architecture and JavaScript doctrine/rejects, and checklist.
Keep runtime promotion policy in a separate module, validate remote config,
bound telemetry inputs, and preserve static archive availability on promotion
service failure. Do not store IP addresses or user identifiers in analytics.

Override: AR-R4 (legacy files over 1000 lines)
Reason: the existing desktop and mobile HTML applications are large single files.
Only add a marked promotion mount plus external CSS and script references.
Risk: changing duplicated HTML could drift or disturb the archive UI.
Mitigation: keep promotion logic in small separate modules and test every HTML
entry point. Do not refactor the unrelated SQLite archive application.

## Design

TDF and Grip have real links in the static HTML, including offline copies.
BrowserBox is inserted only at the edge for eligible requests. Country comes
from Cloudflare's trusted request metadata, never a client-supplied header.
Unknown locations, mainland China, Hong Kong and Macau are excluded; recognizable China-focused
crawlers are excluded regardless of location. This is promotion targeting,
not an enforcement mechanism for sales eligibility or search-result geography.

Only HTML entry points and the telemetry endpoint invoke the Pages Worker.
Archive shards, WASM and other large assets remain static and cacheable.
Remote promotion configuration lives in a dedicated KV namespace, allowing
copy/emphasis changes without rebuilding or reuploading the archive data.
Missing/invalid configuration produces the static TDF/Grip links, logs the
failure, and omits BrowserBox. Personalized HTML is never shared-cacheable.

Telemetry counts browser interaction-qualified exposures and trusted clicks,
filtered again at the edge for known automation. It is a qualified-traffic
proxy, not verified humans or purchases. Product links carry attribution tags.
Weekly review compares copy variants within each product, retains exploration,
and refuses to infer success from crawler volume or insufficient observations.
All automatic changes use a fixed, reviewed copy catalog and preserve geography.

## Autonomous review and limits

`Review product promotions` runs Mondays at 13:17 UTC. It checks live promotion
delivery and representative archive shards, queries the preceding 28 days of
production-host Analytics Engine events (never before the last config change),
and records a JSON report and readable Actions summary. Preview traffic is
excluded. Sampled data, suspicious click rates, and invalid data cannot select a
winner. Authentication/API failures fail the job rather than becoming zeroes.

After at least 14 days and 200 qualified exposures per active variant, a variant
with at least 20 clicks and non-overlapping 95% Wilson intervals gets 80% of its
product's copy presentations. Other variants retain 10% each. After 28 days of
adequately exposed copy with zero clicks, the unused approved alternative gets
80%. Six weeks of low volume broadens an initial two-copy experiment to all
three copies equally; this is exploration, not a claim of effectiveness.

These are operational heuristics for inbound traffic, not a causal revenue
experiment. There are no unique-user counts, purchase signals, or guaranteed
bot-proof events. The client requires trusted browser input and visibility;
the endpoint checks origin, user agent, geography and a bounded payload. Direct
HTTP clients can still imitate browsers. Review conservatively and use the UTM
campaign `hackerbook_tools_v1` to inspect downstream product conversions.

The reviewer only edits approved copy weights. It cannot edit destination URLs,
the BrowserBox market exclusion, application code, or dispatch archive builds.
Changes preserve the previous configuration under `history-<revision>` in KV,
and review artifacts are retained for 90 days. Reports state when no conclusion
is justified. The scheduled workflow is the persistent follow-up mechanism;
it does not depend on this chat or a local machine remaining open.

## Change and rollback

Configuration is the `campaign-v1` key in the `PROMO_CONFIG` namespace declared
in `wrangler.json`. It is validated at both write/review and render boundaries.
KV reads cache for five minutes. Set `enabled: false` to remove the promotion;
restore a saved history value to roll back weights. Keep a monotonically
increasing revision when manually restoring a prior configuration. Disable the
review workflow before a manual experiment that should not be adjusted.

Copy selection and weights change without a Pages deployment or archive rebake.
New copy text, HTML, CSS, or policy code requires a Pages deployment. Such a
deployment needs a complete static asset directory, but the existing immutable
shards may be reused; regenerating the database is not inherently necessary.
Never deploy this checkout's incomplete shard directory to production.

The full rebuild workflow is dispatch-only because the owner requested exactly
one new full rebake. Its previous weekly schedule was already manually disabled.
The separate promotion review runs weekly without regenerating archive data.

## Validation

Use Node 22 and `npm ci`, then `npm test > /tmp/hackerbook-tests.log 2>&1`.
Read the complete log and retain the command's exit status. Tests cover actual
Workers HTML rewriting, country/crawler exclusions, cache separation, static
fallback, browser layout with/without JavaScript, telemetry delivery, and review
decisions. `npm run deploy-preflight` requires the account-scoped deployment
token and account ID. The full rebuild runs this before any expensive work.

The Cloudflare token needs Pages Edit, Workers KV Storage Edit, and Account
Analytics Read for the selected account. No DNS, billing, user-management or
other-account permissions are required. GitHub stores it in the existing
`CLOUDFLARE_API_TOKEN` secret.

## Deployment investigation

Latest failed full build: GitHub Actions run 38203811286, 2026-10-12.
Archive generation and validation finished. `wrangler whoami` failed, its
diagnostics were discarded, and the script attempted interactive login while
an API token was set. No deployment occurred. The workflow was manually disabled.
Add an account/project-scoped API preflight before expensive generation, retain
actionable error responses, and never attempt interactive auth in unattended CI.

