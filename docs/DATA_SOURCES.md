# Data Sources

Every external source WikiPulse uses, how we call it, and the rules we follow.
All sources are free and need no account or credit card.

Last checked: **2026-11-06** (from a cloud dev container; see notes on 429s below).

## Rules we follow for every Wikimedia endpoint

Source: [Wikimedia APIs/Rate limits](https://www.mediawiki.org/wiki/Wikimedia_APIs/Rate_limits)
(new in 2026, marked "subject to experimentation and change"),
[User-Agent policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy),
[API:Etiquette](https://www.mediawiki.org/wiki/API:Etiquette).

| Rule | How WikiPulse does it |
|---|---|
| Send a meaningful User-Agent with contact info (email or full URL) | `wikipulse.config.Settings.user_agent` -> `WikiPulse/<version> (https://github.com/jinam-shah/wikipulse; <WIKIPULSE_CONTACT>)`. Contact defaults to the repo's issues page, so no personal email is committed. |
| Rate limit for unauthenticated clients with a compliant User-Agent: **200 requests/minute** (enforced per minute). Clients with no User-Agent get only 10/minute. | A shared client-side limiter (added in Phase 5/8) keeps us well below 200/min. |
| No more than **3 concurrent requests** | Requests to Wikimedia go through one small worker pool of size <= 3. |
| Respect `Retry-After` on HTTP 429 | The HTTP client sleeps for `Retry-After` seconds, then retries with backoff. |
| Cache responses | Page summaries, diffs and pageviews are cached in Postgres with a TTL. |
| Attribute Wikipedia (CC BY-SA 4.0) | Every page that shows article text, summaries or diffs links to the source article/diff and shows the licence. |

> Note: during setup, the Pageviews API returned `429` with `x-envoy-ratelimited: true`
> from the shared cloud IP even with a compliant User-Agent. This is IP-level throttling
> on shared infrastructure, not a sign the endpoint is gone. From a home connection it
> should behave normally. Re-test from your machine before Phase 5.

## 1. EventStreams - `recentchange` (main stream)

- **URL:** `https://stream.wikimedia.org/v2/stream/recentchange`
- **Protocol:** Server-Sent Events (SSE). Each message has an `id:` line (a JSON list of
  topic/partition/offset-or-timestamp) that can be sent back as `Last-Event-ID` to resume.
- **Contents:** every change on every wiki: `wiki`, `title`, `user`, `bot`, `type`
  (`edit`, `new`, `log`, `categorize`), `length.old/new`, `revision.old/new`, `comment`,
  `timestamp`, `meta.id`, ...
- **Use:** the main input to the producer (Phase 1). We filter to the configured wikis
  (default `enwiki`) and to `type in (edit, new)`.
- **Verified:** connected and received events on 2026-11-06.
- **Docs:** https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service

## 2. EventStreams - other useful streams

The live stream list is at `https://stream.wikimedia.org/?spec`. On 2026-11-06 it included:

| Stream | Why it matters for WikiPulse |
|---|---|
| `mediawiki.revision-tags-change` | Tags added after the fact, e.g. `mw-reverted`. Labels for the vandalism model (Phase 6). |
| `mediawiki.page-create` | New-page tracking (dashboard "new pages per minute"). |
| `mediawiki.revision-create` | Full revision metadata including tags at creation time (`mw-undo`, `mw-rollback`, `mw-manual-revert`). |
| `mediawiki.page_revert_risk_prediction_change.v1` | **Wikimedia's own revert-risk predictions, published as a stream.** Lets us compare against Lift Wing for every edit without calling the Lift Wing API per edit. To be confirmed in Phase 6 (schema, which wikis, latency). |

## 3. MediaWiki Action API - compare and revisions

- **URL:** `https://en.wikipedia.org/w/api.php`
- **Calls:** `action=compare&fromrev=<id>&torelative=prev` (diff HTML),
  `action=query&prop=revisions` (revision metadata, page protection via `prop=info&inprop=protection`)
- **Use:** What Changed summaries, edit war analysis, diff-based model features (Phases 6, 8).
- **Verified:** `action=compare` returned HTTP 200 on 2026-11-06.
- **Etiquette:** serial requests, `maxlag` parameter for non-interactive jobs, User-Agent.

## 4. Wikimedia REST API - page summary

- **URL:** `https://en.wikipedia.org/api/rest_v1/page/summary/{title}`
- **Use:** short description and lead summary as context for AI explanations and clusters (Phase 9).
- **Verified:** HTTP 200 on 2026-11-06.

## 5. Pageviews API

- **URL:** `https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{project}/{access}/{agent}/{article}/{granularity}/{start}/{end}`
  (`granularity` = `hourly` or `daily`)
- **Use:** confirm a trend reflects real public interest (Phase 5).
- **Caveat:** hourly data appears with a delay (typically an hour or more), so confirmation
  is necessarily later than detection. We'll measure the actual delay in Phase 5.
- **Status 2026-11-06:** throttled (429) from the shared cloud IP; re-test locally.

## 6. Lift Wing - revert-risk model

- **URL:** `https://api.wikimedia.org/service/lw/inference/v1/models/revertrisk-language-agnostic:predict`
- **Request:** `POST {"rev_id": <int>, "lang": "en"}`
- **Use:** baseline to compare WikiPulse's vandalism model against (Phase 6).
- **Verified:** endpoint responded on 2026-11-06 (it returned a model error for a
  revision without a parent, which confirms the service is up and reachable anonymously).
- **Docs:** https://wikitech.wikimedia.org/wiki/Machine_Learning/LiftWing/Usage
- Falls under the same 200 req/min API limit. For bulk comparison, prefer the
  `page_revert_risk_prediction_change` stream (section 2) if it covers our edits.

## Licensing and attribution

- Wikipedia text: [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
  Anything derived from article text (summaries, diffs, AI explanations) shows a link to
  the article/diff and "Content from Wikipedia, licensed CC BY-SA 4.0".
- Edit metadata (usernames, comments) is public. We show usernames exactly as Wikipedia does
  and never label a person; see "Responsible use" in the README.
