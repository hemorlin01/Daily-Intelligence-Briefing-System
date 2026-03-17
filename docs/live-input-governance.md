# Live Input Governance

The live-input layer is governed by two configs:

- `config/sources.json`
  - authoritative governed source universe
  - editorial/source metadata
- `config/feed-overrides.json`
  - authoritative ingestion metadata for every governed source
  - support status, support level, ingestion method, adapter type, notes, and zero-or-more feed definitions
- `config/live-input-rules.json`
  - fetch timeout / retry behavior
  - response-size guardrails
  - success-ladder sources used in workflow/runtime reporting

## Status Model

The live-input layer now uses two orthogonal status concepts:

- `support_status`
  - config/policy truth
  - describes whether the repo has a governed ingestion path configured, restricted, unsupported, or still under review
- `validation_status`
  - runtime truth
  - describes what the latest run actually proved about that source or feed

### support_status

- `supported`
  - public feed path is configured and treated as normal fetch coverage
- `partial`
  - a concrete governed ingestion path exists, but coverage is limited, summary-only, newsletter-shaped, or still only partially validated
- `restricted`
  - source is governed, but automated ingestion depends on subscription, licensed, or otherwise non-repository-safe access
- `unsupported`
  - no repository-safe public ingestion path is configured
- `pending_review`
  - source remains governed and visible, but a concrete public ingestion path still needs validation
- `unstable`
  - feed path exists, but operational stability is not yet good enough for normal supported coverage
- `inactive`
  - source is intentionally inactive in the live-input layer

### validation_status

- `not_configured`
  - no active feed definitions exist for this source or feed
- `not_attempted`
  - a feed is configured, but the current run did not attempt it
- `fetch_failed`
  - the run attempted the feed, but HTTP/network fetch did not succeed
- `parse_failed`
  - fetch succeeded, but parsing failed
- `fetched_zero_entries`
  - fetch and parse succeeded, but zero feed entries were observed
- `normalized_zero_items`
  - entries were observed, but no usable raw items survived normalization
- `validated_raw_items`
  - the run produced one or more usable raw items from that feed or source

## Adapter Types

- `xml_feed`
  - standard RSS / Atom parsing
- `html_listing`
  - governed source-specific listing extraction with explicit regex patterns
- `none`
  - no automated live-input adapter is currently configured

## Feed Definition Shape

Each `feed_definitions` item may include:

- `feed_id`
- `label`
- `url`
- `format`
  - `rss`, `atom`, or `html`
- `adapter_type`
- `content_mode`
  - `full_text`, `summary_only`, or `mixed`
- `expected_entry_type`
- `active_status`
- `patterns`
  - only for `html_listing`

## Diagnostics

The live-input builder writes:

- `artifacts/inputs/governed_source_inventory_report.json`
- `artifacts/inputs/feed_inventory_report.json`
- `artifacts/inputs/feed_fetch_report.json`
- `artifacts/inputs/live_input_generation_report.json`
- `artifacts/inputs/governed_source_validation_ledger.json`
- `artifacts/inputs/live_input_runtime_report.json`
- `artifacts/inputs/live_input_operational_summary.json`
- `artifacts/inputs/source_coverage_gap_report.json`
- `artifacts/inputs/raw_item_source_distribution.json`
- `artifacts/inputs/raw_item_language_distribution.json`
- `artifacts/inputs/raw_item_class_distribution.json`
- `artifacts/inputs/raw-items.json`

## Validation Ledger

`governed_source_validation_ledger.json` separates configured availability from proven runtime behavior.

Per feed and per source it records fields such as:

- configured status
- validation status
- last attempted timestamp
- last fetch success timestamp
- last parse success timestamp
- last raw-item success timestamp
- last error code / summary
- last HTTP status
- last observed item counts
- consecutive failures

This ledger is the main source of operational truth when deciding whether a feed is merely configured or actually working.

## Runtime Failure Categories

The fetch layer now classifies failures more precisely where possible:

- `dns_error`
- `timeout`
- `tls_error`
- `connection_error`
- `too_many_redirects`
- `http_401`
- `http_403`
- `http_404`
- `http_429`
- `http_5xx`
- `invalid_content_type`
- `non_xml_response`
- `empty_response`
- `parser_failure`
- `normalization_zero_items`

These categories appear in feed-level diagnostics and in the workflow-facing summary output.

## Verified Working Evidence

A source is operationally proven only when a real run reaches `validation_status = validated_raw_items`.

The runtime layer now keeps these stages distinct:

- configured but unproven
- attempted but failed
- fetched successfully
- parsed successfully
- raw-item generation succeeded

`live_input_runtime_report.json` is the quickest machine-readable way to inspect that path.

## Workflow Summary Output

`npm run dibs:summarize-live-inputs` prints a compact summary suitable for GitHub Actions logs. It now shows:

- governed source total
- configured source total
- non-configured source total
- attempted source total
- configured sources by `support_status`
- validation counts by `validation_status`
- configured/attempted feed counts
- fetch/parse outcome counts
- failure categories
- attempted sources with zero entries
- attempted sources with entries but zero normalized items
- attempted sources with raw items
- raw items generated
- top raw-item source classes
- warning flags
- a core success-ladder table
- sample feed failures with classification

## Adding a Governed Source Properly

1. Add or update the source in `config/sources.json`.
2. Add or update the matching entry in `config/feed-overrides.json`.
3. Choose a support status honestly.
4. Only add `feed_definitions` when a real governed ingestion path exists.
5. Prefer `xml_feed` before introducing `html_listing`.
6. Add or update tests that cover the new ingestion path or status decision.

## Truth-Passing a New Endpoint

Before marking a source strongly supported:

1. Confirm the URL is a direct machine-readable feed or governed structured endpoint, not just a directory or landing page.
2. Confirm the source-level `ingestion_method` and `adapter_type` match the actual feed definitions.
3. Run live-input generation in a real outbound-network environment and inspect both `governed_source_validation_ledger.json` and `live_input_runtime_report.json`.
4. Confirm whether the runtime result is a true XML/Atom/RSS fetch or merely a redirected HTML landing page.
5. Only treat a source as operationally validated after at least one run reaches `validated_raw_items`.
