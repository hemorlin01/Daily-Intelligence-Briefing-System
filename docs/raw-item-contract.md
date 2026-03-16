# Raw Item Contract

This document defines the Phase 1 normalization boundary for incoming raw items before they are converted into canonical article records.

## Envelope

Each raw input passed into `buildCandidatePools()` must have this shape:

```json
{
  "source_id": "reuters",
  "item": {
    "title": "Original title text",
    "url": "https://example.com/story",
    "canonical_text": "Full extracted body text if available",
    "snippet": "Snippet or teaser text if available",
    "summary": "Source-provided summary if available",
    "published_at": "2026-03-16T08:00:00.000Z",
    "author": "Reporter Name",
    "article_type": "news",
    "ingestion_method": "rss",
    "paywall_flag": false,
    "original_publication_url": "https://original.example.com/story",
    "is_original_reporting": true,
    "is_syndicated_copy": false
  }
}
```

`source_id` is required at the envelope layer and must match a configured approved source.

## Required Raw Item Fields

Required for eligibility:

- `title` or `headline` or `name`
- `url` or `link` or `permalink`
- publication time via one of:
  - `publication_time_utc`
  - `published_at`
  - `pubDate`
  - `date`
  - `isoDate`
  - `publication_time_local`
  - `published_local`

Required content signal:

- At least one of:
  - `canonical_text` or `content` or `body` or `article_text` or `text`
  - a substantial `raw_snippet` or `snippet` or `description`
  - a substantial `source_summary` or `summary` or `dek`

## Optional Raw Item Fields

- `author` or `byline` or `creator`
- `article_type` or `item_type` or `content_type`
- `ingestion_method`
- `paywall_flag`
- `original_publication_url`
- `is_original_reporting`
- `is_syndicated_copy`

These optional fields improve provenance, duplicate resolution, and extraction-quality scoring, but they are not silently fabricated when missing.

## Handling Rules

Missing body text:

- The item is still normalized.
- `canonical_text` remains null.
- `missing_canonical_text` is recorded in normalization warnings.
- The item cannot look “complete” without body text.

Snippet-only items:

- If the snippet is substantial enough and the item is otherwise article-like, fresh, and valid, it may enter `backup_pool`.
- It does not enter `mainPool` by default just because a snippet exists.

Source-provided summary without body text:

- Summaries are treated as a fallback content signal.
- Summary-only items are normalized honestly and usually route to `backup_pool`, not `mainPool`.

Malformed items:

- Invalid URL, missing title, or missing publication time remain explicit rejection reasons.
- The pipeline does not invent placeholders to force malformed items through.

## Code Reference

The executable contract aliases are defined in [src/models/raw-item-contract.js](/D:/Codex/newsletter_v2/src/models/raw-item-contract.js).
