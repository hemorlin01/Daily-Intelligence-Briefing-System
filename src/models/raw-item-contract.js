export const RAW_ITEM_FIELD_ALIASES = Object.freeze({
  title: Object.freeze(['title', 'headline', 'name']),
  url: Object.freeze(['url', 'link', 'permalink']),
  canonicalText: Object.freeze(['canonical_text', 'content', 'body', 'article_text', 'text']),
  rawSnippet: Object.freeze(['raw_snippet', 'snippet', 'description']),
  sourceProvidedSummary: Object.freeze(['source_summary', 'summary', 'dek']),
  author: Object.freeze(['author', 'byline', 'creator']),
  articleType: Object.freeze(['article_type', 'item_type', 'content_type']),
  publicationTimeUtc: Object.freeze(['publication_time_utc', 'published_at', 'pubDate', 'date', 'isoDate']),
  publicationTimeLocal: Object.freeze(['publication_time_local', 'published_local']),
  ingestionMethod: Object.freeze(['ingestion_method']),
  paywallFlag: Object.freeze(['paywall_flag']),
  originalPublicationUrl: Object.freeze(['original_publication_url']),
  isOriginalReporting: Object.freeze(['is_original_reporting']),
  isSyndicatedCopy: Object.freeze(['is_syndicated_copy'])
});

export const RAW_ITEM_INPUT_CONTRACT = Object.freeze({
  boundary_name: 'phase1-normalization-input',
  required_fields: Object.freeze([
    {
      canonical_field: 'title',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.title,
      requirement: 'required'
    },
    {
      canonical_field: 'url',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.url,
      requirement: 'required'
    },
    {
      canonical_field: 'publication_time',
      accepted_aliases: [
        ...RAW_ITEM_FIELD_ALIASES.publicationTimeUtc,
        ...RAW_ITEM_FIELD_ALIASES.publicationTimeLocal
      ],
      requirement: 'required_for_main_or_backup_pool'
    }
  ]),
  optional_fields: Object.freeze([
    {
      canonical_field: 'canonical_text',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.canonicalText,
      handling: 'preferred full-text body; missing body is allowed but weakens extraction quality and usually prevents main-pool entry'
    },
    {
      canonical_field: 'raw_snippet',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.rawSnippet,
      handling: 'fallback content signal; substantial snippet-only items can enter backup_pool'
    },
    {
      canonical_field: 'source_provided_summary',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.sourceProvidedSummary,
      handling: 'fallback content signal; treated similarly to snippet when body text is absent'
    },
    {
      canonical_field: 'author',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.author,
      handling: 'optional provenance signal used in extraction quality and duplicate tie-breaking'
    },
    {
      canonical_field: 'article_type',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.articleType,
      handling: 'optional; defaults to the source-config expected_article_type'
    },
    {
      canonical_field: 'ingestion_method',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.ingestionMethod,
      handling: 'optional override; defaults to source-config fetch_method'
    },
    {
      canonical_field: 'paywall_flag',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.paywallFlag,
      handling: 'optional override; defaults from source-config paywall_policy'
    },
    {
      canonical_field: 'original_publication_url',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.originalPublicationUrl,
      handling: 'optional provenance signal for syndicated duplicate resolution'
    },
    {
      canonical_field: 'is_original_reporting',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.isOriginalReporting,
      handling: 'optional provenance signal for duplicate tie-breaking'
    },
    {
      canonical_field: 'is_syndicated_copy',
      accepted_aliases: RAW_ITEM_FIELD_ALIASES.isSyndicatedCopy,
      handling: 'optional provenance signal for duplicate tie-breaking'
    }
  ]),
  content_signal_rule: 'At least one of canonical_text, a substantial raw_snippet, or a substantial source_provided_summary must be present for eligibility.',
  missing_body_handling: 'Items with no canonical_text are normalized honestly with missing_canonical_text warnings. They are not upgraded to complete articles.',
  snippet_only_handling: 'Snippet-only or source-summary-only items may be admitted to backup_pool when they remain article-like, fresh, and otherwise valid.',
  malformed_handling: 'Missing title, invalid URL, or missing publication time remain auditable rejection reasons rather than silent defaults.'
});
