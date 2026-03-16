const REQUIRED_CANONICAL_FIELDS = [
  'article_id',
  'source_id',
  'title',
  'url',
  'author',
  'publication_time_utc',
  'publication_time_local',
  'language',
  'source_class',
  'raw_snippet',
  'canonical_text',
  'extraction_quality_score',
  'article_type',
  'paywall_flag',
  'ingestion_method',
  'fetched_at'
];

export function createCanonicalArticleRecord(data) {
  const record = {
    article_id: data.article_id,
    source_id: data.source_id,
    title: data.title ?? null,
    url: data.url ?? null,
    author: data.author ?? null,
    publication_time_utc: data.publication_time_utc ?? null,
    publication_time_local: data.publication_time_local ?? null,
    language: data.language ?? null,
    source_class: data.source_class ?? null,
    raw_snippet: data.raw_snippet ?? null,
    canonical_text: data.canonical_text ?? null,
    extraction_quality_score: Number(data.extraction_quality_score ?? 0),
    article_type: data.article_type ?? null,
    paywall_flag: Boolean(data.paywall_flag),
    ingestion_method: data.ingestion_method ?? null,
    fetched_at: data.fetched_at ?? null,
    ...data
  };

  for (const field of REQUIRED_CANONICAL_FIELDS) {
    if (!(field in record)) {
      throw new Error(`Canonical article record is missing required field "${field}"`);
    }
  }

  return Object.freeze(record);
}
