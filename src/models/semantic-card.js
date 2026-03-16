const REQUIRED_SEMANTIC_FIELDS = [
  'article_id',
  'source_id',
  'title',
  'url',
  'language',
  'event_type',
  'primary_entities',
  'secondary_entities',
  'geography_primary',
  'geography_secondary',
  'topic_labels',
  'strategic_dimensions',
  'candidate_keywords',
  'factual_summary',
  'why_it_matters',
  'novelty_signal',
  'user_relevance_signal',
  'confidence_score',
  'warnings'
];

function assertFieldPresence(card, articleId) {
  for (const field of REQUIRED_SEMANTIC_FIELDS) {
    if (!(field in card)) {
      throw new Error(`Semantic card validation failed for article ${articleId}: missing required field "${field}"`);
    }
  }
}

function assertStringArray(value, fieldName, articleId) {
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string')) {
    throw new Error(`Semantic card validation failed for article ${articleId}: "${fieldName}" must be an array of strings`);
  }
}

export function validateSemanticCard(card, taxonomy) {
  const articleId = card?.article_id ?? 'unknown';
  if (!card || typeof card !== 'object' || Array.isArray(card)) {
    throw new Error(`Semantic card validation failed for article ${articleId}: card must be an object`);
  }

  assertFieldPresence(card, articleId);

  if (typeof card.title !== 'string' || card.title.length === 0) {
    throw new Error(`Semantic card validation failed for article ${articleId}: "title" must be a non-empty string`);
  }

  if (typeof card.url !== 'string' || card.url.length === 0) {
    throw new Error(`Semantic card validation failed for article ${articleId}: "url" must be a non-empty string`);
  }

  if (!taxonomy.event_types.includes(card.event_type)) {
    throw new Error(`Semantic card validation failed for article ${articleId}: invalid event_type "${card.event_type}"`);
  }

  assertStringArray(card.primary_entities, 'primary_entities', articleId);
  assertStringArray(card.secondary_entities, 'secondary_entities', articleId);
  assertStringArray(card.geography_secondary, 'geography_secondary', articleId);
  assertStringArray(card.topic_labels, 'topic_labels', articleId);
  assertStringArray(card.strategic_dimensions, 'strategic_dimensions', articleId);
  assertStringArray(card.candidate_keywords, 'candidate_keywords', articleId);

  if (card.topic_labels.length === 0) {
    throw new Error(`Semantic card validation failed for article ${articleId}: "topic_labels" must not be empty`);
  }

  if (card.candidate_keywords.length === 0) {
    throw new Error(`Semantic card validation failed for article ${articleId}: "candidate_keywords" must not be empty`);
  }

  if (card.geography_primary !== null && typeof card.geography_primary !== 'string') {
    throw new Error(`Semantic card validation failed for article ${articleId}: "geography_primary" must be a string or null`);
  }

  for (const label of card.topic_labels) {
    if (!taxonomy.topic_labels.includes(label)) {
      throw new Error(`Semantic card validation failed for article ${articleId}: invalid topic label "${label}"`);
    }
  }

  for (const label of card.strategic_dimensions) {
    if (!taxonomy.strategic_dimensions.includes(label)) {
      throw new Error(`Semantic card validation failed for article ${articleId}: invalid strategic dimension "${label}"`);
    }
  }

  if (!taxonomy.novelty_signals.includes(card.novelty_signal)) {
    throw new Error(`Semantic card validation failed for article ${articleId}: invalid novelty_signal "${card.novelty_signal}"`);
  }

  if (!taxonomy.user_relevance_signals.includes(card.user_relevance_signal)) {
    throw new Error(`Semantic card validation failed for article ${articleId}: invalid user_relevance_signal "${card.user_relevance_signal}"`);
  }

  if (typeof card.factual_summary !== 'string' || card.factual_summary.trim().length === 0) {
    throw new Error(`Semantic card validation failed for article ${articleId}: "factual_summary" must be a non-empty string`);
  }

  if (typeof card.why_it_matters !== 'string' || card.why_it_matters.trim().length === 0) {
    throw new Error(`Semantic card validation failed for article ${articleId}: "why_it_matters" must be a non-empty string`);
  }

  if (
    typeof card.confidence_score !== 'number'
    || Number.isNaN(card.confidence_score)
    || card.confidence_score < 0
    || card.confidence_score > 1
  ) {
    throw new Error(`Semantic card validation failed for article ${articleId}: "confidence_score" must be between 0 and 1`);
  }

  if (!Array.isArray(card.warnings)) {
    throw new Error(`Semantic card validation failed for article ${articleId}: "warnings" must be an array`);
  }

  for (const warning of card.warnings) {
    if (!warning || typeof warning !== 'object' || Array.isArray(warning)) {
      throw new Error(`Semantic card validation failed for article ${articleId}: warnings must contain objects`);
    }
    if (
      typeof warning.code !== 'string'
      || warning.code.trim().length === 0
      || typeof warning.message !== 'string'
      || warning.message.trim().length === 0
    ) {
      throw new Error(`Semantic card validation failed for article ${articleId}: warning entries need "code" and "message"`);
    }
    if (!taxonomy.warning_severities.includes(warning.severity)) {
      throw new Error(`Semantic card validation failed for article ${articleId}: invalid warning severity "${warning.severity}"`);
    }
  }

  for (const keyword of card.candidate_keywords) {
    if (keyword.trim().length === 0 || keyword.length > 32 || keyword.split(/\s+/).length > 4) {
      throw new Error(`Semantic card validation failed for article ${articleId}: candidate keyword "${keyword}" is not concise`);
    }
  }

  return true;
}

export function createSemanticCard(data, taxonomy) {
  const card = {
    article_id: data.article_id,
    source_id: data.source_id,
    title: data.title,
    url: data.url,
    language: data.language ?? null,
    event_type: data.event_type,
    primary_entities: data.primary_entities ?? [],
    secondary_entities: data.secondary_entities ?? [],
    geography_primary: data.geography_primary ?? null,
    geography_secondary: data.geography_secondary ?? [],
    topic_labels: data.topic_labels ?? [],
    strategic_dimensions: data.strategic_dimensions ?? [],
    candidate_keywords: data.candidate_keywords ?? [],
    factual_summary: data.factual_summary ?? '',
    why_it_matters: data.why_it_matters ?? '',
    novelty_signal: data.novelty_signal,
    user_relevance_signal: data.user_relevance_signal,
    confidence_score: Number(data.confidence_score ?? 0),
    warnings: data.warnings ?? [],
    ...data
  };

  validateSemanticCard(card, taxonomy);
  return Object.freeze(card);
}
