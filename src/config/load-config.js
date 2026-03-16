import { readFileSync } from 'node:fs';

const SOURCE_CLASS_VALUES = new Set([
  'academic_intellectual',
  'business_consulting_insight',
  'china_policy_economy',
  'climate_sustainability',
  'culture_design_lifestyle',
  'global_hard_news',
  'policy_geopolitics_institutions',
  'technology_digital_economy',
  'urban_infrastructure'
]);

const FETCH_METHOD_VALUES = new Set(['api', 'manual', 'newsletter', 'rss', 'website']);
const PARSER_TYPE_VALUES = new Set(['article', 'json-feed', 'newsletter', 'rss']);
const CANONICALIZATION_POLICY_VALUES = new Set(['paywalled-standard', 'preserve-query', 'standard']);
const PAYWALL_POLICY_VALUES = new Set(['free', 'hard', 'metered', 'mixed']);
const EXPECTED_ARTICLE_TYPE_VALUES = new Set(['analysis', 'essay', 'feature', 'news', 'opinion', 'research']);
const RELIABILITY_STATUS_VALUES = new Set(['experimental', 'secondary', 'trusted']);
const ACTIVE_STATUS_VALUES = new Set(['active', 'inactive']);

const REQUIRED_SOURCE_FIELDS = [
  'source_id',
  'display_name',
  'source_class',
  'language',
  'primary_region',
  'default_topic_affinities',
  'priority_tier',
  'fetch_method',
  'parser_type',
  'canonicalization_policy',
  'paywall_policy',
  'expected_article_type',
  'reliability_status',
  'active_status'
];

function readJsonFile(path) {
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch (error) {
    throw new Error(`Unable to read JSON config at ${path}: ${error.message}`);
  }
}

function assertStringArray(value, fieldName, context) {
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string' || item.trim().length === 0)) {
    throw new Error(`${context}: "${fieldName}" must be an array of non-empty strings`);
  }
}

function assertString(value, fieldName, context) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new Error(`${context}: "${fieldName}" must be a non-empty string`);
  }
}

function assertArrayOfStrings(value, fieldName, context) {
  if (!Array.isArray(value) || value.length === 0 || value.some((item) => typeof item !== 'string' || item.trim().length === 0)) {
    throw new Error(`${context}: "${fieldName}" must be a non-empty array of strings`);
  }
}

function assertEnum(value, allowedValues, fieldName, context) {
  if (!allowedValues.has(value)) {
    throw new Error(`${context}: "${fieldName}" must be one of ${Array.from(allowedValues).join(', ')}`);
  }
}

function assertNumber(value, fieldName, context, predicate) {
  if (typeof value !== 'number' || Number.isNaN(value) || !predicate(value)) {
    throw new Error(`${context}: "${fieldName}" is invalid`);
  }
}

function validateSourceEntry(entry, index) {
  const context = `Invalid source config entry at index ${index}${entry?.source_id ? ` (${entry.source_id})` : ''}`;
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
    throw new Error(`${context}: entry must be an object`);
  }

  for (const field of REQUIRED_SOURCE_FIELDS) {
    if (!(field in entry)) {
      throw new Error(`${context}: missing required field "${field}"`);
    }
  }

  assertString(entry.source_id, 'source_id', context);
  assertString(entry.display_name, 'display_name', context);
  assertString(entry.language, 'language', context);
  assertString(entry.primary_region, 'primary_region', context);
  assertArrayOfStrings(entry.default_topic_affinities, 'default_topic_affinities', context);
  assertNumber(entry.priority_tier, 'priority_tier', context, (value) => Number.isInteger(value) && value >= 1 && value <= 3);
  assertEnum(entry.source_class, SOURCE_CLASS_VALUES, 'source_class', context);
  assertEnum(entry.fetch_method, FETCH_METHOD_VALUES, 'fetch_method', context);
  assertEnum(entry.parser_type, PARSER_TYPE_VALUES, 'parser_type', context);
  assertEnum(entry.canonicalization_policy, CANONICALIZATION_POLICY_VALUES, 'canonicalization_policy', context);
  assertEnum(entry.paywall_policy, PAYWALL_POLICY_VALUES, 'paywall_policy', context);
  assertEnum(entry.expected_article_type, EXPECTED_ARTICLE_TYPE_VALUES, 'expected_article_type', context);
  assertEnum(entry.reliability_status, RELIABILITY_STATUS_VALUES, 'reliability_status', context);
  assertEnum(entry.active_status, ACTIVE_STATUS_VALUES, 'active_status', context);

  if ('allowed_long_form_window_hours' in entry) {
    assertNumber(entry.allowed_long_form_window_hours, 'allowed_long_form_window_hours', context, (value) => value > 36 && value <= 168);
  }
}

export function loadSourceCatalog(path) {
  const catalog = readJsonFile(path);
  if (!catalog || typeof catalog !== 'object' || Array.isArray(catalog)) {
    throw new Error(`Invalid source catalog at ${path}: top-level JSON object required`);
  }

  if (!Array.isArray(catalog.sources) || catalog.sources.length === 0) {
    throw new Error(`Invalid source catalog at ${path}: "sources" must be a non-empty array`);
  }

  const sourcesById = new Map();
  for (const [index, entry] of catalog.sources.entries()) {
    validateSourceEntry(entry, index);
    if (sourcesById.has(entry.source_id)) {
      throw new Error(`Invalid source catalog at ${path}: duplicate source_id "${entry.source_id}"`);
    }
    sourcesById.set(entry.source_id, Object.freeze({ ...entry }));
  }

  return Object.freeze({
    catalogVersion: catalog.catalog_version ?? 'unversioned',
    sources: sourcesById
  });
}

export function loadThresholds(path) {
  const thresholds = readJsonFile(path);
  if (!thresholds?.candidate_windows || !thresholds?.content_thresholds || !thresholds?.quality_scoring || !thresholds?.deduplication || !thresholds?.article_like_rules) {
    throw new Error(`Invalid thresholds config at ${path}: missing required top-level sections`);
  }

  return Object.freeze(thresholds);
}

export function loadSemanticTaxonomy(path) {
  const taxonomy = readJsonFile(path);
  const context = `Invalid semantic taxonomy at ${path}`;
  if (!taxonomy || typeof taxonomy !== 'object' || Array.isArray(taxonomy)) {
    throw new Error(`${context}: top-level JSON object required`);
  }

  assertStringArray(taxonomy.event_types, 'event_types', context);
  assertStringArray(taxonomy.strategic_dimensions, 'strategic_dimensions', context);
  assertStringArray(taxonomy.topic_labels, 'topic_labels', context);
  assertStringArray(taxonomy.novelty_signals, 'novelty_signals', context);
  assertStringArray(taxonomy.user_relevance_signals, 'user_relevance_signals', context);
  assertStringArray(taxonomy.warning_severities, 'warning_severities', context);

  return Object.freeze(taxonomy);
}

export function loadSemanticRules(path) {
  const rules = readJsonFile(path);
  const context = `Invalid semantic rules config at ${path}`;
  if (!rules?.language_behavior || !rules?.summary_rules || !rules?.confidence_rules || !rules?.keyword_rules) {
    throw new Error(`${context}: missing required top-level sections`);
  }

  const mapFields = [
    'event_type_keywords',
    'topic_keywords',
    'strategic_dimension_keywords',
    'geography_keywords'
  ];

  for (const field of mapFields) {
    if (!rules[field] || typeof rules[field] !== 'object' || Array.isArray(rules[field])) {
      throw new Error(`${context}: "${field}" must be an object`);
    }
  }

  if (!Array.isArray(rules.english_stop_entities)) {
    throw new Error(`${context}: "english_stop_entities" must be an array`);
  }

  return Object.freeze(rules);
}

export function loadEditorialRules(path) {
  const rules = readJsonFile(path);
  const context = `Invalid editorial rules config at ${path}`;

  if (!rules?.selection || !rules?.caps || !rules?.clustering || !rules?.scoring || !rules?.domain_quotas) {
    throw new Error(`${context}: missing required top-level sections`);
  }

  const requiredSelectionFields = [
    'default_target_count',
    'minimum_target_count',
    'maximum_target_count',
    'minimum_candidate_score',
    'backfill_minimum_candidate_score',
    'healthy_pool_size'
  ];
  for (const field of requiredSelectionFields) {
    assertNumber(rules.selection[field], `selection.${field}`, context, (value) => value > 0);
  }

  if (
    rules.selection.minimum_target_count > rules.selection.default_target_count
    || rules.selection.default_target_count > rules.selection.maximum_target_count
  ) {
    throw new Error(`${context}: selection target bounds are inconsistent`);
  }

  const requiredCapFields = [
    'source_max_count',
    'cluster_max_count',
    'preferred_cluster_count',
    'source_class_dominance_warning_ratio'
  ];
  for (const field of requiredCapFields) {
    assertNumber(rules.caps[field], `caps.${field}`, context, (value) => value > 0);
  }

  if (rules.caps.preferred_cluster_count > rules.caps.cluster_max_count) {
    throw new Error(`${context}: preferred cluster count cannot exceed cluster max count`);
  }

  if (!rules.clustering.weights || typeof rules.clustering.weights !== 'object' || Array.isArray(rules.clustering.weights)) {
    throw new Error(`${context}: "clustering.weights" must be an object`);
  }

  assertNumber(rules.clustering.similarity_threshold, 'clustering.similarity_threshold', context, (value) => value > 0 && value <= 1);
  assertNumber(rules.clustering.min_signal_matches, 'clustering.min_signal_matches', context, (value) => Number.isInteger(value) && value >= 1);
  assertNumber(rules.clustering.time_decay_hours, 'clustering.time_decay_hours', context, (value) => value > 0);

  if (!rules.clustering.angle_diversity || typeof rules.clustering.angle_diversity !== 'object' || Array.isArray(rules.clustering.angle_diversity)) {
    throw new Error(`${context}: "clustering.angle_diversity" must be an object`);
  }

  for (const field of ['title_similarity_max', 'keyword_overlap_max', 'summary_overlap_max']) {
    assertNumber(rules.clustering.angle_diversity[field], `clustering.angle_diversity.${field}`, context, (value) => value > 0 && value <= 1);
  }

  if (!rules.scoring.weights || typeof rules.scoring.weights !== 'object' || Array.isArray(rules.scoring.weights)) {
    throw new Error(`${context}: "scoring.weights" must be an object`);
  }

  const requiredScoreFields = [
    'source_priority_score',
    'freshness_score',
    'extraction_quality_score',
    'semantic_confidence_score',
    'user_relevance_score',
    'novelty_score',
    'domain_need_score',
    'cluster_uniqueness_score',
    'long_form_bonus',
    'redundancy_penalty'
  ];
  for (const field of requiredScoreFields) {
    assertNumber(rules.scoring.weights[field], `scoring.weights.${field}`, context, (value) => value >= 0 && value <= 1);
  }

  if (!rules.scoring.source_priority_by_tier || typeof rules.scoring.source_priority_by_tier !== 'object' || Array.isArray(rules.scoring.source_priority_by_tier)) {
    throw new Error(`${context}: "scoring.source_priority_by_tier" must be an object`);
  }
  if (!rules.scoring.user_relevance_scores || typeof rules.scoring.user_relevance_scores !== 'object' || Array.isArray(rules.scoring.user_relevance_scores)) {
    throw new Error(`${context}: "scoring.user_relevance_scores" must be an object`);
  }
  if (!rules.scoring.novelty_scores || typeof rules.scoring.novelty_scores !== 'object' || Array.isArray(rules.scoring.novelty_scores)) {
    throw new Error(`${context}: "scoring.novelty_scores" must be an object`);
  }
  if (!rules.scoring.freshness_windows_hours || typeof rules.scoring.freshness_windows_hours !== 'object' || Array.isArray(rules.scoring.freshness_windows_hours)) {
    throw new Error(`${context}: "scoring.freshness_windows_hours" must be an object`);
  }
  if (!Array.isArray(rules.scoring.long_form_bonus_event_types) || !Array.isArray(rules.scoring.long_form_bonus_article_types)) {
    throw new Error(`${context}: long-form bonus lists must be arrays`);
  }

  if (!Array.isArray(rules.domain_priority_order) || rules.domain_priority_order.length === 0) {
    throw new Error(`${context}: "domain_priority_order" must be a non-empty array`);
  }

  const domainNames = Object.keys(rules.domain_quotas);
  if (domainNames.length === 0) {
    throw new Error(`${context}: "domain_quotas" must define at least one domain`);
  }

  for (const domain of domainNames) {
    const quota = rules.domain_quotas[domain];
    if (!quota || typeof quota !== 'object' || Array.isArray(quota)) {
      throw new Error(`${context}: quota for "${domain}" must be an object`);
    }

    for (const field of ['target_count', 'hard_max_count', 'soft_min_count', 'priority']) {
      assertNumber(quota[field], `domain_quotas.${domain}.${field}`, context, (value) => Number.isInteger(value) && value >= 0);
    }

    if (quota.target_count > quota.hard_max_count) {
      throw new Error(`${context}: domain "${domain}" target_count cannot exceed hard_max_count`);
    }

    if (quota.soft_min_count > quota.target_count) {
      throw new Error(`${context}: domain "${domain}" soft_min_count cannot exceed target_count`);
    }

    assertStringArray(quota.adjacent_domains ?? [], `domain_quotas.${domain}.adjacent_domains`, context);
  }

  for (const domain of rules.domain_priority_order) {
    if (!rules.domain_quotas[domain]) {
      throw new Error(`${context}: domain_priority_order references unknown domain "${domain}"`);
    }
  }

  for (const quota of Object.values(rules.domain_quotas)) {
    for (const adjacentDomain of quota.adjacent_domains) {
      if (!rules.domain_quotas[adjacentDomain]) {
        throw new Error(`${context}: adjacent domain "${adjacentDomain}" is not defined in domain_quotas`);
      }
    }
  }

  if (!Array.isArray(rules.strategic_backfill_priority) || rules.strategic_backfill_priority.length === 0) {
    throw new Error(`${context}: "strategic_backfill_priority" must be a non-empty array`);
  }

  return Object.freeze(rules);
}

export function loadRenderingRules(path) {
  const rules = readJsonFile(path);
  const context = `Invalid rendering rules config at ${path}`;

  if (!rules?.briefing || !rules?.status_display || !rules?.blocks || !rules?.email || !rules?.telegram || !rules?.markdown) {
    throw new Error(`${context}: missing required top-level sections`);
  }

  assertString(rules.briefing.title, 'briefing.title', context);
  assertString(rules.briefing.timezone, 'briefing.timezone', context);
  assertString(rules.briefing.date_format, 'briefing.date_format', context);

  if (typeof rules.status_display.show_run_status !== 'boolean') {
    throw new Error(`${context}: "status_display.show_run_status" must be a boolean`);
  }
  if (!rules.status_display.labels || typeof rules.status_display.labels !== 'object' || Array.isArray(rules.status_display.labels)) {
    throw new Error(`${context}: "status_display.labels" must be an object`);
  }
  for (const key of ['on_target', 'under_default_target', 'degraded']) {
    assertString(rules.status_display.labels[key], `status_display.labels.${key}`, context);
  }

  if (!rules.blocks.definitions || typeof rules.blocks.definitions !== 'object' || Array.isArray(rules.blocks.definitions)) {
    throw new Error(`${context}: "blocks.definitions" must be an object`);
  }
  assertStringArray(rules.blocks.order, 'blocks.order', context);
  for (const blockId of rules.blocks.order) {
    const definition = rules.blocks.definitions[blockId];
    if (!definition || typeof definition !== 'object' || Array.isArray(definition)) {
      throw new Error(`${context}: missing block definition for "${blockId}"`);
    }
    assertString(definition.label, `blocks.definitions.${blockId}.label`, context);
    assertStringArray(definition.domains, `blocks.definitions.${blockId}.domains`, context);
  }

  const domainToBlock = {};
  for (const blockId of rules.blocks.order) {
    for (const domain of rules.blocks.definitions[blockId].domains) {
      if (domainToBlock[domain]) {
        throw new Error(`${context}: domain "${domain}" is mapped to multiple rendering blocks`);
      }
      domainToBlock[domain] = blockId;
    }
  }

  if (typeof rules.email.include_footer !== 'boolean' || typeof rules.email.show_keywords !== 'boolean') {
    throw new Error(`${context}: email flags must be booleans`);
  }
  assertString(rules.email.format, 'email.format', context);

  const telegramNumberFields = [
    'length_budget_chars',
    'summary_max_chars',
    'why_max_chars',
    'compact_summary_max_chars',
    'compact_why_max_chars',
    'minimal_summary_max_chars',
    'minimal_why_max_chars'
  ];
  for (const field of telegramNumberFields) {
    assertNumber(rules.telegram[field], `telegram.${field}`, context, (value) => Number.isInteger(value) && value > 0);
  }
  assertString(rules.telegram.format, 'telegram.format', context);
  assertString(rules.telegram.block_heading_prefix, 'telegram.block_heading_prefix', context);

  if (
    rules.telegram.minimal_summary_max_chars > rules.telegram.compact_summary_max_chars
    || rules.telegram.compact_summary_max_chars > rules.telegram.summary_max_chars
    || rules.telegram.minimal_why_max_chars > rules.telegram.compact_why_max_chars
    || rules.telegram.compact_why_max_chars > rules.telegram.why_max_chars
  ) {
    throw new Error(`${context}: telegram truncation thresholds are inconsistent`);
  }

  if (typeof rules.markdown.include_footer !== 'boolean') {
    throw new Error(`${context}: "markdown.include_footer" must be a boolean`);
  }

  return Object.freeze({
    ...rules,
    blocks: Object.freeze({
      ...rules.blocks,
      domain_to_block: Object.freeze(domainToBlock)
    })
  });
}

export function loadDeliveryRules(path) {
  const rules = readJsonFile(path);
  const context = `Invalid delivery rules config at ${path}`;

  if (!rules?.artifacts || !rules?.delivery || !rules?.retry || !rules?.schedule) {
    throw new Error(`${context}: missing required top-level sections`);
  }

  assertString(rules.artifacts.output_root, 'artifacts.output_root', context);
  assertString(rules.artifacts.ledger_path, 'artifacts.ledger_path', context);

  for (const channel of ['email', 'telegram']) {
    const config = rules.delivery[channel];
    if (!config || typeof config !== 'object' || Array.isArray(config)) {
      throw new Error(`${context}: missing delivery config for "${channel}"`);
    }
    if (typeof config.enabled !== 'boolean') {
      throw new Error(`${context}: "delivery.${channel}.enabled" must be a boolean`);
    }
    assertString(config.mode, `delivery.${channel}.mode`, context);
    assertString(config.destination_env, `delivery.${channel}.destination_env`, context);
    assertString(config.destination_fallback, `delivery.${channel}.destination_fallback`, context);
  }

  assertNumber(rules.retry.max_attempts, 'retry.max_attempts', context, (value) => Number.isInteger(value) && value >= 1 && value <= 10);
  assertStringArray(rules.retry.retryable_error_codes, 'retry.retryable_error_codes', context);

  if (typeof rules.schedule.enabled !== 'boolean') {
    throw new Error(`${context}: "schedule.enabled" must be a boolean`);
  }
  assertString(rules.schedule.timezone, 'schedule.timezone', context);
  assertStringArray(rules.schedule.days, 'schedule.days', context);
  assertNumber(rules.schedule.hour, 'schedule.hour', context, (value) => Number.isInteger(value) && value >= 0 && value <= 23);
  assertNumber(rules.schedule.minute, 'schedule.minute', context, (value) => Number.isInteger(value) && value >= 0 && value <= 59);

  return Object.freeze(rules);
}
