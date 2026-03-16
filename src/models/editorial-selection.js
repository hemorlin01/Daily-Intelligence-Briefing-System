const REQUIRED_SELECTED_ITEM_FIELDS = [
  'article_id',
  'cluster_id',
  'source_id',
  'source_display_name',
  'title',
  'url',
  'candidate_keywords',
  'factual_summary',
  'why_it_matters',
  'topic_labels',
  'primary_domain',
  'score_breakdown',
  'final_composite_score',
  'selection_reason_codes',
  'warnings'
];

const REQUIRED_SCORE_COMPONENTS = [
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

const REQUIRED_RESULT_FIELDS = [
  'selected_items',
  'selected_article_ids',
  'selected_count',
  'degraded_mode',
  'under_default_target',
  'run_status',
  'quota_fill_report',
  'source_cap_report',
  'cluster_cap_report',
  'backfill_actions',
  'exclusion_summary'
];

const RUN_STATUS_VALUES = new Set([
  'on_target',
  'under_default_target',
  'degraded'
]);

function assertNonEmptyString(value, fieldName, context) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new Error(`${context}: "${fieldName}" must be a non-empty string`);
  }
}

function assertStringArray(value, fieldName, context) {
  if (!Array.isArray(value) || value.length === 0 || value.some((item) => typeof item !== 'string' || item.trim().length === 0)) {
    throw new Error(`${context}: "${fieldName}" must be a non-empty array of strings`);
  }
}

function assertBoundedNumber(value, fieldName, context) {
  if (typeof value !== 'number' || Number.isNaN(value) || value < 0 || value > 1) {
    throw new Error(`${context}: "${fieldName}" must be a number between 0 and 1`);
  }
}

export function validateSelectedItem(item, editorialRules, knownClusterIds) {
  const context = `Editorial selection validation failed for article ${item?.article_id ?? 'unknown'}`;

  if (!item || typeof item !== 'object' || Array.isArray(item)) {
    throw new Error(`${context}: selected item must be an object`);
  }

  for (const field of REQUIRED_SELECTED_ITEM_FIELDS) {
    if (!(field in item)) {
      throw new Error(`${context}: missing required field "${field}"`);
    }
  }

  assertNonEmptyString(item.article_id, 'article_id', context);
  assertNonEmptyString(item.cluster_id, 'cluster_id', context);
  assertNonEmptyString(item.source_id, 'source_id', context);
  assertNonEmptyString(item.source_display_name, 'source_display_name', context);
  assertNonEmptyString(item.title, 'title', context);
  assertNonEmptyString(item.url, 'url', context);
  assertStringArray(item.candidate_keywords, 'candidate_keywords', context);
  assertNonEmptyString(item.factual_summary, 'factual_summary', context);
  assertNonEmptyString(item.why_it_matters, 'why_it_matters', context);
  assertStringArray(item.topic_labels, 'topic_labels', context);
  assertNonEmptyString(item.primary_domain, 'primary_domain', context);
  assertStringArray(item.selection_reason_codes, 'selection_reason_codes', context);

  if (!editorialRules.domain_quotas[item.primary_domain]) {
    throw new Error(`${context}: unknown primary_domain "${item.primary_domain}"`);
  }

  if (!knownClusterIds.has(item.cluster_id)) {
    throw new Error(`${context}: cluster_id "${item.cluster_id}" is not known`);
  }

  if (!item.score_breakdown || typeof item.score_breakdown !== 'object' || Array.isArray(item.score_breakdown)) {
    throw new Error(`${context}: "score_breakdown" must be an object`);
  }

  for (const component of REQUIRED_SCORE_COMPONENTS) {
    assertBoundedNumber(item.score_breakdown[component], `score_breakdown.${component}`, context);
  }

  assertBoundedNumber(item.final_composite_score, 'final_composite_score', context);

  if (!Array.isArray(item.warnings)) {
    throw new Error(`${context}: "warnings" must be an array`);
  }

  return true;
}

export function createSelectedItem(data, editorialRules, knownClusterIds) {
  const item = {
    article_id: data.article_id,
    cluster_id: data.cluster_id,
    source_id: data.source_id,
    source_display_name: data.source_display_name,
    title: data.title,
    url: data.url,
    candidate_keywords: data.candidate_keywords ?? [],
    factual_summary: data.factual_summary ?? '',
    why_it_matters: data.why_it_matters ?? '',
    topic_labels: data.topic_labels ?? [],
    primary_domain: data.primary_domain,
    score_breakdown: data.score_breakdown,
    final_composite_score: Number(data.final_composite_score ?? 0),
    selection_reason_codes: data.selection_reason_codes ?? [],
    warnings: data.warnings ?? [],
    ...data
  };

  validateSelectedItem(item, editorialRules, knownClusterIds);
  return Object.freeze(item);
}

export function validateEditorialSelectionResult(result, editorialRules, clusters) {
  const context = 'Editorial selection result validation failed';
  if (!result || typeof result !== 'object' || Array.isArray(result)) {
    throw new Error(`${context}: result must be an object`);
  }

  for (const field of REQUIRED_RESULT_FIELDS) {
    if (!(field in result)) {
      throw new Error(`${context}: missing required field "${field}"`);
    }
  }

  if (!Array.isArray(result.selected_items)) {
    throw new Error(`${context}: "selected_items" must be an array`);
  }

  if (!Array.isArray(result.selected_article_ids)) {
    throw new Error(`${context}: "selected_article_ids" must be an array`);
  }

  if (typeof result.degraded_mode !== 'boolean') {
    throw new Error(`${context}: "degraded_mode" must be a boolean`);
  }

  if (typeof result.under_default_target !== 'boolean') {
    throw new Error(`${context}: "under_default_target" must be a boolean`);
  }

  if (!RUN_STATUS_VALUES.has(result.run_status)) {
    throw new Error(`${context}: invalid run_status "${result.run_status}"`);
  }

  if (result.selected_article_ids.length !== result.selected_items.length || result.selected_count !== result.selected_items.length) {
    throw new Error(`${context}: selected item counts are inconsistent`);
  }

  for (const [index, item] of result.selected_items.entries()) {
    if (result.selected_article_ids[index] !== item.article_id) {
      throw new Error(`${context}: "selected_article_ids" must match the final ordered "selected_items"`);
    }
  }

  if (new Set(result.selected_article_ids).size !== result.selected_article_ids.length) {
    throw new Error(`${context}: duplicate article ids appear in the selected set`);
  }

  const knownClusterIds = new Set(clusters.map((cluster) => cluster.cluster_id));
  for (const item of result.selected_items) {
    validateSelectedItem(item, editorialRules, knownClusterIds);
  }

  const sourceCounts = {};
  const clusterCounts = {};
  const domainCounts = {};
  for (const item of result.selected_items) {
    sourceCounts[item.source_id] = (sourceCounts[item.source_id] ?? 0) + 1;
    clusterCounts[item.cluster_id] = (clusterCounts[item.cluster_id] ?? 0) + 1;
    domainCounts[item.primary_domain] = (domainCounts[item.primary_domain] ?? 0) + 1;
  }

  for (const [sourceId, count] of Object.entries(sourceCounts)) {
    if (count > editorialRules.caps.source_max_count) {
      throw new Error(`${context}: source cap exceeded for "${sourceId}"`);
    }
  }

  for (const [clusterId, count] of Object.entries(clusterCounts)) {
    if (count > editorialRules.caps.cluster_max_count) {
      throw new Error(`${context}: cluster cap exceeded for "${clusterId}"`);
    }
  }

  for (const [domain, count] of Object.entries(domainCounts)) {
    if (count > editorialRules.domain_quotas[domain].hard_max_count) {
      throw new Error(`${context}: domain cap exceeded for "${domain}"`);
    }
  }

  if (result.selected_count > editorialRules.selection.maximum_target_count) {
    throw new Error(`${context}: selected count exceeds configured maximum target`);
  }

  if (result.selected_count < editorialRules.selection.minimum_target_count) {
    if (!result.degraded_mode || result.under_default_target || result.run_status !== 'degraded') {
      throw new Error(`${context}: runs below the configured minimum target must be marked degraded`);
    }
  } else if (result.selected_count < editorialRules.selection.default_target_count) {
    if (result.degraded_mode || !result.under_default_target || result.run_status !== 'under_default_target') {
      throw new Error(`${context}: runs between the configured minimum and default targets must be marked as under_default_target`);
    }
  } else if (result.degraded_mode || result.under_default_target || result.run_status !== 'on_target') {
    throw new Error(`${context}: runs at or above the default target must be marked on_target`);
  }

  return true;
}

export function createEditorialSelectionResult(data, editorialRules, clusters) {
  const result = {
    selected_items: data.selected_items ?? [],
    selected_article_ids: data.selected_article_ids ?? [],
    selected_count: Number(data.selected_count ?? 0),
    degraded_mode: Boolean(data.degraded_mode),
    under_default_target: Boolean(data.under_default_target),
    run_status: data.run_status ?? 'on_target',
    quota_fill_report: data.quota_fill_report ?? {},
    source_cap_report: data.source_cap_report ?? {},
    cluster_cap_report: data.cluster_cap_report ?? {},
    backfill_actions: data.backfill_actions ?? [],
    exclusion_summary: data.exclusion_summary ?? {},
    ...data
  };

  validateEditorialSelectionResult(result, editorialRules, clusters);
  return Object.freeze(result);
}
