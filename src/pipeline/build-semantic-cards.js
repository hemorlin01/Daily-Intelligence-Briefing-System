import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { loadSemanticRules, loadSemanticTaxonomy } from '../config/load-config.js';
import { createSemanticCard } from '../models/semantic-card.js';
import { buildTokenFingerprint, diceCoefficient, jaccardSimilarity, normalizeTitleForComparison, normalizeWhitespace, tokenize } from '../utils/text.js';

const DEFAULT_SEMANTIC_TAXONOMY_PATH = resolve(process.cwd(), 'config', 'semantic-taxonomy.json');
const DEFAULT_SEMANTIC_RULES_PATH = resolve(process.cwd(), 'config', 'semantic-rules.json');

function addWarning(target, code, message, severity = 'warning') {
  if (!target.some((warning) => warning.code === code)) {
    target.push({ code, message, severity });
  }
}

function getTextBlob(record) {
  return [
    record.title,
    record.raw_snippet,
    record.source_provided_summary,
    record.canonical_text
  ].filter(Boolean).join(' ');
}

function isProbablyChinese(text) {
  return /[\u3400-\u9fff]/u.test(text);
}

function detectLanguageAmbiguity(record, textBlob, warnings) {
  const hasChinese = /[\u3400-\u9fff]/u.test(textBlob);
  const hasLatinWords = /[A-Za-z]{4,}/.test(textBlob);

  if (record.language === 'unknown' || !record.language) {
    addWarning(warnings, 'language_unclear', 'Source language is missing or unclear.', 'warning');
    return;
  }

  if ((record.language === 'zh' && hasLatinWords) || (record.language === 'en' && hasChinese)) {
    addWarning(warnings, 'language_mixed', 'The article text appears mixed-language.', 'warning');
  }

  if (hasChinese && hasLatinWords && record.language !== 'zh' && record.language !== 'en') {
    addWarning(warnings, 'language_unclear', 'The article language mix is ambiguous.', 'warning');
  }
}

function splitSentences(text, language) {
  const normalized = normalizeWhitespace(text);
  if (!normalized) {
    return [];
  }

  const splitter = language === 'zh'
    ? /(?<=[。！？])/u
    : /(?<=[.!?])\s+/u;

  return normalized
    .split(splitter)
    .map((sentence) => sentence.trim())
    .filter(Boolean);
}

function pickLeadSentence(record, language) {
  const candidates = [
    ...(record.canonical_text ? splitSentences(record.canonical_text, language) : []),
    ...(record.raw_snippet ? splitSentences(record.raw_snippet, language) : []),
    ...(record.source_provided_summary ? splitSentences(record.source_provided_summary, language) : []),
    record.title
  ];

  for (const candidate of candidates) {
    if (candidate && candidate.length >= 20) {
      return candidate;
    }
  }

  return record.title;
}

function truncateForLanguage(text, language, maxChars) {
  const normalized = normalizeWhitespace(text);
  if (normalized.length <= maxChars) {
    return normalized;
  }

  return `${normalized.slice(0, maxChars - 1).trim()}…`;
}

function summarizeFactually(record, rules, warnings) {
  const language = record.language === 'zh' ? 'zh' : 'en';
  const maxChars = language === 'zh'
    ? rules.summary_rules.non_english_max_chars
    : rules.summary_rules.english_max_chars;
  const lead = pickLeadSentence(record, language);
  const summary = truncateForLanguage(lead, language, maxChars);

  if (!record.canonical_text || record.canonical_text.length < 200) {
    addWarning(warnings, 'weak_canonical_text', 'Canonical text is weak; factual summary leans on title/snippet.', 'warning');
  }

  return summary;
}

function scoreKeywordMatches(textBlob, keywordMap) {
  const normalized = normalizeTitleForComparison(textBlob);
  const scores = [];

  for (const [label, keywords] of Object.entries(keywordMap)) {
    let score = 0;
    for (const keyword of keywords) {
      const normalizedKeyword = normalizeTitleForComparison(keyword);
      if (normalizedKeyword && normalized.includes(normalizedKeyword)) {
        score += 1;
      }
    }
    scores.push({ label, score });
  }

  return scores.sort((left, right) => right.score - left.score || left.label.localeCompare(right.label));
}

function inferEventType(record, rules) {
  const textBlob = getTextBlob(record);
  const scored = scoreKeywordMatches(textBlob, rules.event_type_keywords);
  const top = scored[0];

  if (top && top.score > 0) {
    return { label: top.label, matchedBy: 'keyword' };
  }

  if (['analysis', 'essay', 'feature', 'research'].includes(record.article_type)) {
    return { label: 'long_form_analysis', matchedBy: 'article_type' };
  }

  if (record.article_type === 'opinion') {
    return { label: 'opinion_or_argument', matchedBy: 'article_type' };
  }

  return { label: 'market_signal', matchedBy: 'fallback' };
}

function inferTopicLabels(record, rules, warnings) {
  const textBlob = getTextBlob(record);
  const scored = scoreKeywordMatches(textBlob, rules.topic_keywords)
    .filter((entry) => entry.score > 0)
    .slice(0, 3)
    .map((entry) => entry.label);

  if (scored.length > 0) {
    return scored;
  }

  if (Array.isArray(record.default_topic_affinities) && record.default_topic_affinities.length > 0) {
    addWarning(warnings, 'topic_fallback_to_source_affinities', 'Topic labels fell back to source-config affinities.', 'info');
    return record.default_topic_affinities.slice(0, 3);
  }

  addWarning(warnings, 'uncontrolled_label_fallback', 'No strong topic signal was found; fallback topic label applied.', 'warning');
  return ['policy_analysis'];
}

function inferStrategicDimensions(record, rules) {
  const textBlob = getTextBlob(record);
  const scored = scoreKeywordMatches(textBlob, rules.strategic_dimension_keywords)
    .filter((entry) => entry.score > 0)
    .slice(0, 3)
    .map((entry) => entry.label);

  if (scored.length > 0) {
    return scored;
  }

  if (record.topic_labels?.includes('technology')) {
    return ['digital_infrastructure'];
  }

  if (record.topic_labels?.includes('climate_transition')) {
    return ['energy_transition'];
  }

  return [];
}

function extractEnglishEntities(text, stopEntities) {
  const matches = text.match(/\b(?:[A-Z][a-z]+|[A-Z]{2,})(?:\s+(?:[A-Z][a-z]+|[A-Z]{2,})){0,3}\b/g) ?? [];
  return matches
    .map((match) => match.trim())
    .filter((match) => !stopEntities.includes(match))
    .filter((match) => match.length > 1);
}

function extractChineseEntities(text) {
  const matches = text.match(/[\u3400-\u9fff]{2,12}(?:公司|集团|政府|部|局|银行|大学|委员会|平台|研究院)/gu) ?? [];
  return matches.map((match) => match.trim());
}

function uniqueOrdered(values) {
  const seen = new Set();
  const ordered = [];
  for (const value of values) {
    if (!value || seen.has(value)) {
      continue;
    }
    seen.add(value);
    ordered.push(value);
  }
  return ordered;
}

function inferEntities(record, rules, warnings) {
  const combined = [record.title, record.raw_snippet, record.canonical_text].filter(Boolean).join(' ');
  const entities = uniqueOrdered([
    ...extractEnglishEntities(combined, rules.english_stop_entities),
    ...extractChineseEntities(combined)
  ]);

  const primary = entities.slice(0, 3);
  const secondary = entities.slice(3, 6);

  if (primary.length === 0) {
    addWarning(warnings, 'entity_extraction_uncertain', 'No strong entities were extracted from the article.', 'warning');
  } else if (primary.every((entity) => entity.length < 3)) {
    addWarning(warnings, 'entity_extraction_uncertain', 'Extracted entities are weak or overly generic.', 'warning');
  }

  return { primary, secondary };
}

function inferGeographies(record, rules, warnings) {
  const textBlob = getTextBlob(record).toLowerCase();
  const matches = [];

  for (const [label, keywords] of Object.entries(rules.geography_keywords)) {
    const score = keywords.reduce((total, keyword) => total + (textBlob.includes(keyword.toLowerCase()) ? 1 : 0), 0);
    if (score > 0) {
      matches.push({ label, score });
    }
  }

  matches.sort((left, right) => right.score - left.score || left.label.localeCompare(right.label));
  if (matches.length === 0) {
    addWarning(warnings, 'geography_ambiguity', 'No clear geography was extracted from the article.', 'warning');
    return { primary: null, secondary: [] };
  }

  if (matches.length > 2 && matches[0].score === matches[1].score) {
    addWarning(warnings, 'geography_ambiguity', 'Multiple geographies scored similarly.', 'info');
  }

  return {
    primary: matches[0].label,
    secondary: matches.slice(1, 3).map((entry) => entry.label)
  };
}

function inferNoveltySignal(record, eventType, warnings) {
  const title = normalizeTitleForComparison(record.title);
  if (/(update|follow up|follow-on|again|still|latest|continued)/.test(title)) {
    return { signal: 'follow_on_update', confidence: 0.65 };
  }

  if (['long_form_analysis', 'opinion_or_argument'].includes(eventType)) {
    return { signal: 'new_angle', confidence: 0.74 };
  }

  if ((record.age_hours ?? 999) <= 12) {
    return { signal: 'new_event', confidence: 0.72 };
  }

  if (!record.canonical_text || record.canonical_text.length < 200) {
    addWarning(warnings, 'low_novelty_confidence', 'Novelty signal is based on limited article substance.', 'warning');
    return { signal: 'repeated_coverage', confidence: 0.38 };
  }

  return { signal: 'new_event', confidence: 0.62 };
}

function inferUserRelevanceSignal(topicLabels, sourcePriorityTier) {
  const highPriorityTopics = new Set(['global_macro', 'technology', 'china_economy', 'geopolitics', 'climate_transition']);
  if (topicLabels.some((label) => highPriorityTopics.has(label)) && sourcePriorityTier <= 2) {
    return 'high';
  }

  if (topicLabels.includes('urban_systems') || topicLabels.includes('digital_economy') || topicLabels.includes('policy_analysis')) {
    return 'medium';
  }

  return 'low';
}

function buildCandidateKeywords({ entities, geographies, topicLabels, strategicDimensions, rules }) {
  const seeds = uniqueOrdered([
    ...entities.primary,
    ...entities.secondary,
    geographies.primary,
    ...geographies.secondary,
    ...topicLabels,
    ...strategicDimensions
  ]).filter(Boolean);

  return seeds
    .map((keyword) => normalizeWhitespace(keyword))
    .filter((keyword) => keyword.length > 0 && keyword.length <= rules.keyword_rules.max_keyword_chars)
    .filter((keyword) => keyword.split(/\s+/).length <= rules.keyword_rules.max_keyword_words)
    .slice(0, rules.keyword_rules.max_keywords);
}

function implicationFromTopic(topic) {
  const map = {
    global_macro: 'the broader macro environment',
    geopolitics: 'cross-border risk and policy positioning',
    technology: 'technology competition and deployment',
    china_economy: 'China-facing economic strategy',
    climate_transition: 'the pace of climate and energy transition',
    urban_systems: 'city-level infrastructure and governance',
    digital_economy: 'digital business models and platforms',
    policy_analysis: 'institutional and regulatory decision-making',
    culture_design: 'cultural and design signals',
    lifestyle_signals: 'consumer and lifestyle signals'
  };

  return map[topic] ?? 'broader strategic conditions';
}

function implicationFromStrategicDimension(label) {
  const map = {
    supply_chain: 'supply-chain resilience',
    industrial_policy: 'industrial policy direction',
    climate_risk: 'climate exposure and adaptation',
    platform_power: 'platform leverage and competition',
    capital_markets: 'capital allocation and market pricing',
    consumer_shift: 'changing consumer behavior',
    energy_transition: 'energy-system transition',
    urban_governance: 'urban governance choices',
    digital_infrastructure: 'digital infrastructure build-out',
    regulation: 'regulatory pressure',
    labor: 'labor-market pressure',
    geopolitics: 'geopolitical positioning',
    trade: 'trade flows and constraints',
    public_health: 'public-health resilience',
    AI_competition: 'AI competition'
  };
  return map[label] ?? 'structural relevance';
}

function buildWhySubject(entities, geographies, eventType) {
  const primaryEntity = entities.primary[0] ?? null;
  if (primaryEntity && geographies.primary) {
    return `${primaryEntity} in ${geographies.primary}`;
  }

  if (primaryEntity) {
    return primaryEntity;
  }

  if (geographies.primary) {
    return `${eventType.replace(/_/g, ' ')} developments in ${geographies.primary}`;
  }

  return `this ${eventType.replace(/_/g, ' ')}`;
}

function buildWhyImpactPhrase(topicLabels, strategicDimensions) {
  const primaryTopic = topicLabels[0] ?? 'policy_analysis';
  const primaryDimension = strategicDimensions[0] ?? null;
  return primaryDimension
    ? implicationFromStrategicDimension(primaryDimension)
    : implicationFromTopic(primaryTopic);
}

function buildWhyTemplate(eventType, subject, impactPhrase) {
  const templates = {
    policy_move: `${subject} could reset ${impactPhrase} assumptions as official priorities start to move.`,
    regulatory_shift: `${subject} could change ${impactPhrase} by tightening the rules around how companies operate.`,
    legal_action: `${subject} could reshape ${impactPhrase} because legal pressure can alter strategic room to maneuver.`,
    earnings_result: `${subject} is a useful read-through for ${impactPhrase}, especially around how operators and investors recalibrate.`,
    market_signal: `${subject} is a useful signal for ${impactPhrase}, not just a one-day headline.`,
    funding_or_deal: `${subject} could shift ${impactPhrase} by changing capital, ownership, or competitive positioning.`,
    product_launch: `${subject} could alter ${impactPhrase} if it changes the pace or direction of competitive execution.`,
    executive_change: `${subject} could alter ${impactPhrase} if leadership priorities start to shift.`,
    infrastructure_project: `${subject} could change ${impactPhrase} by setting a new operating baseline for later decisions.`,
    scientific_finding: `${subject} could shift ${impactPhrase} because new evidence often changes the baseline for policy or investment choices.`,
    conflict_escalation: `${subject} raises the stakes for ${impactPhrase}, with broader effects on risk planning and policy assumptions.`,
    long_form_analysis: `${subject} reframes ${impactPhrase} in a way that could shape how later developments are interpreted.`,
    opinion_or_argument: `${subject} reframes ${impactPhrase} and may influence how decision-makers read the next set of developments.`
  };

  return templates[eventType] ?? `${subject} could alter ${impactPhrase} in ways that matter beyond the immediate article.`;
}

function buildWhyItMatters(record, entities, geographies, eventType, topicLabels, strategicDimensions, rules) {
  const subject = buildWhySubject(entities, geographies, eventType);
  const impactPhrase = buildWhyImpactPhrase(topicLabels, strategicDimensions);
  const sentence = buildWhyTemplate(eventType, subject, impactPhrase);

  const primaryTopic = topicLabels[0] ?? 'policy_analysis';
  return truncateForLanguage(
    sentence,
    rules.language_behavior.why_it_matters_default_language,
    rules.summary_rules.why_it_matters_max_chars
  );
}

function computeOverlap(summary, whyItMatters, rules) {
  const lexical = diceCoefficient(summary, whyItMatters);
  const summaryTokens = buildTokenFingerprint(summary, 240);
  const whyTokens = buildTokenFingerprint(whyItMatters, 240);
  const tokenOverlap = jaccardSimilarity(summaryTokens, whyTokens);

  return {
    lexical,
    token: tokenOverlap,
    exceeds: lexical >= rules.summary_rules.overlap_threshold || tokenOverlap >= rules.summary_rules.token_overlap_threshold
  };
}

function enforceSummarySeparation({ summary, whyItMatters, record, entities, geographies, eventType, topicLabels, strategicDimensions, rules, warnings }) {
  let finalWhyItMatters = whyItMatters;
  let overlap = computeOverlap(summary, finalWhyItMatters, rules);

  if (!overlap.exceeds) {
    return { whyItMatters: finalWhyItMatters, overlap };
  }

  finalWhyItMatters = buildWhyItMatters(
    record,
    entities,
    geographies,
    eventType,
    topicLabels,
    strategicDimensions,
    rules
  );
  overlap = computeOverlap(summary, finalWhyItMatters, rules);

  if (overlap.exceeds) {
    addWarning(warnings, 'summary_why_overlap', 'Factual summary and why_it_matters remain too similar.', 'warning');
  }

  return { whyItMatters: finalWhyItMatters, overlap };
}

function computeConfidenceScore({ record, eventTypeMatch, entities, geographies, warnings, noveltyConfidence, overlap }) {
  let score = 0.35;
  score += Math.min(record.extraction_quality_score, 1) * 0.35;
  score += eventTypeMatch === 'keyword' ? 0.1 : 0.05;
  score += entities.primary.length > 0 ? 0.08 : 0;
  score += geographies.primary ? 0.05 : 0;
  score += noveltyConfidence * 0.07;
  score -= warnings.length * 0.04;
  if (overlap.exceeds) {
    score -= 0.08;
  }

  return Math.max(0, Math.min(1, Number(score.toFixed(4))));
}

function extractSemanticCard(record, taxonomy, rules) {
  const warnings = [];
  const textBlob = getTextBlob(record);

  detectLanguageAmbiguity(record, textBlob, warnings);

  if (record.extraction_quality_score < rules.confidence_rules.weak_input_threshold) {
    addWarning(warnings, 'low_input_quality', 'Semantic extraction is operating on a weak upstream article.', 'warning');
  }

  const eventType = inferEventType(record, rules);
  if (eventType.matchedBy === 'fallback') {
    addWarning(warnings, 'event_type_fallback', 'Event type fell back to a generic default.', 'info');
  }
  const topicLabels = inferTopicLabels(record, rules, warnings);
  record.topic_labels = topicLabels;
  const strategicDimensions = inferStrategicDimensions(record, rules);
  const entities = inferEntities(record, rules, warnings);
  const geographies = inferGeographies(record, rules, warnings);
  record.geography_primary = geographies.primary;
  const factualSummary = summarizeFactually(record, rules, warnings);
  const novelty = inferNoveltySignal(record, eventType.label, warnings);
  const userRelevanceSignal = inferUserRelevanceSignal(topicLabels, record.source_priority_tier ?? 3);
  const candidateKeywords = buildCandidateKeywords({
    entities,
    geographies,
    topicLabels,
    strategicDimensions,
    rules
  });

  let whyItMatters = buildWhyItMatters(
    record,
    entities,
    geographies,
    eventType.label,
    topicLabels,
    strategicDimensions,
    rules
  );
  const separation = enforceSummarySeparation({
    summary: factualSummary,
    whyItMatters,
    record,
    entities,
    geographies,
    eventType: eventType.label,
    topicLabels,
    strategicDimensions,
    rules,
    warnings
  });
  whyItMatters = separation.whyItMatters;

  const confidenceScore = computeConfidenceScore({
    record,
    eventTypeMatch: eventType.matchedBy,
    entities,
    geographies,
    warnings,
    noveltyConfidence: novelty.confidence,
    overlap: separation.overlap
  });

  if (confidenceScore < rules.confidence_rules.low_confidence_threshold) {
    addWarning(warnings, 'low_confidence_extraction', 'Semantic extraction confidence is low.', 'warning');
  }

  return createSemanticCard({
    article_id: record.article_id,
    source_id: record.source_id,
    title: record.title,
    url: record.url,
    language: record.language,
    event_type: eventType.label,
    primary_entities: entities.primary,
    secondary_entities: entities.secondary,
    geography_primary: geographies.primary,
    geography_secondary: geographies.secondary,
    topic_labels: topicLabels,
    strategic_dimensions: strategicDimensions,
    candidate_keywords: candidateKeywords,
    factual_summary: factualSummary,
    why_it_matters: whyItMatters,
    novelty_signal: novelty.signal,
    user_relevance_signal: userRelevanceSignal,
    confidence_score: confidenceScore,
    warnings,
    metadata: {
      publication_time_utc: record.publication_time_utc ?? null,
      publication_time_local: record.publication_time_local ?? null,
      extraction_quality_score: record.extraction_quality_score ?? 0,
      extraction_quality_breakdown: record.extraction_quality_breakdown ?? null,
      source_priority_tier: record.source_priority_tier ?? 3,
      source_class: record.source_class ?? 'unknown',
      article_type: record.article_type ?? null,
      candidate_disposition: record.candidate_disposition ?? 'main',
      canonical_fetched_at: record.fetched_at ?? null,
      source_display_name: record.source_display_name ?? record.source_id,
      source_default_topic_affinities: record.default_topic_affinities ?? [],
      extraction_input_quality: record.extraction_quality_score,
      event_type_match: eventType.matchedBy,
      overlap_scores: separation.overlap,
      attempted_empty_topic_labels: topicLabels.length === 0,
      attempted_empty_candidate_keywords: candidateKeywords.length === 0
    }
  }, taxonomy);
}

function incrementCounter(target, key) {
  target[key] = (target[key] ?? 0) + 1;
}

function buildConfidenceDistribution(cards) {
  const distribution = {
    '0.00-0.24': 0,
    '0.25-0.49': 0,
    '0.50-0.74': 0,
    '0.75-1.00': 0
  };

  for (const card of cards) {
    if (card.confidence_score < 0.25) {
      distribution['0.00-0.24'] += 1;
    } else if (card.confidence_score < 0.5) {
      distribution['0.25-0.49'] += 1;
    } else if (card.confidence_score < 0.75) {
      distribution['0.50-0.74'] += 1;
    } else {
      distribution['0.75-1.00'] += 1;
    }
  }

  return distribution;
}

function buildSemanticDiagnostics({ runTimestamp, cards, failures, rules }) {
  const warningCounts = {};
  const eventTypeDistribution = {};
  const topicLabelDistribution = {};
  const strategicDimensionDistribution = {};
  const languageDistribution = {};
  const perSourceWarningSummary = {};
  const failureCountsByType = {};
  let emptyTopicLabelAttemptCount = 0;
  let emptyCandidateKeywordAttemptCount = 0;

  for (const card of cards) {
    incrementCounter(eventTypeDistribution, card.event_type);
    incrementCounter(languageDistribution, card.language ?? 'unknown');

    for (const label of card.topic_labels) {
      incrementCounter(topicLabelDistribution, label);
    }

    for (const label of card.strategic_dimensions) {
      incrementCounter(strategicDimensionDistribution, label);
    }

    const sourceWarnings = perSourceWarningSummary[card.source_id] ?? { total_cards: 0, warning_counts: {} };
    sourceWarnings.total_cards += 1;
    perSourceWarningSummary[card.source_id] = sourceWarnings;

    for (const warning of card.warnings) {
      incrementCounter(warningCounts, warning.code);
      incrementCounter(sourceWarnings.warning_counts, warning.code);
    }

    if (card.metadata?.attempted_empty_topic_labels === true) {
      emptyTopicLabelAttemptCount += 1;
    }

    if (card.metadata?.attempted_empty_candidate_keywords === true) {
      emptyCandidateKeywordAttemptCount += 1;
    }
  }

  for (const failure of failures) {
    incrementCounter(failureCountsByType, failure.reason);
  }

  return {
    run_timestamp: runTimestamp,
    total_semantic_cards_produced: cards.length,
    total_semantic_failures: failures.length,
    confidence_score_distribution: buildConfidenceDistribution(cards),
    warning_counts_by_type: warningCounts,
    event_type_distribution: eventTypeDistribution,
    topic_label_distribution: topicLabelDistribution,
    strategic_dimension_distribution: strategicDimensionDistribution,
    language_distribution: languageDistribution,
    empty_topic_label_attempt_count: emptyTopicLabelAttemptCount,
    empty_topic_label_validation_failure_count: failures.filter((failure) => /"topic_labels" must not be empty/.test(failure.message)).length,
    empty_candidate_keyword_attempt_count: emptyCandidateKeywordAttemptCount,
    empty_candidate_keyword_validation_failure_count: failures.filter((failure) => /"candidate_keywords" must not be empty/.test(failure.message)).length,
    geography_ambiguity_warning_count: warningCounts.geography_ambiguity ?? 0,
    entity_extraction_uncertainty_warning_count: warningCounts.entity_extraction_uncertain ?? 0,
    overlap_warning_count: warningCounts.summary_why_overlap ?? 0,
    summary_why_overlap_warning_count: warningCounts.summary_why_overlap ?? 0,
    low_confidence_count: warningCounts.low_confidence_extraction ?? 0,
    low_confidence_semantic_card_count: warningCounts.low_confidence_extraction ?? 0,
    low_confidence_threshold: rules.confidence_rules.low_confidence_threshold,
    failure_counts_by_type: failureCountsByType,
    per_source_semantic_warning_summaries: perSourceWarningSummary,
    failures
  };
}

function writeSemanticDiagnostics(outputDir, cards, diagnostics) {
  mkdirSync(outputDir, { recursive: true });
  writeFileSync(resolve(outputDir, 'semantic_cards.json'), JSON.stringify(cards, null, 2));
  writeFileSync(resolve(outputDir, 'semantic_diagnostics.json'), JSON.stringify(diagnostics, null, 2));
}

export function buildSemanticCards({
  canonicalRecords,
  outputDir = null,
  runTimestamp = new Date().toISOString(),
  taxonomyPath = DEFAULT_SEMANTIC_TAXONOMY_PATH,
  rulesPath = DEFAULT_SEMANTIC_RULES_PATH
}) {
  const taxonomy = loadSemanticTaxonomy(taxonomyPath);
  const rules = loadSemanticRules(rulesPath);
  const cards = [];
  const failures = [];

  for (const record of canonicalRecords) {
    if (record?.candidate_disposition && record.candidate_disposition !== 'main') {
      failures.push({
        article_id: record.article_id ?? 'unknown',
        source_id: record.source_id ?? 'unknown',
        reason: 'non_main_pool_record',
        message: 'Semantic extraction only accepts Phase 1 mainPool records.'
      });
      continue;
    }

    try {
      cards.push(extractSemanticCard({ ...record }, taxonomy, rules));
    } catch (error) {
      failures.push({
        article_id: record.article_id ?? 'unknown',
        source_id: record.source_id ?? 'unknown',
        reason: 'semantic_validation_failure',
        message: error.message
      });
    }
  }

  const diagnostics = buildSemanticDiagnostics({
    runTimestamp,
    cards,
    failures,
    rules
  });

  if (outputDir) {
    writeSemanticDiagnostics(outputDir, cards, diagnostics);
  }

  return {
    runTimestamp,
    cards,
    failures,
    diagnostics
  };
}
