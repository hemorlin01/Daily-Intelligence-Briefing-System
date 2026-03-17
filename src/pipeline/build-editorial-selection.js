import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { loadEditorialRules, loadSemanticTaxonomy, loadSourceCatalog } from '../config/load-config.js';
import { createEditorialSelectionResult, createSelectedItem } from '../models/editorial-selection.js';
import { validateSemanticCard } from '../models/semantic-card.js';
import { clusterSemanticCards } from './cluster-semantic-cards.js';
import { buildTokenFingerprint, diceCoefficient, jaccardSimilarity } from '../utils/text.js';

const DEFAULT_EDITORIAL_RULES_PATH = resolve(process.cwd(), 'config', 'editorial-rules.json');
const DEFAULT_SEMANTIC_TAXONOMY_PATH = resolve(process.cwd(), 'config', 'semantic-taxonomy.json');
const DEFAULT_SOURCES_PATH = resolve(process.cwd(), 'config', 'sources.json');

function clamp(value, minimum = 0, maximum = 1) {
  return Math.max(minimum, Math.min(maximum, value));
}

function round(value) {
  return Number(value.toFixed(4));
}

function incrementCounter(target, key) {
  target[key] = (target[key] ?? 0) + 1;
}

function toTimestamp(value) {
  const timestamp = Date.parse(value ?? '');
  return Number.isNaN(timestamp) ? null : timestamp;
}

function getPublicationTimestamp(card) {
  return toTimestamp(card.metadata?.publication_time_utc);
}

function getAgeHours(card, runTimestamp) {
  const publicationTimestamp = getPublicationTimestamp(card);
  if (!publicationTimestamp) {
    return 999;
  }
  return Math.max(0, (Date.parse(runTimestamp) - publicationTimestamp) / 36e5);
}

function getSourcePriorityTier(card) {
  return Number(card.metadata?.source_priority_tier ?? 3);
}

function getSourceClass(card) {
  return card.metadata?.source_class ?? 'unknown';
}

function getSourceDisplayName(card, sourceCatalog) {
  return sourceCatalog.sources.get(card.source_id)?.display_name
    ?? card.metadata?.source_display_name
    ?? card.source_id;
}

function getAuthorByline(card) {
  return card.metadata?.canonical_author ?? null;
}

function getExtractionQuality(card) {
  return clamp(Number(card.metadata?.extraction_quality_score ?? 0));
}

function getArticleType(card) {
  return card.metadata?.article_type ?? null;
}

function isLowConfidence(card, rules) {
  return card.confidence_score < Math.max(rules.selection.minimum_candidate_score - 0.05, 0.4);
}

function buildDomainCandidateCounts(cards, rules) {
  const counts = Object.fromEntries(Object.keys(rules.domain_quotas).map((domain) => [domain, 0]));
  for (const card of cards) {
    for (const domain of card.topic_labels ?? []) {
      if (domain in counts) {
        counts[domain] += 1;
      }
    }
  }
  return counts;
}

function buildDomainNeedMap(cards, rules) {
  const availabilityCounts = buildDomainCandidateCounts(cards, rules);
  const maxPriority = Math.max(...Object.values(rules.domain_quotas).map((quota) => quota.priority));
  const needByDomain = {};

  for (const [domain, quota] of Object.entries(rules.domain_quotas)) {
    const available = availabilityCounts[domain];
    if (available === 0) {
      needByDomain[domain] = 0;
      continue;
    }

    const scarcity = clamp(quota.target_count / available);
    const softMinBoost = quota.soft_min_count > 0 && available <= quota.target_count ? 0.12 : 0;
    const priorityBoost = (quota.priority / maxPriority) * 0.12;
    needByDomain[domain] = round(clamp(0.24 + (scarcity * 0.52) + softMinBoost + priorityBoost));
  }

  return {
    availability_counts: availabilityCounts,
    need_by_domain: needByDomain
  };
}

function compareDomains(left, right, rules) {
  const leftPriority = rules.domain_quotas[left]?.priority ?? 0;
  const rightPriority = rules.domain_quotas[right]?.priority ?? 0;
  if (leftPriority !== rightPriority) {
    return rightPriority - leftPriority;
  }
  return rules.domain_priority_order.indexOf(left) - rules.domain_priority_order.indexOf(right);
}

function getEligibleDomains(card, rules) {
  return (card.topic_labels ?? [])
    .filter((label) => label in rules.domain_quotas)
    .sort((left, right) => compareDomains(left, right, rules));
}

function getPreferredPrimaryDomain(card, domainNeedMap, rules) {
  const eligibleDomains = getEligibleDomains(card, rules);
  if (eligibleDomains.length === 0) {
    return null;
  }

  return [...eligibleDomains].sort((left, right) => {
    const leftNeed = domainNeedMap.need_by_domain[left] ?? 0;
    const rightNeed = domainNeedMap.need_by_domain[right] ?? 0;
    if (leftNeed !== rightNeed) {
      return rightNeed - leftNeed;
    }
    return compareDomains(left, right, rules);
  })[0];
}

function computeFreshnessScore(card, runTimestamp, rules) {
  const ageHours = getAgeHours(card, runTimestamp);
  const windows = Object.entries(rules.scoring.freshness_windows_hours)
    .map(([hours, score]) => ({ hours: Number(hours), score }))
    .sort((left, right) => left.hours - right.hours);

  for (const window of windows) {
    if (ageHours <= window.hours) {
      return round(window.score);
    }
  }

  return round(windows[windows.length - 1]?.score ?? 0.2);
}

function computeSourcePriorityScore(card, rules) {
  const tier = String(getSourcePriorityTier(card));
  return round(clamp(Number(rules.scoring.source_priority_by_tier[tier] ?? 0.48)));
}

function computeUserRelevanceScore(card, rules) {
  return round(clamp(Number(rules.scoring.user_relevance_scores[card.user_relevance_signal] ?? 0.4)));
}

function computeNoveltyScore(card, rules) {
  return round(clamp(Number(rules.scoring.novelty_scores[card.novelty_signal] ?? 0.4)));
}

function computeLongFormBonus(card, rules) {
  return rules.scoring.long_form_bonus_event_types.includes(card.event_type) || rules.scoring.long_form_bonus_article_types.includes(getArticleType(card))
    ? 1
    : 0;
}

function computeClusterUniquenessScore(card, cluster) {
  const representativeBoost = cluster.representative_article_id === card.article_id ? 0.18 : 0;
  const sizePenalty = Math.min(0.55, (cluster.cluster_size - 1) * 0.16);
  return round(clamp(0.92 - sizePenalty + representativeBoost));
}

function computeRedundancyPenalty(card, cluster, scoredCardsById) {
  if (cluster.cluster_size <= 1) {
    return 0;
  }

  const representative = scoredCardsById.get(cluster.representative_article_id)?.semantic_card ?? null;
  const titleSimilarity = representative ? diceCoefficient(card.title, representative.title) : 0;
  const keywordOverlap = representative
    ? jaccardSimilarity(card.candidate_keywords ?? [], representative.candidate_keywords ?? [])
    : 0;
  let penalty = Math.min(0.68, (cluster.cluster_size - 1) * 0.16);
  if (cluster.representative_article_id !== card.article_id) {
    penalty += titleSimilarity * 0.18;
    penalty += keywordOverlap * 0.12;
  }
  if (card.novelty_signal === 'repeated_coverage') {
    penalty += 0.12;
  }
  return round(clamp(penalty));
}

function compareScoredCandidates(left, right) {
  if (left.final_composite_score !== right.final_composite_score) {
    return right.final_composite_score - left.final_composite_score;
  }
  if (left.score_breakdown.semantic_confidence_score !== right.score_breakdown.semantic_confidence_score) {
    return right.score_breakdown.semantic_confidence_score - left.score_breakdown.semantic_confidence_score;
  }
  return left.article_id.localeCompare(right.article_id);
}

function scoreSemanticCards(cards, clusters, runTimestamp, rules, sourceCatalog) {
  const domainNeedMap = buildDomainNeedMap(cards, rules);
  const clusterByArticleId = new Map();
  for (const cluster of clusters) {
    for (const articleId of cluster.member_article_ids) {
      clusterByArticleId.set(articleId, cluster);
    }
  }

  const scoredCandidates = [];
  const scoredByArticleId = new Map();

  for (const card of cards) {
    const cluster = clusterByArticleId.get(card.article_id);
    const eligibleDomains = getEligibleDomains(card, rules);
    const preferredPrimaryDomain = getPreferredPrimaryDomain(card, domainNeedMap, rules);
    const scoreBreakdown = {
      source_priority_score: computeSourcePriorityScore(card, rules),
      freshness_score: computeFreshnessScore(card, runTimestamp, rules),
      extraction_quality_score: round(getExtractionQuality(card)),
      semantic_confidence_score: round(clamp(card.confidence_score)),
      user_relevance_score: computeUserRelevanceScore(card, rules),
      novelty_score: computeNoveltyScore(card, rules),
      domain_need_score: round(domainNeedMap.need_by_domain[preferredPrimaryDomain] ?? 0),
      cluster_uniqueness_score: computeClusterUniquenessScore(card, cluster),
      long_form_bonus: computeLongFormBonus(card, rules),
      redundancy_penalty: 0
    };

    const scoredCandidate = {
      article_id: card.article_id,
      cluster_id: cluster.cluster_id,
      source_id: card.source_id,
      source_display_name: getSourceDisplayName(card, sourceCatalog),
      author_byline: getAuthorByline(card),
      source_class: getSourceClass(card),
      title: card.title,
      url: card.url,
      language: card.language,
      event_type: card.event_type,
      topic_labels: [...card.topic_labels],
      strategic_dimensions: [...card.strategic_dimensions],
      primary_entities: [...card.primary_entities],
      geography_primary: card.geography_primary,
      candidate_keywords: [...card.candidate_keywords],
      factual_summary: card.factual_summary,
      why_it_matters: card.why_it_matters,
      novelty_signal: card.novelty_signal,
      warnings: [...card.warnings],
      eligible_domains: eligibleDomains,
      preferred_primary_domain: preferredPrimaryDomain,
      is_cluster_representative: cluster.representative_article_id === card.article_id,
      cluster_size: cluster.cluster_size,
      score_breakdown: {
        ...scoreBreakdown,
        weights: rules.scoring.weights,
        age_hours: round(getAgeHours(card, runTimestamp))
      },
      semantic_card: card
    };

    scoredCandidates.push(scoredCandidate);
    scoredByArticleId.set(card.article_id, scoredCandidate);
  }

  for (const candidate of scoredCandidates) {
    const cluster = clusterByArticleId.get(candidate.article_id);
    candidate.score_breakdown.redundancy_penalty = computeRedundancyPenalty(candidate.semantic_card, cluster, scoredByArticleId);
    const weights = rules.scoring.weights;
    const positiveTotal = (
      candidate.score_breakdown.source_priority_score * weights.source_priority_score
      + candidate.score_breakdown.freshness_score * weights.freshness_score
      + candidate.score_breakdown.extraction_quality_score * weights.extraction_quality_score
      + candidate.score_breakdown.semantic_confidence_score * weights.semantic_confidence_score
      + candidate.score_breakdown.user_relevance_score * weights.user_relevance_score
      + candidate.score_breakdown.novelty_score * weights.novelty_score
      + candidate.score_breakdown.domain_need_score * weights.domain_need_score
      + candidate.score_breakdown.cluster_uniqueness_score * weights.cluster_uniqueness_score
      + candidate.score_breakdown.long_form_bonus * weights.long_form_bonus
    );
    candidate.final_composite_score = round(clamp(positiveTotal - (candidate.score_breakdown.redundancy_penalty * weights.redundancy_penalty)));
  }

  return {
    domainNeedMap,
    scored_candidates: scoredCandidates.sort(compareScoredCandidates)
  };
}

function buildSelectedCounts(selectedItems) {
  const counts = {
    domains: {},
    sources: {},
    clusters: {},
    source_classes: {},
    strategic_dimensions: {}
  };

  for (const item of selectedItems) {
    incrementCounter(counts.domains, item.primary_domain);
    incrementCounter(counts.sources, item.source_id);
    incrementCounter(counts.clusters, item.cluster_id);
    incrementCounter(counts.source_classes, item.source_class);
    for (const dimension of item.strategic_dimensions ?? []) {
      incrementCounter(counts.strategic_dimensions, dimension);
    }
  }

  return counts;
}

function hasMateriallyDifferentAngle(candidate, selectedItemsInCluster, rules) {
  if (selectedItemsInCluster.length === 0) {
    return true;
  }

  return selectedItemsInCluster.every((selectedItem) => {
    const titleSimilarity = diceCoefficient(candidate.title, selectedItem.title);
    const keywordOverlap = jaccardSimilarity(candidate.candidate_keywords, selectedItem.candidate_keywords);
    const summaryOverlap = diceCoefficient(candidate.factual_summary, selectedItem.factual_summary);
    const strategicDifference = jaccardSimilarity(candidate.strategic_dimensions, selectedItem.strategic_dimensions) < 0.7;
    const domainDifference = candidate.preferred_primary_domain !== selectedItem.primary_domain;
    const noveltyDifference = candidate.novelty_signal !== selectedItem.novelty_signal;

    return (
      titleSimilarity < rules.clustering.angle_diversity.title_similarity_max
      && summaryOverlap < rules.clustering.angle_diversity.summary_overlap_max
      && (
        keywordOverlap < rules.clustering.angle_diversity.keyword_overlap_max
        || strategicDifference
        || domainDifference
        || noveltyDifference
      )
    );
  });
}

function buildCandidateLookup(scoredCandidates) {
  const lookup = new Map();
  for (const candidate of scoredCandidates) {
    lookup.set(candidate.article_id, candidate);
  }
  return lookup;
}

function initializeExclusionState(scoredCandidates, rules) {
  const reasons = new Map();
  for (const candidate of scoredCandidates) {
    reasons.set(candidate.article_id, new Set());
    if (!candidate.is_cluster_representative) {
      reasons.get(candidate.article_id).add('not_cluster_representative');
    }
    if (candidate.final_composite_score < rules.selection.minimum_candidate_score) {
      reasons.get(candidate.article_id).add('low_score');
    }
    if (candidate.semantic_card?.metadata?.candidate_disposition && candidate.semantic_card.metadata.candidate_disposition !== 'main') {
      reasons.get(candidate.article_id).add('non_main_pool_record');
    }
    if (candidate.semantic_card.confidence_score < Math.max(rules.selection.minimum_candidate_score - 0.05, 0.4)) {
      reasons.get(candidate.article_id).add('low_confidence');
    }
  }
  return reasons;
}

function canUseDomainForBackfill(domain, selectedCounts, rules) {
  if (domain !== 'geopolitics') {
    return true;
  }
  const quota = rules.domain_quotas.geopolitics;
  return (selectedCounts.domains.geopolitics ?? 0) < quota.target_count && (selectedCounts.domains.geopolitics ?? 0) < quota.hard_max_count;
}

function selectCandidate({
  candidate,
  assignedDomain,
  selectedItems,
  selectedIds,
  exclusionReasons,
  rules,
  knownClusterIds,
  selectionReasonCodes
}) {
  const selectedCounts = buildSelectedCounts(selectedItems);
  const sourceCount = selectedCounts.sources[candidate.source_id] ?? 0;
  const clusterCount = selectedCounts.clusters[candidate.cluster_id] ?? 0;
  const domainCount = selectedCounts.domains[assignedDomain] ?? 0;

  if (candidate.semantic_card.metadata?.candidate_disposition && candidate.semantic_card.metadata.candidate_disposition !== 'main') {
    exclusionReasons.get(candidate.article_id).add('non_main_pool_record');
    return false;
  }

  if (selectedIds.has(candidate.article_id)) {
    return false;
  }

  if (candidate.final_composite_score < rules.selection.minimum_candidate_score) {
    exclusionReasons.get(candidate.article_id).add('low_score');
    return false;
  }

  if (sourceCount >= rules.caps.source_max_count) {
    exclusionReasons.get(candidate.article_id).add('source_cap_reached');
    return false;
  }

  if (domainCount >= rules.domain_quotas[assignedDomain].hard_max_count) {
    exclusionReasons.get(candidate.article_id).add('domain_cap_reached');
    return false;
  }

  if (clusterCount >= rules.caps.cluster_max_count) {
    exclusionReasons.get(candidate.article_id).add('cluster_cap_reached');
    return false;
  }

  if (clusterCount >= rules.caps.preferred_cluster_count) {
    const selectedItemsInCluster = selectedItems.filter((item) => item.cluster_id === candidate.cluster_id);
    if (!hasMateriallyDifferentAngle(candidate, selectedItemsInCluster, rules)) {
      exclusionReasons.get(candidate.article_id).add('duplicate_angle_within_cluster');
      return false;
    }
  }

  const selectedItem = createSelectedItem({
    article_id: candidate.article_id,
    cluster_id: candidate.cluster_id,
    source_id: candidate.source_id,
    source_display_name: candidate.source_display_name,
    author_byline: candidate.author_byline,
    source_class: candidate.source_class,
    title: candidate.title,
    url: candidate.url,
    candidate_keywords: candidate.candidate_keywords,
    factual_summary: candidate.factual_summary,
    why_it_matters: candidate.why_it_matters,
    topic_labels: candidate.topic_labels,
    strategic_dimensions: candidate.strategic_dimensions,
    primary_domain: assignedDomain,
    score_breakdown: candidate.score_breakdown,
    final_composite_score: candidate.final_composite_score,
    selection_reason_codes: selectionReasonCodes,
    warnings: candidate.warnings
  }, rules, knownClusterIds);

  selectedItems.push(selectedItem);
  selectedIds.add(candidate.article_id);
  exclusionReasons.delete(candidate.article_id);
  return true;
}

function attemptDomainFill({
  domain,
  candidates,
  rules,
  selectedItems,
  selectedIds,
  exclusionReasons,
  knownClusterIds
}) {
  const selections = [];
  const targetCount = rules.domain_quotas[domain].target_count;

  while ((buildSelectedCounts(selectedItems).domains[domain] ?? 0) < targetCount && selectedItems.length < rules.selection.default_target_count) {
    const nextCandidate = candidates.find((candidate) => !selectedIds.has(candidate.article_id) && candidate.eligible_domains.includes(domain));
    if (!nextCandidate) {
      break;
    }

    const reasonCodes = ['domain_target_fill', 'high_editorial_score'];
    if (nextCandidate.is_cluster_representative) {
      reasonCodes.push('cluster_representative');
    }
    if ((buildSelectedCounts(selectedItems).domains[domain] ?? 0) < rules.domain_quotas[domain].soft_min_count) {
      reasonCodes.push('soft_min_fill');
    }

    const selected = selectCandidate({
      candidate: nextCandidate,
      assignedDomain: domain,
      selectedItems,
      selectedIds,
      exclusionReasons,
      rules,
      knownClusterIds,
      selectionReasonCodes: reasonCodes
    });

    if (!selected) {
      candidates = candidates.filter((candidate) => candidate.article_id !== nextCandidate.article_id);
      continue;
    }

    selections.push(nextCandidate.article_id);
  }

  return selections;
}

function findBackfillCandidate({
  requestedDomain,
  candidatePool,
  selectedItems,
  selectedIds,
  rules,
  mode
}) {
  const selectedCounts = buildSelectedCounts(selectedItems);
  const prioritizedDomains = mode === 'adjacent'
    ? rules.domain_quotas[requestedDomain].adjacent_domains
    : [];

  const strategicCounts = selectedCounts.strategic_dimensions;

  return candidatePool.find((candidate) => {
    if (selectedIds.has(candidate.article_id)) {
      return false;
    }

    if (candidate.final_composite_score < rules.selection.backfill_minimum_candidate_score) {
      return false;
    }

    if (mode === 'adjacent') {
      const domain = prioritizedDomains.find((adjacentDomain) => candidate.eligible_domains.includes(adjacentDomain));
      if (!domain || !canUseDomainForBackfill(domain, selectedCounts, rules)) {
        return false;
      }
      candidate._backfill_domain = domain;
      return true;
    }

    for (const dimension of rules.strategic_backfill_priority) {
      if (!candidate.strategic_dimensions.includes(dimension)) {
        continue;
      }
      if ((strategicCounts[dimension] ?? 0) > 0) {
        continue;
      }

      const availableDomain = candidate.eligible_domains.find((domain) => canUseDomainForBackfill(domain, selectedCounts, rules));
      if (!availableDomain) {
        return false;
      }

      candidate._backfill_domain = availableDomain;
      candidate._backfill_dimension = dimension;
      return true;
    }

    return false;
  }) ?? null;
}

function buildQuotaFillReport(selectedItems, rules) {
  const selectedCounts = buildSelectedCounts(selectedItems);
  const perDomain = {};
  const underfilledDomains = [];

  for (const [domain, quota] of Object.entries(rules.domain_quotas)) {
    const selectedCount = selectedCounts.domains[domain] ?? 0;
    const entry = {
      target_count: quota.target_count,
      hard_max_count: quota.hard_max_count,
      soft_min_count: quota.soft_min_count,
      selected_count: selectedCount,
      target_filled: selectedCount >= quota.target_count,
      soft_min_filled: selectedCount >= quota.soft_min_count,
      hard_cap_respected: selectedCount <= quota.hard_max_count
    };
    if (selectedCount < quota.target_count) {
      underfilledDomains.push(domain);
    }
    perDomain[domain] = entry;
  }

  return {
    target_count: rules.selection.default_target_count,
    minimum_target_count: rules.selection.minimum_target_count,
    selected_count: selectedItems.length,
    per_domain: perDomain,
    underfilled_domains: underfilledDomains
  };
}

function buildScoreComponentCoverage(scoredCandidates) {
  const coverage = {};
  for (const field of [
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
  ]) {
    coverage[field] = scoredCandidates.every((candidate) => typeof candidate.score_breakdown[field] === 'number');
  }
  return coverage;
}

function writeEditorialDiagnostics(outputDir, payloads) {
  mkdirSync(outputDir, { recursive: true });
  writeFileSync(resolve(outputDir, 'cluster_map.json'), JSON.stringify(payloads.clusterMap, null, 2));
  writeFileSync(resolve(outputDir, 'scored_candidates.json'), JSON.stringify(payloads.scoredCandidates, null, 2));
  writeFileSync(resolve(outputDir, 'editorial_selection_debug.json'), JSON.stringify(payloads.editorialDebug, null, 2));
  writeFileSync(resolve(outputDir, 'topic_distribution.json'), JSON.stringify(payloads.topicDistribution, null, 2));
}

function deriveRunStatus(selectedCount, rules) {
  if (selectedCount < rules.selection.minimum_target_count) {
    return {
      degraded_mode: true,
      under_default_target: false,
      run_status: 'degraded'
    };
  }

  if (selectedCount < rules.selection.default_target_count) {
    return {
      degraded_mode: false,
      under_default_target: true,
      run_status: 'under_default_target'
    };
  }

  return {
    degraded_mode: false,
    under_default_target: false,
    run_status: 'on_target'
  };
}

export function buildEditorialSelection({
  semanticCards,
  outputDir = null,
  runTimestamp = new Date().toISOString(),
  editorialRulesPath = DEFAULT_EDITORIAL_RULES_PATH,
  taxonomyPath = DEFAULT_SEMANTIC_TAXONOMY_PATH,
  sourcesPath = DEFAULT_SOURCES_PATH
}) {
  const rules = loadEditorialRules(editorialRulesPath);
  const taxonomy = loadSemanticTaxonomy(taxonomyPath);
  const sourceCatalog = loadSourceCatalog(sourcesPath);

  for (const card of semanticCards) {
    validateSemanticCard(card, taxonomy);
    if (card.metadata?.candidate_disposition && card.metadata.candidate_disposition !== 'main') {
      throw new Error(`Editorial selection only accepts semantic cards derived from Phase 1 mainPool records: ${card.article_id}`);
    }
  }

  const { clusters, card_cluster_index: clusterIndex } = clusterSemanticCards(semanticCards, rules);
  const { domainNeedMap, scored_candidates: scoredCandidates } = scoreSemanticCards(semanticCards, clusters, runTimestamp, rules, sourceCatalog);
  const knownClusterIds = new Set(clusters.map((cluster) => cluster.cluster_id));
  const candidateLookup = buildCandidateLookup(scoredCandidates);
  const exclusionReasons = initializeExclusionState(scoredCandidates, rules);
  const selectedItems = [];
  const selectedIds = new Set();
  const backfillActions = [];

  const representativeCandidates = scoredCandidates.filter((candidate) => candidate.is_cluster_representative);
  const representativePool = [...representativeCandidates];
  for (const domain of rules.domain_priority_order) {
    attemptDomainFill({
      domain,
      candidates: representativePool,
      rules,
      selectedItems,
      selectedIds,
      exclusionReasons,
      knownClusterIds
    });
  }

  const quotaReportAfterFirstPass = buildQuotaFillReport(selectedItems, rules);
  for (const domain of quotaReportAfterFirstPass.underfilled_domains) {
    if (selectedItems.length >= rules.selection.default_target_count) {
      break;
    }

    const adjacencyCandidate = findBackfillCandidate({
      requestedDomain: domain,
      candidatePool: scoredCandidates,
      selectedItems,
      selectedIds,
      rules,
      mode: 'adjacent'
    });

    if (adjacencyCandidate) {
      const assignedDomain = adjacencyCandidate._backfill_domain;
      const selected = selectCandidate({
        candidate: adjacencyCandidate,
        assignedDomain,
        selectedItems,
        selectedIds,
        exclusionReasons,
        rules,
        knownClusterIds,
        selectionReasonCodes: ['adjacent_backfill', `requested_domain:${domain}`, 'high_editorial_score']
      });

      if (selected) {
        backfillActions.push({
          strategy: 'adjacent_domain',
          requested_domain: domain,
          reassigned_domain: assignedDomain,
          article_id: adjacencyCandidate.article_id
        });
        continue;
      }
    }

    const strategicCandidate = findBackfillCandidate({
      requestedDomain: domain,
      candidatePool: scoredCandidates,
      selectedItems,
      selectedIds,
      rules,
      mode: 'strategic'
    });

    if (strategicCandidate) {
      const assignedDomain = strategicCandidate._backfill_domain;
      const selected = selectCandidate({
        candidate: strategicCandidate,
        assignedDomain,
        selectedItems,
        selectedIds,
        exclusionReasons,
        rules,
        knownClusterIds,
        selectionReasonCodes: ['strategic_backfill', `requested_domain:${domain}`, `strategic_dimension:${strategicCandidate._backfill_dimension}`, 'high_editorial_score']
      });

      if (selected) {
        backfillActions.push({
          strategy: 'strategic_dimension',
          requested_domain: domain,
          reassigned_domain: assignedDomain,
          strategic_dimension: strategicCandidate._backfill_dimension,
          article_id: strategicCandidate.article_id
        });
        continue;
      }
    }

    for (const candidate of scoredCandidates) {
      if (selectedIds.has(candidate.article_id)) {
        continue;
      }
      exclusionReasons.get(candidate.article_id).add('failed_backfill_priority');
    }
  }

  const finalSelectedItems = [...selectedItems].sort((left, right) => right.final_composite_score - left.final_composite_score || left.article_id.localeCompare(right.article_id));
  const finalSelectedArticleIds = finalSelectedItems.map((item) => item.article_id);
  const runStatus = deriveRunStatus(finalSelectedItems.length, rules);
  const selectedCounts = buildSelectedCounts(selectedItems);
  const sourceCapViolations = Object.entries(selectedCounts.sources)
    .filter(([, count]) => count > rules.caps.source_max_count)
    .map(([source_id, count]) => ({ source_id, count }));
  const clusterCapViolations = Object.entries(selectedCounts.clusters)
    .filter(([, count]) => count > rules.caps.cluster_max_count)
    .map(([cluster_id, count]) => ({ cluster_id, count }));

  const result = createEditorialSelectionResult({
    run_timestamp: runTimestamp,
    selected_items: finalSelectedItems,
    selected_article_ids: finalSelectedArticleIds,
    selected_count: finalSelectedItems.length,
    degraded_mode: runStatus.degraded_mode,
    under_default_target: runStatus.under_default_target,
    run_status: runStatus.run_status,
    quota_fill_report: buildQuotaFillReport(finalSelectedItems, rules),
    source_cap_report: {
      source_max_count: rules.caps.source_max_count,
      counts: selectedCounts.sources,
      violations: sourceCapViolations
    },
    cluster_cap_report: {
      preferred_cluster_count: rules.caps.preferred_cluster_count,
      cluster_max_count: rules.caps.cluster_max_count,
      counts: selectedCounts.clusters,
      violations: clusterCapViolations
    },
    backfill_actions: backfillActions,
    exclusion_summary: {
      total_excluded: scoredCandidates.length - selectedItems.length,
      reasons: Object.fromEntries(
        Array.from(exclusionReasons.values())
          .flatMap((reasonSet) => [...reasonSet])
          .sort()
          .reduce((entries, reason) => {
            entries.set(reason, (entries.get(reason) ?? 0) + 1);
            return entries;
          }, new Map())
      )
    }
  }, rules, clusters);

  const scoredCandidatesOutput = scoredCandidates.map((candidate) => ({
    article_id: candidate.article_id,
    cluster_id: candidate.cluster_id,
    source_id: candidate.source_id,
    source_display_name: candidate.source_display_name,
    author_byline: candidate.author_byline,
    source_class: candidate.source_class,
    title: candidate.title,
    url: candidate.url,
    topic_labels: candidate.topic_labels,
    eligible_domains: candidate.eligible_domains,
    preferred_primary_domain: candidate.preferred_primary_domain,
    is_cluster_representative: candidate.is_cluster_representative,
    final_composite_score: candidate.final_composite_score,
    score_breakdown: candidate.score_breakdown,
    selected: selectedIds.has(candidate.article_id),
    assigned_primary_domain: result.selected_items.find((item) => item.article_id === candidate.article_id)?.primary_domain ?? null,
    selection_reason_codes: result.selected_items.find((item) => item.article_id === candidate.article_id)?.selection_reason_codes ?? [],
    exclusion_reason_codes: selectedIds.has(candidate.article_id) ? [] : [...(exclusionReasons.get(candidate.article_id) ?? [])].sort(),
    warning_codes: candidate.warnings.map((warning) => warning.code)
  }));

  const perSourceCounts = {};
  const perSourceClassCounts = {};
  const perClusterCounts = {};
  for (const item of result.selected_items) {
    incrementCounter(perSourceCounts, item.source_id);
    incrementCounter(perSourceClassCounts, item.source_class);
    incrementCounter(perClusterCounts, item.cluster_id);
  }

  const dominanceWarnings = [];
  const dominantSourceClass = Object.entries(perSourceClassCounts).sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))[0];
  if (dominantSourceClass && (dominantSourceClass[1] / Math.max(1, result.selected_count)) > rules.caps.source_class_dominance_warning_ratio) {
    dominanceWarnings.push(`source_class_dominance:${dominantSourceClass[0]}`);
  }

  const diagnostics = {
    run_timestamp: runTimestamp,
    total_clusters: clusters.length,
    total_scored_candidates: scoredCandidates.length,
    selected_count: result.selected_count,
    degraded_mode: result.degraded_mode,
    under_default_target: result.under_default_target,
    run_status: result.run_status,
    per_domain_counts: buildSelectedCounts(result.selected_items).domains,
    per_source_counts: perSourceCounts,
    per_source_class_counts: perSourceClassCounts,
    per_cluster_counts: perClusterCounts,
    quota_fill_status: result.quota_fill_report,
    backfill_actions: result.backfill_actions,
    warning_flags: dominanceWarnings,
    exclusion_summary: result.exclusion_summary,
    low_confidence_exclusion_count: scoredCandidatesOutput.filter((candidate) => candidate.exclusion_reason_codes.includes('low_confidence')).length,
    score_component_coverage: buildScoreComponentCoverage(scoredCandidates)
  };

  const clusterMap = {
    run_timestamp: runTimestamp,
    total_clusters: clusters.length,
    singleton_cluster_count: clusters.filter((cluster) => cluster.cluster_size === 1).length,
    multi_member_cluster_count: clusters.filter((cluster) => cluster.cluster_size > 1).length,
    clusters
  };

  const topicDistribution = {
    run_timestamp: runTimestamp,
    candidate_domain_counts: domainNeedMap.availability_counts,
    selected_domain_counts: buildSelectedCounts(result.selected_items).domains,
    quota_targets: Object.fromEntries(Object.entries(rules.domain_quotas).map(([domain, quota]) => [domain, {
      target_count: quota.target_count,
      soft_min_count: quota.soft_min_count,
      hard_max_count: quota.hard_max_count
    }])),
    underfilled_domains: result.quota_fill_report.underfilled_domains,
    backfill_actions: result.backfill_actions
  };

  if (outputDir) {
    writeEditorialDiagnostics(outputDir, {
      clusterMap,
      scoredCandidates: scoredCandidatesOutput,
      editorialDebug: {
        ...diagnostics,
        selected_article_ids: result.selected_article_ids,
        selected_items: result.selected_items
      },
      topicDistribution
    });
  }

  return {
    runTimestamp,
    clusters,
    scoredCandidates: scoredCandidatesOutput,
    result,
    diagnostics,
    clusterIndex,
    candidateLookup
  };
}
