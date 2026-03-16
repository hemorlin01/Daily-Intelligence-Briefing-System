import { buildTokenFingerprint, diceCoefficient, jaccardSimilarity, normalizeTitleForComparison } from '../utils/text.js';

function clamp(value, minimum = 0, maximum = 1) {
  return Math.max(minimum, Math.min(maximum, value));
}

function round(value) {
  return Number(value.toFixed(4));
}

function slugify(value) {
  return normalizeTitleForComparison(value).replace(/\s+/g, '-').slice(0, 48) || 'unknown';
}

function toTimestamp(value) {
  const timestamp = Date.parse(value ?? '');
  return Number.isNaN(timestamp) ? null : timestamp;
}

function sortCardsForClustering(cards) {
  return [...cards].sort((left, right) => {
    const leftTime = toTimestamp(left.metadata?.publication_time_utc) ?? 0;
    const rightTime = toTimestamp(right.metadata?.publication_time_utc) ?? 0;
    if (leftTime !== rightTime) {
      return rightTime - leftTime;
    }
    return left.article_id.localeCompare(right.article_id);
  });
}

function normalizeStringSet(values) {
  return Array.from(new Set(
    (values ?? [])
      .filter((value) => typeof value === 'string' && value.trim().length > 0)
      .map((value) => value.trim().toLowerCase())
  ));
}

function collectEntities(card) {
  return normalizeStringSet([
    ...(card.primary_entities ?? []),
    ...(card.secondary_entities ?? [])
  ]);
}

function collectGeographies(card) {
  return normalizeStringSet([
    card.geography_primary,
    ...(card.geography_secondary ?? [])
  ]);
}

function titleKeywordSimilarity(left, right) {
  const titleSimilarity = diceCoefficient(left.title, right.title);
  const keywordSimilarity = jaccardSimilarity(
    buildTokenFingerprint((left.candidate_keywords ?? []).join(' '), 240),
    buildTokenFingerprint((right.candidate_keywords ?? []).join(' '), 240)
  );
  return Math.max(titleSimilarity, keywordSimilarity);
}

function computeTimeProximity(left, right, timeDecayHours) {
  const leftTime = toTimestamp(left.metadata?.publication_time_utc);
  const rightTime = toTimestamp(right.metadata?.publication_time_utc);
  if (!leftTime || !rightTime) {
    return 0.35;
  }

  const differenceHours = Math.abs(leftTime - rightTime) / 36e5;
  return clamp(1 - (differenceHours / timeDecayHours));
}

function computePairSimilarity(left, right, rules) {
  const weights = rules.clustering.weights;
  const leftEntities = collectEntities(left);
  const rightEntities = collectEntities(right);
  const leftGeographies = collectGeographies(left);
  const rightGeographies = collectGeographies(right);

  const entityOverlap = jaccardSimilarity(leftEntities, rightEntities);
  const geographyOverlap = left.geography_primary && right.geography_primary && left.geography_primary === right.geography_primary
    ? 1
    : jaccardSimilarity(leftGeographies, rightGeographies);
  const eventTypeMatch = left.event_type === right.event_type ? 1 : 0;
  const topicOverlap = jaccardSimilarity(left.topic_labels ?? [], right.topic_labels ?? []);
  const strategicOverlap = jaccardSimilarity(left.strategic_dimensions ?? [], right.strategic_dimensions ?? []);
  const keywordTitleScore = titleKeywordSimilarity(left, right);
  const timeProximity = computeTimeProximity(left, right, rules.clustering.time_decay_hours);

  const componentScores = {
    entity_overlap: round(entityOverlap),
    geography_overlap: round(geographyOverlap),
    event_type_match: round(eventTypeMatch),
    topic_overlap: round(topicOverlap),
    strategic_overlap: round(strategicOverlap),
    keyword_title_similarity: round(keywordTitleScore),
    time_proximity: round(timeProximity)
  };

  const weightedScore = round(
    (componentScores.entity_overlap * weights.entity_overlap)
    + (componentScores.geography_overlap * weights.geography_overlap)
    + (componentScores.event_type_match * weights.event_type_match)
    + (componentScores.topic_overlap * weights.topic_overlap)
    + (componentScores.strategic_overlap * weights.strategic_overlap)
    + (componentScores.keyword_title_similarity * weights.keyword_title_similarity)
    + (componentScores.time_proximity * weights.time_proximity)
  );

  const signalCount = [
    componentScores.entity_overlap >= 0.34,
    componentScores.geography_overlap >= 0.34,
    componentScores.event_type_match === 1,
    componentScores.topic_overlap >= 0.34,
    componentScores.strategic_overlap >= 0.34,
    componentScores.keyword_title_similarity >= 0.42,
    componentScores.time_proximity >= 0.4
  ].filter(Boolean).length;

  return {
    component_scores: componentScores,
    weighted_score: weightedScore,
    signal_count: signalCount,
    qualifies: weightedScore >= rules.clustering.similarity_threshold && signalCount >= rules.clustering.min_signal_matches
  };
}

class UnionFind {
  constructor(size) {
    this.parent = Array.from({ length: size }, (_, index) => index);
  }

  find(index) {
    if (this.parent[index] !== index) {
      this.parent[index] = this.find(this.parent[index]);
    }
    return this.parent[index];
  }

  union(left, right) {
    const leftRoot = this.find(left);
    const rightRoot = this.find(right);
    if (leftRoot !== rightRoot) {
      this.parent[rightRoot] = leftRoot;
    }
  }
}

function buildFrequencyMap(values) {
  const counts = new Map();
  for (const value of values) {
    if (!value) {
      continue;
    }
    counts.set(value, (counts.get(value) ?? 0) + 1);
  }
  return counts;
}

function sortFrequencyEntries(entries) {
  return [...entries].sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
}

function computeRepresentativeScore(card) {
  const priorityTier = Number(card.metadata?.source_priority_tier ?? 3);
  const priorityScore = priorityTier === 1 ? 1 : priorityTier === 2 ? 0.72 : 0.48;
  const noveltyScore = {
    new_event: 1,
    new_angle: 0.85,
    follow_on_update: 0.62,
    repeated_coverage: 0.32
  }[card.novelty_signal] ?? 0.5;

  let score = 0;
  score += (card.confidence_score ?? 0) * 0.38;
  score += Math.min(1, Number(card.metadata?.extraction_quality_score ?? 0)) * 0.26;
  score += priorityScore * 0.14;
  score += noveltyScore * 0.12;
  score += card.primary_entities.length > 0 ? 0.05 : 0;
  score += card.geography_primary ? 0.03 : 0;
  score -= (card.warnings?.length ?? 0) * 0.015;

  return round(clamp(score));
}

function pickRepresentative(clusterCards) {
  const ranked = [...clusterCards].sort((left, right) => {
    const leftScore = computeRepresentativeScore(left);
    const rightScore = computeRepresentativeScore(right);
    if (leftScore !== rightScore) {
      return rightScore - leftScore;
    }

    const leftTime = toTimestamp(left.metadata?.publication_time_utc) ?? 0;
    const rightTime = toTimestamp(right.metadata?.publication_time_utc) ?? 0;
    if (leftTime !== rightTime) {
      return rightTime - leftTime;
    }

    return left.article_id.localeCompare(right.article_id);
  });

  const representative = ranked[0];
  return {
    representative,
    score_breakdown: {
      representative_score: computeRepresentativeScore(representative),
      source_priority_tier: Number(representative.metadata?.source_priority_tier ?? 3),
      semantic_confidence_score: representative.confidence_score,
      extraction_quality_score: Number(representative.metadata?.extraction_quality_score ?? 0),
      novelty_signal: representative.novelty_signal,
      warning_count: representative.warnings.length
    }
  };
}

function buildClusterId(clusterCards, dominantEventType) {
  const sortedMemberIds = clusterCards.map((card) => card.article_id).sort((left, right) => left.localeCompare(right));
  return `cluster-${slugify(dominantEventType)}-${slugify(sortedMemberIds[0])}`;
}

function summarizeCluster(clusterCards, links) {
  const entityCounts = buildFrequencyMap(clusterCards.flatMap((card) => card.primary_entities));
  const geographyCounts = buildFrequencyMap(clusterCards.flatMap((card) => [card.geography_primary, ...(card.geography_secondary ?? [])]));
  const eventTypeCounts = buildFrequencyMap(clusterCards.map((card) => card.event_type));
  const topicCounts = buildFrequencyMap(clusterCards.flatMap((card) => card.topic_labels));
  const { representative, score_breakdown } = pickRepresentative(clusterCards);
  const dominantEventType = sortFrequencyEntries(eventTypeCounts.entries())[0]?.[0] ?? representative.event_type;
  const clusterId = buildClusterId(clusterCards, dominantEventType);

  return {
    cluster_id: clusterId,
    member_article_ids: clusterCards.map((card) => card.article_id).sort((left, right) => left.localeCompare(right)),
    representative_article_id: representative.article_id,
    cluster_size: clusterCards.length,
    dominant_entities: sortFrequencyEntries(entityCounts.entries()).slice(0, 3).map(([value]) => value),
    dominant_geography: sortFrequencyEntries(geographyCounts.entries())[0]?.[0] ?? representative.geography_primary ?? null,
    dominant_event_type: dominantEventType,
    dominant_topics: sortFrequencyEntries(topicCounts.entries()).slice(0, 3).map(([value]) => value),
    representative_selection: {
      article_id: representative.article_id,
      score_breakdown
    },
    member_details: clusterCards
      .map((card) => ({
        article_id: card.article_id,
        source_id: card.source_id,
        title: card.title,
        confidence_score: card.confidence_score,
        event_type: card.event_type,
        topic_labels: card.topic_labels,
        warnings: card.warnings.map((warning) => warning.code)
      }))
      .sort((left, right) => left.article_id.localeCompare(right.article_id)),
    similarity_links: links.sort((left, right) => left.left_article_id.localeCompare(right.left_article_id) || left.right_article_id.localeCompare(right.right_article_id))
  };
}

export function clusterSemanticCards(cards, rules) {
  const orderedCards = sortCardsForClustering(cards);
  const unionFind = new UnionFind(orderedCards.length);
  const linksByPairKey = new Map();

  for (let leftIndex = 0; leftIndex < orderedCards.length; leftIndex += 1) {
    for (let rightIndex = leftIndex + 1; rightIndex < orderedCards.length; rightIndex += 1) {
      const comparison = computePairSimilarity(orderedCards[leftIndex], orderedCards[rightIndex], rules);
      if (!comparison.qualifies) {
        continue;
      }

      unionFind.union(leftIndex, rightIndex);
      const pairKey = `${orderedCards[leftIndex].article_id}::${orderedCards[rightIndex].article_id}`;
      linksByPairKey.set(pairKey, {
        left_article_id: orderedCards[leftIndex].article_id,
        right_article_id: orderedCards[rightIndex].article_id,
        similarity: comparison
      });
    }
  }

  const groups = new Map();
  for (let index = 0; index < orderedCards.length; index += 1) {
    const root = unionFind.find(index);
    const group = groups.get(root) ?? [];
    group.push(orderedCards[index]);
    groups.set(root, group);
  }

  const clusters = [];
  const cardClusterIndex = new Map();

  for (const clusterCards of [...groups.values()].sort((left, right) => left[0].article_id.localeCompare(right[0].article_id))) {
    const memberIdSet = new Set(clusterCards.map((card) => card.article_id));
    const links = [...linksByPairKey.values()].filter((link) => memberIdSet.has(link.left_article_id) && memberIdSet.has(link.right_article_id));
    const cluster = summarizeCluster(clusterCards, links);
    clusters.push(cluster);
    for (const articleId of cluster.member_article_ids) {
      cardClusterIndex.set(articleId, cluster);
    }
  }

  return {
    clusters: clusters.sort((left, right) => left.cluster_id.localeCompare(right.cluster_id)),
    card_cluster_index: cardClusterIndex
  };
}
