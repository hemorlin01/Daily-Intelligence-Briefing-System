import { buildTokenFingerprint, diceCoefficient, jaccardSimilarity } from '../utils/text.js';

function compareCandidates(left, right) {
  if (left.extraction_quality_score !== right.extraction_quality_score) {
    return right.extraction_quality_score - left.extraction_quality_score;
  }

  if (left.candidate_disposition !== right.candidate_disposition) {
    return left.candidate_disposition === 'main' ? -1 : 1;
  }

  if (left.source_priority_tier !== right.source_priority_tier) {
    return left.source_priority_tier - right.source_priority_tier;
  }

  if (left.publication_identity_score !== right.publication_identity_score) {
    return right.publication_identity_score - left.publication_identity_score;
  }

  return left.article_id.localeCompare(right.article_id);
}

function duplicateKind(left, right, rules) {
  if (left.url && right.url && left.url === right.url) {
    return 'exact_url';
  }

  if (left.canonical_url && right.canonical_url && left.canonical_url === right.canonical_url) {
    return 'canonical_url';
  }

  const titleSimilarity = diceCoefficient(left.title ?? '', right.title ?? '');
  if (titleSimilarity >= rules.deduplication.near_duplicate_title_threshold) {
    return 'near_duplicate_title';
  }

  const leftFingerprint = left.content_fingerprint ?? buildTokenFingerprint(left.canonical_text ?? left.raw_snippet ?? '');
  const rightFingerprint = right.content_fingerprint ?? buildTokenFingerprint(right.canonical_text ?? right.raw_snippet ?? '');
  const contentSimilarity = jaccardSimilarity(leftFingerprint, rightFingerprint);

  if (
    titleSimilarity >= rules.deduplication.syndicated_title_threshold
    && contentSimilarity >= rules.deduplication.syndicated_content_threshold
  ) {
    return 'syndicated_duplicate';
  }

  return null;
}

export function deduplicateCandidates(candidates, rules) {
  const survivors = [];
  const actions = [];
  const sortedCandidates = [...candidates].sort((left, right) => left.article_id.localeCompare(right.article_id));

  for (const candidate of sortedCandidates) {
    const matches = [];
    for (const survivor of survivors) {
      const reason = duplicateKind(candidate, survivor, rules);
      if (reason) {
        matches.push({ survivor, reason });
      }
    }

    if (matches.length === 0) {
      survivors.push(candidate);
      continue;
    }

    const contenders = [candidate, ...matches.map((match) => match.survivor)];
    const winner = [...contenders].sort(compareCandidates)[0];

    for (const contender of contenders) {
      if (contender === winner) {
        continue;
      }

      const reason = duplicateKind(winner, contender, rules) ?? 'duplicate';
      actions.push({
        action: 'drop_duplicate',
        reason,
        kept_article_id: winner.article_id,
        dropped_article_id: contender.article_id,
        kept_source_id: winner.source_id,
        dropped_source_id: contender.source_id,
        comparator: {
          kept_extraction_quality_score: winner.extraction_quality_score,
          dropped_extraction_quality_score: contender.extraction_quality_score,
          kept_priority_tier: winner.source_priority_tier,
          dropped_priority_tier: contender.source_priority_tier,
          kept_publication_identity_score: winner.publication_identity_score,
          dropped_publication_identity_score: contender.publication_identity_score
        }
      });
    }

    const matchedIds = new Set(matches.map((match) => match.survivor.article_id));
    for (let index = survivors.length - 1; index >= 0; index -= 1) {
      if (matchedIds.has(survivors[index].article_id)) {
        survivors.splice(index, 1);
      }
    }

    survivors.push(winner);
  }

  survivors.sort((left, right) => left.article_id.localeCompare(right.article_id));
  return {
    candidates: survivors,
    actions
  };
}
