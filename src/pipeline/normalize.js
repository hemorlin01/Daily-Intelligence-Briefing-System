import { createHash } from 'node:crypto';
import { createCanonicalArticleRecord } from '../models/canonical-article.js';
import { RAW_ITEM_FIELD_ALIASES } from '../models/raw-item-contract.js';
import { buildTokenFingerprint, hasValue } from '../utils/text.js';
import { canonicalizeUrl, isValidUrl } from '../utils/url.js';

function resolveFirstString(rawItem, fields) {
  for (const field of fields) {
    const value = rawItem?.[field];
    if (typeof value === 'string' && value.trim().length > 0) {
      return value;
    }
  }
  return null;
}

function resolveFirstDefined(rawItem, fields) {
  for (const field of fields) {
    if (field in (rawItem ?? {})) {
      const value = rawItem[field];
      if (value !== null && value !== undefined) {
        return value;
      }
    }
  }
  return null;
}

function resolvePublicationTimes(rawItem) {
  const rawUtcCandidate = resolveFirstDefined(rawItem, RAW_ITEM_FIELD_ALIASES.publicationTimeUtc);
  const rawLocalCandidate = resolveFirstDefined(rawItem, RAW_ITEM_FIELD_ALIASES.publicationTimeLocal) ?? rawUtcCandidate ?? null;

  const parseCandidate = (value) => {
    if (typeof value === 'number') {
      const asDate = new Date(value);
      return Number.isNaN(asDate.getTime()) ? null : asDate;
    }

    if (typeof value === 'string' && value.trim().length > 0) {
      const asDate = new Date(value);
      return Number.isNaN(asDate.getTime()) ? null : asDate;
    }

    return null;
  };

  const utcDate = parseCandidate(rawUtcCandidate);
  const localDate = parseCandidate(rawLocalCandidate);

  return {
    publicationTimeUtc: utcDate ? utcDate.toISOString() : null,
    publicationTimeLocal: typeof rawLocalCandidate === 'string' && rawLocalCandidate.trim().length > 0
      ? rawLocalCandidate
      : localDate
        ? localDate.toISOString()
        : utcDate
          ? utcDate.toISOString()
          : null,
    hasValidPublicationTime: Boolean(utcDate || localDate)
  };
}

function computeExtractionQualityScore({ rawItem, canonicalText, rawSnippet, sourceProvidedSummary, urlIsValid, hasPublicationTime, articleType, rules }) {
  const scorecard = [];
  let score = 0;

  const canonicalLength = canonicalText.length;
  const snippetLength = rawSnippet.length;
  const summaryLength = sourceProvidedSummary.length;

  if (canonicalLength >= rules.quality_scoring.canonical_text_good_chars) {
    score += 0.4;
    scorecard.push({ rule: 'canonical_text_good', impact: 0.4 });
  } else if (canonicalLength >= rules.quality_scoring.canonical_text_partial_chars) {
    score += 0.24;
    scorecard.push({ rule: 'canonical_text_partial', impact: 0.24 });
  } else if (canonicalLength > 0) {
    score += 0.12;
    scorecard.push({ rule: 'canonical_text_short', impact: 0.12 });
  } else {
    scorecard.push({ rule: 'canonical_text_missing', impact: 0 });
  }

  if (snippetLength >= rules.quality_scoring.snippet_good_chars || summaryLength >= rules.quality_scoring.snippet_good_chars) {
    score += 0.2;
    scorecard.push({ rule: 'substantial_snippet_or_summary', impact: 0.2 });
  } else if (snippetLength >= rules.quality_scoring.snippet_partial_chars || summaryLength >= rules.quality_scoring.snippet_partial_chars) {
    score += 0.12;
    scorecard.push({ rule: 'partial_snippet_or_summary', impact: 0.12 });
  }

  if (hasPublicationTime) {
    score += 0.12;
    scorecard.push({ rule: 'publication_time_present', impact: 0.12 });
  }

  if (hasValue(resolveFirstString(rawItem, RAW_ITEM_FIELD_ALIASES.author))) {
    score += 0.08;
    scorecard.push({ rule: 'author_present', impact: 0.08 });
  }

  if (urlIsValid) {
    score += 0.1;
    scorecard.push({ rule: 'url_valid', impact: 0.1 });
  }

  if (!['gallery', 'media-only', 'navigation', 'podcast', 'video', 'video-only'].includes(articleType)) {
    score += 0.1;
    scorecard.push({ rule: 'article_like_type', impact: 0.1 });
  } else {
    score -= 0.2;
    scorecard.push({ rule: 'article_like_penalty', impact: -0.2 });
  }

  if (canonicalLength === 0 && snippetLength < rules.content_thresholds.substantial_snippet_chars && summaryLength < rules.content_thresholds.substantial_snippet_chars) {
    score -= 0.1;
    scorecard.push({ rule: 'thin_content_penalty', impact: -0.1 });
  }

  return {
    score: Math.max(0, Math.min(1, Number(score.toFixed(4)))),
    scorecard
  };
}

function classifyCompleteness(score, canonicalText, rawSnippet, rules) {
  if (score >= rules.content_thresholds.main_pool_min_quality && canonicalText.length > 0) {
    return 'complete';
  }

  if (score >= rules.content_thresholds.backup_pool_min_quality || rawSnippet.length >= rules.content_thresholds.substantial_snippet_chars) {
    return 'partial';
  }

  return 'weak';
}

function buildArticleId(sourceId, canonicalUrl, title) {
  return createHash('sha256')
    .update(`${sourceId}::${canonicalUrl ?? 'no-url'}::${title ?? 'no-title'}`)
    .digest('hex')
    .slice(0, 16);
}

function computePublicationIdentityScore(rawItem, canonicalTextLength, hasPublicationTime) {
  let score = 0;
  if (rawItem?.is_original_reporting === true) {
    score += 2;
  }
  if (rawItem?.is_syndicated_copy === true) {
    score -= 1.5;
  }
  if (hasValue(resolveFirstString(rawItem, RAW_ITEM_FIELD_ALIASES.author))) {
    score += 0.5;
  }
  if (hasPublicationTime) {
    score += 0.5;
  }
  if (canonicalTextLength > 0) {
    score += 0.5;
  }
  if (hasValue(rawItem?.original_publication_url)) {
    score += 0.5;
  }
  return score;
}

export function normalizeRawItem({ source, rawItem, rules, fetchedAt }) {
  const title = resolveFirstString(rawItem, RAW_ITEM_FIELD_ALIASES.title);
  const url = resolveFirstString(rawItem, RAW_ITEM_FIELD_ALIASES.url);
  const rawSnippet = resolveFirstString(rawItem, RAW_ITEM_FIELD_ALIASES.rawSnippet) ?? '';
  const sourceProvidedSummary = resolveFirstString(rawItem, RAW_ITEM_FIELD_ALIASES.sourceProvidedSummary) ?? '';
  const canonicalText = resolveFirstString(rawItem, RAW_ITEM_FIELD_ALIASES.canonicalText) ?? '';
  const author = resolveFirstString(rawItem, RAW_ITEM_FIELD_ALIASES.author);
  const articleType = resolveFirstString(rawItem, RAW_ITEM_FIELD_ALIASES.articleType) ?? source.expected_article_type;
  const normalizedArticleType = articleType.toLowerCase();
  const publication = resolvePublicationTimes(rawItem);
  const urlIsValid = isValidUrl(url);
  const canonicalUrl = canonicalizeUrl(url, source.canonicalization_policy);
  const quality = computeExtractionQualityScore({
    rawItem,
    canonicalText,
    rawSnippet,
    sourceProvidedSummary,
    urlIsValid,
    hasPublicationTime: publication.hasValidPublicationTime,
    articleType: normalizedArticleType,
    rules
  });
  const completeness = classifyCompleteness(quality.score, canonicalText, rawSnippet, rules);
  const warnings = [];

  if (!title) {
    warnings.push('missing_title');
  }
  if (!urlIsValid) {
    warnings.push('invalid_url');
  }
  if (!publication.hasValidPublicationTime) {
    warnings.push('missing_publication_time');
  }
  if (canonicalText.length === 0) {
    warnings.push('missing_canonical_text');
  }

  return createCanonicalArticleRecord({
    article_id: buildArticleId(source.source_id, canonicalUrl, title),
    source_id: source.source_id,
    source_display_name: source.display_name,
    source_priority_tier: source.priority_tier,
    title,
    url,
    canonical_url: canonicalUrl,
    author,
    publication_time_utc: publication.publicationTimeUtc,
    publication_time_local: publication.publicationTimeLocal,
    language: source.language,
    source_class: source.source_class,
    raw_snippet: rawSnippet || null,
    source_provided_summary: sourceProvidedSummary || null,
    canonical_text: canonicalText || null,
    extraction_quality_score: quality.score,
    extraction_quality_breakdown: quality.scorecard,
    article_type: normalizedArticleType,
    paywall_flag: resolveFirstDefined(rawItem, RAW_ITEM_FIELD_ALIASES.paywallFlag) ?? source.paywall_policy !== 'free',
    ingestion_method: resolveFirstString(rawItem, RAW_ITEM_FIELD_ALIASES.ingestionMethod) ?? source.fetch_method,
    fetched_at: fetchedAt,
    default_topic_affinities: source.default_topic_affinities,
    reliability_status: source.reliability_status,
    content_completeness: completeness,
    normalization_warnings: warnings,
    publication_identity_score: computePublicationIdentityScore(rawItem, canonicalText.length, publication.hasValidPublicationTime),
    content_fingerprint: buildTokenFingerprint(
      canonicalText || sourceProvidedSummary || rawSnippet,
      rules.deduplication.max_content_fingerprint_chars
    )
  });
}
