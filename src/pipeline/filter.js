import { hasValue, normalizeTitleForComparison } from '../utils/text.js';
import { isValidUrl } from '../utils/url.js';

function inferAgeHours(publicationTimeUtc, now) {
  if (!publicationTimeUtc) {
    return null;
  }

  const then = new Date(publicationTimeUtc);
  if (Number.isNaN(then.getTime())) {
    return null;
  }

  return (now.getTime() - then.getTime()) / 36e5;
}

function isArticleLike(record, rules) {
  if (rules.article_like_rules.blocked_article_types.includes(record.article_type)) {
    return false;
  }

  if (typeof record.url === 'string') {
    const lowerUrl = record.url.toLowerCase();
    if (rules.article_like_rules.blocked_url_substrings.some((token) => lowerUrl.includes(token))) {
      return false;
    }
  }

  const normalizedTitle = normalizeTitleForComparison(record.title ?? '');
  if (rules.article_like_rules.blocked_title_keywords.some((keyword) => normalizedTitle.includes(keyword))) {
    return false;
  }

  return true;
}

function hasEnoughContentSignal(record, rules) {
  const canonicalLength = record.canonical_text?.length ?? 0;
  const snippetLength = record.raw_snippet?.length ?? 0;
  const summaryLength = record.source_provided_summary?.length ?? 0;

  return canonicalLength > 0
    || snippetLength >= rules.content_thresholds.substantial_snippet_chars
    || summaryLength >= rules.content_thresholds.substantial_snippet_chars;
}

function isLongFormEligible(record, source, rules, ageHours) {
  const sourceWindow = source.allowed_long_form_window_hours ?? rules.candidate_windows.long_form_max_hours;
  const longFormTypes = new Set(['analysis', 'essay', 'feature', 'research']);
  return ageHours !== null
    && ageHours <= sourceWindow
    && longFormTypes.has(record.article_type);
}

export function classifyCandidate({ record, source, rules, now }) {
  const reasons = [];
  const warnings = [];
  const malformedReasons = [];
  const ageHours = inferAgeHours(record.publication_time_utc, now);

  if (!source) {
    reasons.push('unapproved_source');
  } else if (source.active_status !== 'active') {
    reasons.push('inactive_source');
  }

  if (!isValidUrl(record.url)) {
    reasons.push('invalid_url');
    malformedReasons.push('invalid_url');
  }

  if (!hasValue(record.title)) {
    reasons.push('missing_title');
    malformedReasons.push('missing_title');
  }

  if (!hasEnoughContentSignal(record, rules)) {
    reasons.push('insufficient_content_signal');
  }

  if (!isArticleLike(record, rules)) {
    reasons.push('non_article_like');
  }

  if (ageHours === null) {
    reasons.push('missing_publication_time');
    malformedReasons.push('missing_publication_time');
  } else if (ageHours > rules.candidate_windows.default_fetch_hours && !(source && isLongFormEligible(record, source, rules, ageHours))) {
    reasons.push('stale_item');
  } else if (ageHours > rules.candidate_windows.preferred_freshness_hours) {
    warnings.push('outside_preferred_freshness_window');
  }

  if (reasons.length > 0) {
    return {
      disposition: 'rejected',
      reasons,
      warnings,
      age_hours: ageHours,
      malformed: malformedReasons.length > 0
    };
  }

  const canonicalLength = record.canonical_text?.length ?? 0;
  const snippetLength = record.raw_snippet?.length ?? 0;
  const summaryLength = record.source_provided_summary?.length ?? 0;
  const weakButUsable = record.extraction_quality_score < rules.content_thresholds.main_pool_min_quality
    || record.content_completeness === 'weak'
    || (canonicalLength === 0 && (snippetLength >= rules.content_thresholds.substantial_snippet_chars || summaryLength >= rules.content_thresholds.substantial_snippet_chars));

  if (weakButUsable) {
    return {
      disposition: 'backup',
      reasons: ['weak_extraction'],
      warnings,
      age_hours: ageHours,
      malformed: false
    };
  }

  return {
    disposition: 'main',
    reasons: [],
    warnings,
    age_hours: ageHours,
    malformed: false
  };
}
