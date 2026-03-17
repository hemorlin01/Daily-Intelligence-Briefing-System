import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { loadSemanticRules, loadSemanticTaxonomy } from '../config/load-config.js';
import { createSemanticCard } from '../models/semantic-card.js';
import { buildTokenFingerprint, diceCoefficient, jaccardSimilarity, normalizeTitleForComparison, normalizeWhitespace, tokenize } from '../utils/text.js';

const DEFAULT_SEMANTIC_TAXONOMY_PATH = resolve(process.cwd(), 'config', 'semantic-taxonomy.json');
const DEFAULT_SEMANTIC_RULES_PATH = resolve(process.cwd(), 'config', 'semantic-rules.json');
const SUMMARY_RESIDUE_PATTERNS_EN = [
  /the post\b.*\bappeared first on/i,
  /\bappeared first on\b/i,
  /\bwas originally published on\b/i,
  /\boriginally appeared on\b/i,
  /\boriginally published at\b/i,
  /\bthis article was originally published\b/i,
  /\bprimary image\b/i,
  /\bfeatured image\b/i,
  /\bimage credit\b/i,
  /\bphoto credit\b/i,
  /\bposted in\b/i,
  /\bfiled under\b/i,
  /\bcategory:\b/i,
  /\btags?:\b/i
];
const SUMMARY_RESIDUE_PATTERNS_ZH = [
  /\u539f\u6587\u94fe\u63a5/,
  /\u9605\u8bfb\u539f\u6587/,
  /\u66f4\u591a\u5185\u5bb9/,
  /\u6765\u6e90[:\uFF1A]/,
  /\u8f6c\u8f7d/
];
const GENERIC_WHY_PHRASES_EN = [
  /provides a frame/i,
  /clarifies the assumptions/i,
  /decision frame/i,
  /decision assumptions/i
];
const SOFT_CONTENT_DOMAINS = new Set(['culture_design', 'lifestyle_signals']);
const SOFT_CONTENT_TITLE_PATTERNS = [
  /\breview\b/i,
  /\bfirst look\b/i,
  /\bimpressions\b/i,
  /\bguide\b/i,
  /\bbest\b/i,
  /\btop\s+\d+\b/i
];

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

function countWords(text) {
  const normalized = normalizeWhitespace(text);
  return (normalized.match(/\b[\p{L}\p{N}'-]+\b/gu) ?? []).length;
}

function countChineseChars(text) {
  return (normalizeWhitespace(text).match(/[\u3400-\u9fff]/gu) ?? []).length;
}

function stripTrailingEllipsis(text) {
  return normalizeWhitespace(text).replace(/(\.\.\.|…)+$/u, '').trim();
}

function ensureTerminalPunctuation(text, language) {
  const normalized = normalizeWhitespace(text);
  if (!normalized) {
    return '';
  }
  if (language === 'zh') {
    return /[。！？]$/.test(normalized) ? normalized : `${normalized}。`;
  }
  return /[.!?]$/.test(normalized) ? normalized : `${normalized}.`;
}

function isSummaryComplete(text, language) {
  const normalized = normalizeWhitespace(text);
  if (!normalized) {
    return false;
  }
  if (language === 'zh') {
    return /[，。！？；：、]$/.test(normalized) && countChineseChars(normalized) >= 24;
  }
  return /[.!?]$/.test(normalized) && countWords(normalized) >= 12;
}

function splitSentencesNormalized(text, language) {
  const normalized = normalizeWhitespace(text);
  if (!normalized) {
    return [];
  }

  if (language === 'zh') {
    const matches = normalized.match(/[^。！？]+[。！？]?/gu) ?? [];
    return matches.map((sentence) => sentence.trim()).filter(Boolean);
  }

  return normalized
    .split(/(?<=[.!?])\s+/u)
    .map((sentence) => sentence.trim())
    .filter(Boolean);
}

function stripResidueFromSentence(sentence, language) {
  const patterns = language === 'zh' ? SUMMARY_RESIDUE_PATTERNS_ZH : SUMMARY_RESIDUE_PATTERNS_EN;
  let trimmed = sentence;
  for (const pattern of patterns) {
    const index = trimmed.search(pattern);
    if (index >= 0) {
      trimmed = trimmed.slice(0, index).trim();
    }
  }
  return trimmed;
}

function cleanSummaryResidue(summary, language) {
  const normalized = normalizeWhitespace(summary);
  if (!normalized) {
    return normalized;
  }

  const sentences = splitSentencesNormalized(normalized, language);
  if (sentences.length === 0) {
    return ensureTerminalPunctuation(stripTrailingEllipsis(normalized), language);
  }

  const cleanedSentences = sentences
    .map((sentence) => stripResidueFromSentence(sentence, language))
    .filter(Boolean);

  if (cleanedSentences.length === 0) {
    return ensureTerminalPunctuation(stripTrailingEllipsis(normalized), language);
  }

  const joined = language === 'zh' ? cleanedSentences.join('') : cleanedSentences.join(' ');
  return ensureTerminalPunctuation(stripTrailingEllipsis(joined), language);
}

function isSentenceSubstantial(sentence, language) {
  const normalized = normalizeWhitespace(sentence);
  if (!normalized) {
    return false;
  }
  if (language === 'zh') {
    return countChineseChars(normalized) >= 12;
  }
  return countWords(normalized) >= 6;
}

function filterSummaryCandidates(sentences, language) {
  const filtered = sentences.filter((sentence) => isSentenceSubstantial(sentence, language));
  return filtered.length > 0 ? filtered : sentences;
}

function collectSentences(texts, language) {
  const seen = new Set();
  const ordered = [];
  for (const text of texts) {
    for (const sentence of splitSentencesNormalized(text, language)) {
      const normalized = normalizeWhitespace(sentence);
      if (!normalized) {
        continue;
      }
      const key = language === 'zh' ? normalized : normalized.toLowerCase();
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      ordered.push(normalized);
    }
  }
  return ordered;
}

function buildSummaryFromSentences({ sentences, language, minWords, maxWords, minChars, maxChars }) {
  if (sentences.length === 0) {
    return '';
  }

  const selected = [];
  let count = 0;
  const isChinese = language === 'zh';

  for (const sentence of sentences) {
    const length = isChinese ? countChineseChars(sentence) : countWords(sentence);
    if (selected.length === 0) {
      selected.push(sentence);
      count += length;
      continue;
    }

    if (isChinese) {
      if (count >= minChars && count + length > maxChars) {
        break;
      }
    } else if (count >= minWords && count + length > maxWords) {
      break;
    }

    selected.push(sentence);
    count += length;

    if (isChinese && count >= minChars && count >= maxChars) {
      break;
    }
    if (!isChinese && count >= minWords && count >= maxWords) {
      break;
    }
  }

  const joined = isChinese ? selected.join('') : selected.join(' ');
  const cleaned = stripTrailingEllipsis(joined);
  return ensureTerminalPunctuation(cleaned, language);
}

function summarizeFactually(record, rules, warnings) {
  const sourceLanguage = record.language === 'zh' ? 'zh' : 'en';
  const language = rules.language_behavior.factual_summary_same_as_source
    ? sourceLanguage
    : (rules.language_behavior.fallback_language || 'en');

  const hasFullText = typeof record.canonical_text === 'string'
    && record.canonical_text.length >= rules.summary_rules.full_text_min_chars;
  const hasSnippet = typeof record.raw_snippet === 'string'
    && record.raw_snippet.length >= rules.summary_rules.summary_only_min_chars;
  const hasSummary = typeof record.source_provided_summary === 'string'
    && record.source_provided_summary.length >= rules.summary_rules.summary_only_min_chars;

  const primaryTexts = hasFullText
    ? [record.canonical_text, record.raw_snippet, record.source_provided_summary]
    : [record.raw_snippet, record.source_provided_summary, record.canonical_text];

  const sentences = collectSentences(primaryTexts.filter(Boolean), language);
  const summarySentences = filterSummaryCandidates(sentences, language);

  if (summarySentences.length === 0 && record.title) {
    summarySentences.push(record.title);
    addWarning(
      warnings,
      'summary_from_title',
      'Factual summary falls back to the headline because no other text was available.',
      'info'
    );
  }

  if (!hasFullText) {
    addWarning(
      warnings,
      'weak_canonical_text',
      'Canonical text is limited; summary leans on feed snippets or summaries.',
      'warning'
    );
  }

  const summary = language === 'zh'
    ? buildSummaryFromSentences({
        sentences: summarySentences,
        language,
        minChars: hasFullText
          ? rules.summary_rules.non_english_min_chars
          : rules.summary_rules.non_english_summary_only_min_chars,
        maxChars: hasFullText
          ? rules.summary_rules.non_english_max_chars
          : rules.summary_rules.non_english_summary_only_max_chars
      })
    : buildSummaryFromSentences({
        sentences: summarySentences,
        language,
        minWords: hasFullText
          ? rules.summary_rules.english_min_words
          : rules.summary_rules.english_summary_only_min_words,
        maxWords: hasFullText
          ? rules.summary_rules.english_max_words
          : rules.summary_rules.english_summary_only_max_words
      });

  let cleanedSummary = cleanSummaryResidue(summary, language);

  if (!isSummaryComplete(cleanedSummary, language)) {
    const fallbackSentences = summarySentences
      .map((sentence) => stripResidueFromSentence(sentence, language))
      .filter(Boolean);

    if (fallbackSentences.length > 0) {
      const fallbackSummary = language === 'zh'
        ? buildSummaryFromSentences({
            sentences: fallbackSentences,
            language,
            minChars: hasFullText
              ? rules.summary_rules.non_english_min_chars
              : rules.summary_rules.non_english_summary_only_min_chars,
            maxChars: hasFullText
              ? rules.summary_rules.non_english_max_chars
              : rules.summary_rules.non_english_summary_only_max_chars
          })
        : buildSummaryFromSentences({
            sentences: fallbackSentences,
            language,
            minWords: hasFullText
              ? rules.summary_rules.english_min_words
              : rules.summary_rules.english_summary_only_min_words,
            maxWords: hasFullText
              ? rules.summary_rules.english_max_words
              : rules.summary_rules.english_summary_only_max_words
          });

      cleanedSummary = cleanSummaryResidue(fallbackSummary, language);
    }
  }

  if (!isSummaryComplete(cleanedSummary, language) && record.title) {
    cleanedSummary = ensureTerminalPunctuation(stripTrailingEllipsis(record.title), language);
  }

  if (!hasFullText && !hasSnippet && !hasSummary) {
    addWarning(
      warnings,
      'summary_limited_context',
      'Summary is constrained by limited feed text.',
      'info'
    );
  }

  return cleanedSummary;
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

function implicationFromTopicZh(topic) {
  const map = {
    global_macro: '宏观与市场预期',
    geopolitics: '地缘风险与政策判断',
    technology: '技术竞争与产业节奏',
    china_economy: '中国经济与政策走向',
    climate_transition: '气候与能源转型节奏',
    urban_systems: '城市治理与基础设施决策',
    digital_economy: '平台与数字商业模式',
    policy_analysis: '监管与制度安排',
    culture_design: '文化与设计信号',
    lifestyle_signals: '消费与生活方式趋势'
  };

  return map[topic] ?? '结构性判断';
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

function implicationFromStrategicDimensionZh(label) {
  const map = {
    supply_chain: '供应链韧性',
    industrial_policy: '产业政策方向',
    climate_risk: '气候风险与适应',
    platform_power: '平台竞争与议价',
    capital_markets: '资本配置与市场定价',
    consumer_shift: '消费结构变化',
    energy_transition: '能源转型与电力系统',
    urban_governance: '城市治理与公共服务',
    digital_infrastructure: '数字基础设施建设',
    regulation: '监管压力',
    labor: '劳动力与就业',
    geopolitics: '地缘政治',
    trade: '贸易与关税',
    public_health: '公共卫生',
    AI_competition: 'AI 竞争'
  };
  return map[label] ?? '结构性判断';
}

function eventTypeLabel(eventType, language) {
  if (language !== 'zh') {
    return eventType.replace(/_/g, ' ');
  }

  const map = {
    policy_move: '政策变化',
    earnings_result: '业绩结果',
    regulatory_shift: '监管调整',
    product_launch: '产品发布',
    infrastructure_project: '基础设施项目',
    conflict_escalation: '冲突升级',
    market_signal: '市场信号',
    long_form_analysis: '深度分析',
    executive_change: '管理层变动',
    scientific_finding: '科研发现',
    legal_action: '法律行动',
    funding_or_deal: '融资或交易',
    opinion_or_argument: '观点文章'
  };
  return map[eventType] ?? '重要动态';
}

function eventTypeNounEn(eventType) {
  const map = {
    policy_move: 'policy move',
    earnings_result: 'earnings result',
    regulatory_shift: 'regulatory shift',
    product_launch: 'product launch',
    infrastructure_project: 'infrastructure project',
    conflict_escalation: 'conflict escalation',
    market_signal: 'market signal',
    long_form_analysis: 'analysis',
    executive_change: 'executive change',
    scientific_finding: 'scientific finding',
    legal_action: 'legal action',
    funding_or_deal: 'funding or deal',
    opinion_or_argument: 'opinion piece'
  };

  return map[eventType] ?? eventType.replace(/_/g, ' ');
}

function isSoftContentCandidate(record, topicLabels) {
  if (!Array.isArray(topicLabels) || !topicLabels.some((label) => SOFT_CONTENT_DOMAINS.has(label))) {
    return false;
  }

  const articleType = record.article_type ?? '';
  if (['review', 'lifestyle', 'service'].includes(articleType)) {
    return true;
  }

  const title = record.title ?? '';
  return SOFT_CONTENT_TITLE_PATTERNS.some((pattern) => pattern.test(title));
}

function buildWhySubject(entities, geographies, eventType, language) {
  const primaryEntity = entities.primary[0] ?? null;
  if (primaryEntity && geographies.primary) {
    return language === 'zh'
      ? `${primaryEntity}（${geographies.primary}）`
      : `${primaryEntity} in ${geographies.primary}`;
  }

  if (primaryEntity) {
    return primaryEntity;
  }

  if (geographies.primary) {
    return language === 'zh'
      ? `${geographies.primary}的${eventTypeLabel(eventType, language)}`
      : `${eventType.replace(/_/g, ' ')} developments in ${geographies.primary}`;
  }

  return language === 'zh'
    ? `该${eventTypeLabel(eventType, language)}`
    : `this ${eventType.replace(/_/g, ' ')}`;
}

function buildWhyImpactPhrase(topicLabels, strategicDimensions, language) {
  const primaryTopic = topicLabels[0] ?? 'policy_analysis';
  const primaryDimension = strategicDimensions[0] ?? null;
  if (primaryDimension) {
    return language === 'zh'
      ? implicationFromStrategicDimensionZh(primaryDimension)
      : implicationFromStrategicDimension(primaryDimension);
  }

  return language === 'zh'
    ? implicationFromTopicZh(primaryTopic)
    : implicationFromTopic(primaryTopic);
}

function buildWhyAngleEn(eventType) {
  const templates = {
    policy_move: 'it clarifies how policy priorities are tightening or loosening and where enforcement will land',
    regulatory_shift: 'it changes compliance costs and competitive constraints for operators',
    legal_action: 'it changes legal risk and settlement leverage for affected players',
    earnings_result: 'it shows how demand, margins, or guidance are shifting for operators and investors',
    market_signal: 'it resets near-term pricing and risk assumptions in the market',
    funding_or_deal: 'it reshapes ownership or capital flow, shifting competitive positioning',
    product_launch: 'it indicates where capability or adoption is moving in the market',
    executive_change: 'it can redirect strategy and execution in the organization',
    infrastructure_project: 'it sets a new baseline for capacity, logistics, or mobility decisions',
    scientific_finding: 'it updates the evidence base used for policy or investment decisions',
    conflict_escalation: 'it raises cross-border risk assumptions and policy responses',
    long_form_analysis: 'it clarifies the assumptions behind upcoming policy or market decisions',
    opinion_or_argument: 'it highlights the narrative influencing decision-makers and market sentiment'
  };

  return templates[eventType] ?? 'it sets a clear signal for how to read the next decision cycle';
}

function buildWhyAngleEnCompact(eventType) {
  const templates = {
    policy_move: 'policy priorities are shifting and enforcement is moving with them',
    regulatory_shift: 'compliance costs and competitive constraints are changing',
    legal_action: 'legal risk and leverage are moving',
    earnings_result: 'demand and margin signals are shifting',
    market_signal: 'near-term pricing assumptions are moving',
    funding_or_deal: 'capital and ownership are reshaping competition',
    product_launch: 'capability and adoption signals are shifting',
    executive_change: 'leadership direction is shifting',
    infrastructure_project: 'capacity baselines are changing',
    scientific_finding: 'evidence used for decisions is updating',
    conflict_escalation: 'risk assumptions are rising',
    long_form_analysis: 'decision assumptions are shifting',
    opinion_or_argument: 'the narrative shaping decisions is shifting'
  };

  return templates[eventType] ?? 'it shifts decision expectations';
}

function buildWhyAngleZh(eventType) {
  const templates = {
    policy_move: '政策边界正在调整，影响后续配置与执行',
    regulatory_shift: '监管要求变化会改变合规成本与竞争空间',
    legal_action: '法律行动改变操作空间与风险评估',
    earnings_result: '业绩透露需求与盈利趋势的变化',
    market_signal: '市场信号正在重塑短期预期',
    funding_or_deal: '资本与交易改变竞争格局',
    product_launch: '产品发布改变技术与应用节奏',
    executive_change: '领导层变化往往带来战略重点调整',
    infrastructure_project: '基础设施项目设定新的运行基线',
    scientific_finding: '新证据更新政策或投资判断依据',
    conflict_escalation: '冲突升级推高风险与政策不确定性',
    long_form_analysis: '深度分析提供后续解读框架',
    opinion_or_argument: '观点文章影响政策与舆论方向'
  };

  return templates[eventType] ?? '该进展改变后续判断的基线';
}

function buildWhyAngleZhCompact(eventType) {
  const templates = {
    policy_move: '政策边界调整，影响配置与执行',
    regulatory_shift: '监管变化改变竞争空间',
    legal_action: '法律风险在变化',
    earnings_result: '需求与盈利信号在变化',
    market_signal: '市场预期在变化',
    funding_or_deal: '资本与交易改写竞争',
    product_launch: '技术与应用节奏在变化',
    executive_change: '战略重点在变化',
    infrastructure_project: '容量基线在变化',
    scientific_finding: '证据基础在更新',
    conflict_escalation: '风险水平在上升',
    long_form_analysis: '解读框架在变化',
    opinion_or_argument: '舆论叙事在变化'
  };

  return templates[eventType] ?? '判断基线在变化';
}

function buildWhySecondaryClauseEn(topicLabels, strategicDimensions) {
  const primaryDimension = strategicDimensions[0] ?? null;
  if (primaryDimension) {
    return `It also sets expectations for ${implicationFromStrategicDimension(primaryDimension)}.`;
  }
  const primaryTopic = topicLabels[0] ?? null;
  if (primaryTopic) {
    return `It also informs ${implicationFromTopic(primaryTopic)} decisions.`;
  }
  return '';
}

function buildWhySecondaryClauseZh(topicLabels, strategicDimensions) {
  const primaryDimension = strategicDimensions[0] ?? null;
  if (primaryDimension) {
    return `这也关系到${implicationFromStrategicDimensionZh(primaryDimension)}。`;
  }
  const primaryTopic = topicLabels[0] ?? null;
  if (primaryTopic) {
    return `这也关系到${implicationFromTopicZh(primaryTopic)}。`;
  }
  return '';
}

function buildWhyItMattersEn({ subject, impactPhrase, eventType, topicLabels, strategicDimensions, rules, useCompactAngle = false }) {
  const minWords = rules.summary_rules.why_it_matters_min_words;
  const maxWords = rules.summary_rules.why_it_matters_max_words;
  const angle = useCompactAngle ? buildWhyAngleEnCompact(eventType) : buildWhyAngleEn(eventType);
  const compactAngle = buildWhyAngleEnCompact(eventType);

  let sentence = `${subject} matters for ${impactPhrase} because ${angle}.`;
  if (!useCompactAngle && countWords(sentence) > maxWords) {
    sentence = `${subject} matters for ${impactPhrase} because ${compactAngle}.`;
  }
  if (countWords(sentence) > maxWords) {
    sentence = `${subject} matters for ${impactPhrase}.`;
  }
  if (countWords(sentence) < minWords) {
    const secondary = buildWhySecondaryClauseEn(topicLabels, strategicDimensions);
    if (secondary) {
      sentence = `${ensureTerminalPunctuation(sentence, 'en')} ${secondary}`;
    }
  }

  return ensureTerminalPunctuation(sentence, 'en');
}

function buildWhyItMattersZh({ subject, impactPhrase, eventType, topicLabels, strategicDimensions, rules, useCompactAngle = false }) {
  const minChars = rules.summary_rules.why_it_matters_min_chars;
  const maxChars = rules.summary_rules.why_it_matters_max_chars;
  const angle = useCompactAngle ? buildWhyAngleZhCompact(eventType) : buildWhyAngleZh(eventType);
  const compactAngle = buildWhyAngleZhCompact(eventType);

  let sentence = `${subject}对${impactPhrase}很关键，因为${angle}。`;
  if (countChineseChars(sentence) > maxChars) {
    sentence = `${subject}对${impactPhrase}很关键，因为${compactAngle}。`;
  }
  if (countChineseChars(sentence) > maxChars) {
    sentence = `${subject}对${impactPhrase}很关键。`;
  }
  if (countChineseChars(sentence) < minChars) {
    const secondary = buildWhySecondaryClauseZh(topicLabels, strategicDimensions);
    if (secondary) {
      sentence = `${ensureTerminalPunctuation(sentence, 'zh')}${secondary}`;
    }
  }

  return ensureTerminalPunctuation(sentence, 'zh');
}

function buildWhyFallback({ actor, geographies, eventType, impactPhrase, language }) {
  if (language === 'zh') {
    const eventLabel = eventTypeLabel(eventType, 'zh');
    if (actor) {
      return ensureTerminalPunctuation(`${actor}相关的${eventLabel}影响${impactPhrase}`, 'zh');
    }
    if (geographies.primary) {
      return ensureTerminalPunctuation(`${geographies.primary}的${eventLabel}影响${impactPhrase}`, 'zh');
    }
    return ensureTerminalPunctuation(`这项${eventLabel}影响${impactPhrase}`, 'zh');
  }

  const eventNoun = eventTypeNounEn(eventType);
  if (actor) {
    return ensureTerminalPunctuation(`The ${eventNoun} around ${actor} affects ${impactPhrase}`, 'en');
  }
  if (geographies.primary) {
    return ensureTerminalPunctuation(`The ${eventNoun} in ${geographies.primary} affects ${impactPhrase}`, 'en');
  }
  return ensureTerminalPunctuation(`This ${eventNoun} affects ${impactPhrase}`, 'en');
}

function buildWhyItMatters(record, entities, geographies, eventType, topicLabels, strategicDimensions, rules) {
  const sourceLanguage = record.language === 'zh' ? 'zh' : 'en';
  const effectiveLanguage = rules.language_behavior.why_it_matters_same_as_source
    ? sourceLanguage
    : (rules.language_behavior.fallback_language || 'en');

  const subject = buildWhySubject(entities, geographies, eventType, effectiveLanguage);
  const impactPhrase = buildWhyImpactPhrase(topicLabels, strategicDimensions, effectiveLanguage);
  const useCompactAngle = isSoftContentCandidate(record, topicLabels);
  const hasSpecificAnchor = Boolean(entities.primary[0] || geographies.primary);
  let why = effectiveLanguage === 'zh'
    ? buildWhyItMattersZh({ subject, impactPhrase, eventType, topicLabels, strategicDimensions, rules, useCompactAngle })
    : buildWhyItMattersEn({ subject, impactPhrase, eventType, topicLabels, strategicDimensions, rules, useCompactAngle });

  if (effectiveLanguage === 'en') {
    const isGeneric = GENERIC_WHY_PHRASES_EN.some((pattern) => pattern.test(why));
    if (isGeneric || !hasSpecificAnchor) {
      why = buildWhyFallback({ actor: entities.primary[0] ?? null, geographies, eventType, impactPhrase, language: effectiveLanguage });
    }
  } else if (!hasSpecificAnchor) {
    why = buildWhyFallback({ actor: entities.primary[0] ?? null, geographies, eventType, impactPhrase, language: effectiveLanguage });
  }

  return why;
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
      canonical_author: record.author ?? null,
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
