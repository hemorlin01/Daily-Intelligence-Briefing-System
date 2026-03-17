import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildCandidatePools, buildSemanticCards, createSemanticCard, loadSemanticTaxonomy, validateSemanticCard } from '../src/index.js';
import { FIXED_NOW, makeRawEntry } from './fixtures/factories.js';

function withTempDir(callback) {
  const directory = mkdtempSync(join(tmpdir(), 'dibs-phase2-'));
  try {
    return callback(directory);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function buildMainPool(entries) {
  const result = buildCandidatePools({
    rawItems: entries,
    now: FIXED_NOW
  });
  return result.mainPool;
}

function countWords(text) {
  return (text.match(/\b[\p{L}\p{N}'-]+\b/gu) ?? []).length;
}

function hasChinese(text) {
  return /[\u3400-\u9fff]/u.test(text);
}

function endsWithTerminalPunctuation(text, language) {
  if (language === 'zh') {
    return /[。！？]$/.test(text);
  }
  return /[.!?]$/.test(text);
}

function makeCanonicalMainRecord(overrides = {}) {
  const [record] = buildMainPool([
    makeRawEntry('reuters', {
      title: 'US regulators outline new semiconductor export plan',
      url: 'https://www.reuters.com/world/us-regulators-outline-semiconductor-export-plan',
      canonical_text: 'US regulators outlined a new semiconductor export plan for advanced chips and manufacturing tools. The proposal will affect suppliers, trade flows, and AI infrastructure investment across multiple regions. Companies are assessing compliance exposure and supply-chain adjustments as the rule advances.'.repeat(5),
      snippet: 'The proposal is expected to affect suppliers, trade flows, and AI infrastructure investment.',
      article_type: 'news'
    })
  ]);

  return {
    ...record,
    ...overrides,
    candidate_disposition: 'main'
  };
}

test('valid canonical article produces a valid semantic card', () => {
  const [record] = buildMainPool([makeRawEntry('reuters')]);
  const result = buildSemanticCards({
    canonicalRecords: record ? [record] : []
  });

  assert.equal(result.cards.length, 1);
  const card = result.cards[0];
  assert.equal(card.article_id, record.article_id);
  assert.equal(card.source_id, record.source_id);
  assert.equal(card.title, record.title);
  assert.equal(card.url, record.url);
  assert.equal(Array.isArray(card.topic_labels), true);
  assert.equal(typeof card.factual_summary, 'string');
  assert.equal(typeof card.why_it_matters, 'string');
  assert.equal(card.factual_summary.length > 0, true);
  assert.equal(card.why_it_matters.length > 0, true);
});

test('weak canonical article produces semantic warnings', () => {
  const result = buildSemanticCards({
    canonicalRecords: [
      makeCanonicalMainRecord({
        canonical_text: 'Brief update.',
        raw_snippet: 'Brief market update.',
        extraction_quality_score: 0.32,
        title: 'Update on markets'
      })
    ]
  });

  assert.equal(result.cards.length, 1);
  const codes = result.cards[0].warnings.map((warning) => warning.code);
  assert.equal(codes.includes('low_input_quality'), true);
  assert.equal(codes.includes('weak_canonical_text'), true);
});

test('semantic title remains unchanged', () => {
  const originalTitle = 'Original headline text must remain unchanged';
  const result = buildSemanticCards({
    canonicalRecords: [
      makeCanonicalMainRecord({
        title: originalTitle
      })
    ]
  });

  assert.equal(result.cards[0].title, originalTitle);
});

test('factual_summary follows source language behavior', () => {
  const mainPool = buildMainPool([
    makeRawEntry('wsj-cn-daily', {
      title: '中国制造业投资继续扩张',
      url: 'https://cn.wsj.com/articles/manufacturing-investment-expands',
      canonical_text: '中国制造业投资继续扩张，地方政府与企业正在加快推进新产能项目。分析人士认为，这将影响供应链布局和资本开支预期。'.repeat(5),
      snippet: '地方政府与企业正在加快推进新产能项目。',
      article_type: 'analysis'
    })
  ]);

  const result = buildSemanticCards({
    canonicalRecords: mainPool
  });

  assert.equal(result.cards.length, 1);
  assert.equal(hasChinese(result.cards[0].factual_summary), true);
  assert.equal(endsWithTerminalPunctuation(result.cards[0].factual_summary, 'zh'), true);
  assert.doesNotMatch(result.cards[0].factual_summary, /\.{3}|…/u);
});

test('why_it_matters follows source language for English and Chinese items', () => {
  const result = buildSemanticCards({
    canonicalRecords: [
      makeCanonicalMainRecord(),
      {
        ...makeCanonicalMainRecord({
          article_id: 'zh-why-1',
          title: '中国政策调整影响产业布局',
          url: 'https://example.com/zh-policy',
          language: 'zh',
          canonical_text: '中国政策调整影响产业布局，相关部门强调重点领域投资与产业升级方向。'.repeat(4),
          raw_snippet: '政策调整影响产业布局。',
          source_id: 'caixin',
          source_priority_tier: 1
        }),
        candidate_disposition: 'main'
      }
    ]
  });

  assert.equal(hasChinese(result.cards[0].why_it_matters), false);
  assert.equal(hasChinese(result.cards[1].why_it_matters), true);
});

test('summary length policy distinguishes full-text and summary-only items', () => {
  const fullTextRecord = makeCanonicalMainRecord({
    article_id: 'full-text-1',
    canonical_text: 'Regulators outlined a detailed export plan that affects advanced chips, tooling, and supplier exposure across multiple regions. '.repeat(18),
    raw_snippet: 'Regulators outlined a detailed export plan affecting advanced chips and tooling.'
  });
  const summaryOnlyRecord = makeCanonicalMainRecord({
    article_id: 'summary-only-1',
    canonical_text: '',
    raw_snippet: 'Export regulators outlined a plan affecting advanced chips, tooling, and supplier exposure across regions. Suppliers are adjusting capacity, compliance, and inventory buffers as the rule advances. Executives are assessing how quickly the changes cascade through procurement.',
    source_provided_summary: 'Regulators outlined an export plan and suppliers are adjusting capacity and compliance in response.'
  });

  const result = buildSemanticCards({
    canonicalRecords: [fullTextRecord, summaryOnlyRecord]
  });

  const fullSummaryWords = countWords(result.cards[0].factual_summary);
  const summaryOnlyWords = countWords(result.cards[1].factual_summary);

  assert.equal(fullSummaryWords >= 70, true);
  assert.equal(fullSummaryWords <= 110, true);
  assert.equal(summaryOnlyWords >= 20, true);
  assert.equal(summaryOnlyWords <= 70, true);
});

test('why_it_matters stays within intended quality bounds', () => {
  const result = buildSemanticCards({
    canonicalRecords: [makeCanonicalMainRecord()]
  });

  const why = result.cards[0].why_it_matters;
  const words = countWords(why);
  assert.equal(words >= 25, true);
  assert.equal(words <= 55, true);
  assert.doesNotMatch(why, /\bcould\b|\bmay\b|\breframes?\b|\bimplications beyond\b/i);
});

test('invalid event_type is rejected', () => {
  const taxonomy = loadSemanticTaxonomy('./config/semantic-taxonomy.json');

  assert.throws(
    () => createSemanticCard({
      article_id: 'a1',
      source_id: 'reuters',
      title: 'Title',
      url: 'https://example.com',
      language: 'en',
      event_type: 'not_valid',
      primary_entities: [],
      secondary_entities: [],
      geography_primary: null,
      geography_secondary: [],
      topic_labels: ['global_macro'],
      strategic_dimensions: [],
      candidate_keywords: ['macro'],
      factual_summary: 'A factual summary.',
      why_it_matters: 'This matters for the broader macro environment because it signals implications beyond the immediate article detail.',
      novelty_signal: 'new_event',
      user_relevance_signal: 'high',
      confidence_score: 0.8,
      warnings: []
    }, taxonomy),
    /invalid event_type/
  );
});

test('invalid topic label is rejected', () => {
  const taxonomy = loadSemanticTaxonomy('./config/semantic-taxonomy.json');
  const card = {
    article_id: 'a1',
    source_id: 'reuters',
    title: 'Title',
    url: 'https://example.com',
    language: 'en',
    event_type: 'market_signal',
    primary_entities: [],
    secondary_entities: [],
    geography_primary: null,
    geography_secondary: [],
    topic_labels: ['bad_label'],
    strategic_dimensions: [],
    candidate_keywords: ['macro'],
    factual_summary: 'A factual summary.',
    why_it_matters: 'This matters for the broader macro environment because it signals implications beyond the immediate article detail.',
    novelty_signal: 'new_event',
    user_relevance_signal: 'high',
    confidence_score: 0.8,
    warnings: []
  };

  assert.throws(
    () => validateSemanticCard(card, taxonomy),
    /invalid topic label/
  );
});

test('semantic card fails when topic_labels is empty', () => {
  const taxonomy = loadSemanticTaxonomy('./config/semantic-taxonomy.json');
  const card = {
    article_id: 'a-empty-topic',
    source_id: 'reuters',
    title: 'Title',
    url: 'https://example.com',
    language: 'en',
    event_type: 'market_signal',
    primary_entities: ['US'],
    secondary_entities: [],
    geography_primary: 'United States',
    geography_secondary: [],
    topic_labels: [],
    strategic_dimensions: [],
    candidate_keywords: ['US', 'market_signal'],
    factual_summary: 'A factual summary.',
    why_it_matters: 'US market developments could alter capital allocation assumptions.',
    novelty_signal: 'new_event',
    user_relevance_signal: 'high',
    confidence_score: 0.8,
    warnings: []
  };

  assert.throws(
    () => validateSemanticCard(card, taxonomy),
    /"topic_labels" must not be empty/
  );
});

test('semantic card fails when candidate_keywords is empty', () => {
  const taxonomy = loadSemanticTaxonomy('./config/semantic-taxonomy.json');
  const card = {
    article_id: 'a-empty-keywords',
    source_id: 'reuters',
    title: 'Title',
    url: 'https://example.com',
    language: 'en',
    event_type: 'market_signal',
    primary_entities: ['US'],
    secondary_entities: [],
    geography_primary: 'United States',
    geography_secondary: [],
    topic_labels: ['global_macro'],
    strategic_dimensions: [],
    candidate_keywords: [],
    factual_summary: 'A factual summary.',
    why_it_matters: 'US market developments could alter capital allocation assumptions.',
    novelty_signal: 'new_event',
    user_relevance_signal: 'high',
    confidence_score: 0.8,
    warnings: []
  };

  assert.throws(
    () => validateSemanticCard(card, taxonomy),
    /"candidate_keywords" must not be empty/
  );
});

test('why_it_matters is more article-specific than the generic fallback', () => {
  const result = buildSemanticCards({
    canonicalRecords: [
      makeCanonicalMainRecord()
    ]
  });

  const whyItMatters = result.cards[0].why_it_matters;
  assert.match(whyItMatters, /matters for/i);
  assert.match(whyItMatters, /policy|competition|compliance|market|risk/i);
  assert.doesNotMatch(whyItMatters, /implications beyond the immediate article detail/i);
});

test('summary and why_it_matters overlap is handled without collapsing the fields', () => {
  const result = buildSemanticCards({
    canonicalRecords: [
      makeCanonicalMainRecord({
        title: 'This matters for capital allocation because it signals implications beyond the immediate article detail.',
        canonical_text: 'This matters for capital allocation because it signals implications beyond the immediate article detail.',
        raw_snippet: 'This matters for capital allocation because it signals implications beyond the immediate article detail.'
      })
    ]
  });

  const card = result.cards[0];
  const codes = card.warnings.map((warning) => warning.code);
  assert.equal(card.factual_summary === card.why_it_matters, false);
  assert.equal(codes.includes('summary_why_overlap') || card.metadata.overlap_scores.exceeds === false, true);
});

test('low-confidence extraction is surfaced', () => {
  const result = buildSemanticCards({
    canonicalRecords: [
      {
        article_id: 'low-confidence-1',
        source_id: 'reuters',
        title: 'Brief note',
        url: 'https://example.com/brief-note',
        language: 'en',
        source_priority_tier: 1,
        canonical_text: 'Brief note.',
        raw_snippet: 'Brief note.',
        source_provided_summary: null,
        extraction_quality_score: 0.2,
        article_type: 'news',
        candidate_disposition: 'main'
      }
    ]
  });

  assert.equal(result.cards.length, 1);
  assert.equal(result.cards[0].confidence_score < 0.45, true);
  assert.equal(result.cards[0].warnings.some((warning) => warning.code === 'low_confidence_extraction'), true);
});

test('why_it_matters remains distinct from factual_summary after specificity changes', () => {
  const result = buildSemanticCards({
    canonicalRecords: [
      makeCanonicalMainRecord()
    ]
  });

  const card = result.cards[0];
  assert.notEqual(card.factual_summary, card.why_it_matters);
  assert.equal(card.metadata.overlap_scores.exceeds, false);
});

test('backup_pool items are not consumed by the semantic pipeline', () => {
  const phase1 = buildCandidatePools({
    rawItems: [
      makeRawEntry('techcrunch', {
        title: 'AI buyers compare procurement platforms',
        url: 'https://techcrunch.com/2026/03/16/ai-buyers-compare-procurement-platforms/',
        canonical_text: '',
        snippet: 'Enterprise buyers are comparing AI procurement tools, contracts, obligations, and vendor risk as spending expands across the sector in 2026.',
        summary: ''
      })
    ],
    now: FIXED_NOW
  });

  const result = buildSemanticCards({
    canonicalRecords: phase1.backupPool
  });

  assert.equal(result.cards.length, 0);
  assert.equal(result.failures.length, 1);
  assert.equal(result.failures[0].reason, 'non_main_pool_record');
});

test('semantic output is deterministic under the same input', () => {
  const record = makeCanonicalMainRecord();
  const first = buildSemanticCards({
    canonicalRecords: [record],
    runTimestamp: FIXED_NOW.toISOString()
  });
  const second = buildSemanticCards({
    canonicalRecords: [record],
    runTimestamp: FIXED_NOW.toISOString()
  });

  assert.deepEqual(first.cards, second.cards);
  assert.deepEqual(first.diagnostics.warning_counts_by_type, second.diagnostics.warning_counts_by_type);
});

test('semantic diagnostics are generated with expected core fields', () => {
  withTempDir((directory) => {
    const result = buildSemanticCards({
      canonicalRecords: [
        makeCanonicalMainRecord(),
        makeCanonicalMainRecord({
          article_id: 'manual-2',
          source_id: 'financial-times',
          title: 'Investors reassess climate infrastructure spending',
          url: 'https://www.ft.com/content/climate-infrastructure-spending',
          language: 'en',
          source_priority_tier: 1,
          canonical_text: 'Investors are reassessing climate infrastructure spending after new policy support and project delays changed return expectations.'.repeat(4),
          raw_snippet: 'The shift affects project finance, energy transition, and infrastructure planning.',
          source_provided_summary: 'The shift affects project finance and infrastructure planning.'
        })
      ],
      outputDir: directory,
      runTimestamp: FIXED_NOW.toISOString()
    });

    const cardsPath = join(directory, 'semantic_cards.json');
    const diagnosticsPath = join(directory, 'semantic_diagnostics.json');

    assert.equal(existsSync(cardsPath), true);
    assert.equal(existsSync(diagnosticsPath), true);
    assert.equal(result.cards.length, 2);

    const diagnostics = JSON.parse(readFileSync(diagnosticsPath, 'utf8'));
    assert.equal(diagnostics.total_semantic_cards_produced, 2);
    assert.equal('confidence_score_distribution' in diagnostics, true);
    assert.equal('warning_counts_by_type' in diagnostics, true);
    assert.equal('event_type_distribution' in diagnostics, true);
    assert.equal('topic_label_distribution' in diagnostics, true);
    assert.equal('strategic_dimension_distribution' in diagnostics, true);
    assert.equal('language_distribution' in diagnostics, true);
    assert.equal('empty_topic_label_attempt_count' in diagnostics, true);
    assert.equal('empty_topic_label_validation_failure_count' in diagnostics, true);
    assert.equal('empty_candidate_keyword_attempt_count' in diagnostics, true);
    assert.equal('empty_candidate_keyword_validation_failure_count' in diagnostics, true);
    assert.equal('geography_ambiguity_warning_count' in diagnostics, true);
    assert.equal('entity_extraction_uncertainty_warning_count' in diagnostics, true);
    assert.equal('overlap_warning_count' in diagnostics, true);
    assert.equal('summary_why_overlap_warning_count' in diagnostics, true);
    assert.equal('low_confidence_count' in diagnostics, true);
    assert.equal('low_confidence_semantic_card_count' in diagnostics, true);
  });
});

test('diagnostics expose explicit weakness counts for weak semantic outputs', () => {
  const result = buildSemanticCards({
    canonicalRecords: [
      {
        article_id: 'weak-diagnostics-1',
        source_id: 'reuters',
        title: 'market note',
        url: 'https://example.com/brief-note',
        language: 'en',
        source_priority_tier: 1,
        canonical_text: 'market note.',
        raw_snippet: 'market note.',
        source_provided_summary: null,
        extraction_quality_score: 0.2,
        article_type: 'news',
        candidate_disposition: 'main'
      }
    ],
    runTimestamp: FIXED_NOW.toISOString()
  });

  assert.equal(result.diagnostics.low_confidence_semantic_card_count >= 1, true);
  assert.equal(result.diagnostics.entity_extraction_uncertainty_warning_count >= 1, true);
  assert.equal(result.diagnostics.geography_ambiguity_warning_count >= 1, true);
  assert.equal(result.diagnostics.empty_topic_label_validation_failure_count, 0);
  assert.equal(result.diagnostics.empty_candidate_keyword_validation_failure_count, 0);
});
