import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { buildEditorialSelection, createSemanticCard, loadEditorialRules, loadSemanticTaxonomy, validateEditorialSelectionResult } from '../src/index.js';
import { FIXED_NOW, hoursAgoIso } from './fixtures/factories.js';

const TAXONOMY = loadSemanticTaxonomy(resolve(process.cwd(), 'config', 'semantic-taxonomy.json'));
const EDITORIAL_RULES = loadEditorialRules(resolve(process.cwd(), 'config', 'editorial-rules.json'));

function withTempDir(callback) {
  const directory = mkdtempSync(join(tmpdir(), 'dibs-phase3-'));
  try {
    return callback(directory);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function mergeMetadata(baseMetadata, overrideMetadata = {}) {
  return {
    ...baseMetadata,
    ...overrideMetadata
  };
}

function makeSemanticCard(overrides = {}) {
  const articleId = overrides.article_id ?? `article-${Math.random().toString(36).slice(2, 10)}`;
  const base = {
    article_id: articleId,
    source_id: 'reuters',
    title: 'Federal Reserve signals caution on rates as inflation cools',
    url: `https://example.com/${articleId}`,
    language: 'en',
    event_type: 'market_signal',
    primary_entities: ['Federal Reserve'],
    secondary_entities: ['US Treasury'],
    geography_primary: 'United States',
    geography_secondary: [],
    topic_labels: ['global_macro'],
    strategic_dimensions: ['capital_markets'],
    candidate_keywords: ['Federal Reserve', 'United States', 'global_macro'],
    factual_summary: 'The Federal Reserve signaled caution on future rate moves as inflation continued to cool.',
    why_it_matters: 'Federal Reserve signals matter for capital-markets positioning because they reset expectations for borrowing costs.',
    novelty_signal: 'new_event',
    user_relevance_signal: 'high',
    confidence_score: 0.84,
    warnings: [],
    metadata: {
      publication_time_utc: hoursAgoIso(4),
      publication_time_local: hoursAgoIso(4),
      extraction_quality_score: 0.9,
      source_priority_tier: 1,
      source_class: 'global_hard_news',
      article_type: 'analysis',
      candidate_disposition: 'main',
      source_display_name: 'Reuters'
    }
  };

  return createSemanticCard({
    ...base,
    ...overrides,
    metadata: mergeMetadata(base.metadata, overrides.metadata)
  }, TAXONOMY);
}

function makeDomainCard(domain, overrides = {}) {
  const blueprints = {
    global_macro: {
      source_id: 'reuters',
      title: 'Federal Reserve signals caution on rates as inflation cools',
      event_type: 'market_signal',
      primary_entities: ['Federal Reserve'],
      secondary_entities: ['US Treasury'],
      geography_primary: 'United States',
      topic_labels: ['global_macro'],
      strategic_dimensions: ['capital_markets'],
      candidate_keywords: ['Federal Reserve', 'United States', 'global_macro'],
      factual_summary: 'The Federal Reserve signaled caution on future rate moves as inflation continued to cool.',
      why_it_matters: 'Federal Reserve signals matter for capital-markets positioning because they reset expectations for borrowing costs.'
    },
    geopolitics: {
      source_id: 'foreign-policy',
      title: 'NATO diplomats weigh new ceasefire proposal after overnight strikes',
      event_type: 'conflict_escalation',
      primary_entities: ['NATO'],
      secondary_entities: ['European Union'],
      geography_primary: 'Europe',
      topic_labels: ['geopolitics'],
      strategic_dimensions: ['geopolitics', 'trade'],
      candidate_keywords: ['NATO', 'Europe', 'geopolitics'],
      factual_summary: 'NATO diplomats weighed a new ceasefire proposal after another round of overnight strikes.',
      why_it_matters: 'NATO and Europe-facing conflict developments affect cross-border risk assumptions and alliance positioning.'
    },
    technology: {
      source_id: 'bloomberg',
      title: 'Nvidia supplier expands AI packaging capacity after export-rule changes',
      event_type: 'policy_move',
      primary_entities: ['Nvidia'],
      secondary_entities: ['TSMC'],
      geography_primary: 'United States',
      topic_labels: ['technology'],
      strategic_dimensions: ['AI_competition', 'digital_infrastructure'],
      candidate_keywords: ['Nvidia', 'United States', 'technology'],
      factual_summary: 'Nvidia suppliers expanded AI packaging capacity after export-rule changes altered production planning.',
      why_it_matters: 'Nvidia-linked capacity shifts matter for AI competition because they can change deployment pace and infrastructure availability.'
    },
    china_economy: {
      source_id: 'caixin',
      title: 'Provincial factories raise investment plans as Beijing targets manufacturing upgrades',
      event_type: 'market_signal',
      primary_entities: ['Beijing'],
      secondary_entities: ['Provincial factories'],
      geography_primary: 'China',
      topic_labels: ['china_economy'],
      strategic_dimensions: ['industrial_policy', 'capital_markets'],
      candidate_keywords: ['Beijing', 'China', 'china_economy'],
      factual_summary: 'Factories raised investment plans as Beijing pushed another round of manufacturing upgrades.',
      why_it_matters: 'Beijing-led factory investment matters for China-facing economic strategy because it changes capacity expectations and capital allocation.'
    },
    climate_transition: {
      source_id: 'bloomberg-green',
      title: 'European utilities accelerate battery and grid plans after new clean-power rules',
      event_type: 'policy_move',
      primary_entities: ['European Commission'],
      secondary_entities: ['European utilities'],
      geography_primary: 'Europe',
      topic_labels: ['climate_transition'],
      strategic_dimensions: ['energy_transition', 'climate_risk'],
      candidate_keywords: ['European Commission', 'Europe', 'climate_transition'],
      factual_summary: 'European utilities accelerated battery and grid plans after new clean-power rules were announced.',
      why_it_matters: 'European utilities and regulatory shifts matter for energy-transition planning because they change project timing and investment assumptions.'
    },
    urban_systems: {
      source_id: 'citylab',
      title: 'New York City backs faster bus-lane rollout after congestion review',
      event_type: 'infrastructure_project',
      primary_entities: ['New York City'],
      secondary_entities: ['MTA'],
      geography_primary: 'United States',
      topic_labels: ['urban_systems'],
      strategic_dimensions: ['urban_governance'],
      candidate_keywords: ['New York City', 'urban_systems', 'transit'],
      factual_summary: 'New York City backed a faster bus-lane rollout after reviewing congestion impacts across the network.',
      why_it_matters: 'New York City transit decisions matter for urban governance because they set the baseline for later mobility and housing tradeoffs.'
    },
    digital_economy: {
      source_id: 'techcrunch',
      title: 'Stripe acquires procurement workflow startup as enterprise software budgets shift',
      event_type: 'funding_or_deal',
      primary_entities: ['Stripe'],
      secondary_entities: ['Enterprise buyers'],
      geography_primary: 'United States',
      topic_labels: ['digital_economy'],
      strategic_dimensions: ['platform_power', 'capital_markets'],
      candidate_keywords: ['Stripe', 'digital_economy', 'platform_power'],
      factual_summary: 'Stripe acquired a procurement workflow startup as enterprise software budgets shifted toward integrated tools.',
      why_it_matters: 'Stripe-led platform deals matter for digital business models because they can reshape how software spending is captured.'
    },
    policy_analysis: {
      source_id: 'brookings',
      title: 'Brookings argues industrial subsidies need tighter accountability rules',
      event_type: 'long_form_analysis',
      primary_entities: ['Brookings'],
      secondary_entities: ['US agencies'],
      geography_primary: 'United States',
      topic_labels: ['policy_analysis'],
      strategic_dimensions: ['regulation', 'industrial_policy'],
      candidate_keywords: ['Brookings', 'policy_analysis', 'regulation'],
      factual_summary: 'Brookings argued that industrial subsidies need tighter accountability rules and clearer performance benchmarks.',
      why_it_matters: 'Brookings analysis matters for regulatory design because it reframes how future industrial-policy support may be evaluated.'
    },
    culture_design: {
      source_id: 'dezeen',
      title: 'London museum architects rethink public exhibition layouts for denser urban audiences',
      event_type: 'long_form_analysis',
      primary_entities: ['London Design Museum'],
      secondary_entities: ['Architects'],
      geography_primary: 'United Kingdom',
      topic_labels: ['culture_design'],
      strategic_dimensions: ['consumer_shift'],
      candidate_keywords: ['London Design Museum', 'culture_design', 'design'],
      factual_summary: 'Architects are rethinking exhibition layouts as museums adapt to denser urban audiences and shifting visitor flows.',
      why_it_matters: 'London museum design choices matter for cultural institutions because they signal how public-space expectations are changing.'
    },
    lifestyle_signals: {
      source_id: 'monocle',
      title: 'Tokyo coffee chains turn to neighborhood formats as commuting patterns keep shifting',
      event_type: 'market_signal',
      primary_entities: ['Tokyo coffee chains'],
      secondary_entities: ['Commuters'],
      geography_primary: 'Japan',
      topic_labels: ['lifestyle_signals'],
      strategic_dimensions: ['consumer_shift'],
      candidate_keywords: ['Tokyo coffee chains', 'Japan', 'lifestyle_signals'],
      factual_summary: 'Tokyo coffee chains turned to neighborhood formats as commuting patterns kept shifting through the quarter.',
      why_it_matters: 'Tokyo retail-format changes matter for consumer behavior because they show how everyday spending patterns are being re-routed.'
    }
  };

  return makeSemanticCard({
    ...blueprints[domain],
    article_id: overrides.article_id ?? `${domain}-${overrides.source_id ?? blueprints[domain].source_id}`,
    ...overrides
  });
}

function buildBalancedCards() {
  return [
    makeDomainCard('global_macro'),
    makeDomainCard('geopolitics'),
    makeDomainCard('technology'),
    makeDomainCard('china_economy'),
    makeDomainCard('climate_transition'),
    makeDomainCard('urban_systems'),
    makeDomainCard('digital_economy'),
    makeDomainCard('policy_analysis'),
    makeDomainCard('culture_design'),
    makeDomainCard('lifestyle_signals')
  ];
}

test('heavy geopolitics day does not let one conflict chain crowd out the selection', () => {
  const redundantConflictCards = Array.from({ length: 5 }, (_, index) => makeSemanticCard({
    article_id: `gaza-chain-${index + 1}`,
    source_id: index % 2 === 0 ? 'reuters' : 'associated-press',
    title: `Ceasefire talks stall after overnight strikes in Gaza ${index + 1}`,
    url: `https://example.com/gaza-chain-${index + 1}`,
    event_type: 'conflict_escalation',
    primary_entities: ['Israel'],
    secondary_entities: ['Hamas', 'Qatar'],
    geography_primary: 'Middle East',
    topic_labels: ['geopolitics'],
    strategic_dimensions: ['geopolitics'],
    candidate_keywords: ['Israel', 'Middle East', 'geopolitics'],
    factual_summary: 'Ceasefire talks stalled after another round of overnight strikes and diplomatic warnings.',
    why_it_matters: 'Middle East conflict developments matter for geopolitical risk because they can alter diplomatic and security assumptions.',
    metadata: {
      publication_time_utc: hoursAgoIso(index + 1),
      extraction_quality_score: 0.86 - (index * 0.04)
    }
  }));

  const result = buildEditorialSelection({
    semanticCards: [
      ...buildBalancedCards().filter((card) => card.topic_labels[0] !== 'geopolitics'),
      ...redundantConflictCards
    ],
    runTimestamp: FIXED_NOW.toISOString()
  });

  const selectedGeopolitics = result.result.selected_items.filter((item) => item.primary_domain === 'geopolitics');
  assert.equal(selectedGeopolitics.length <= 2, true);
  assert.equal(result.result.cluster_cap_report.violations.length, 0);
});

test('source cap limits one source with many high-score articles', () => {
  const cards = buildBalancedCards();
  const sameSourceExtras = [
    makeDomainCard('technology', {
      article_id: 'same-source-tech',
      source_id: 'reuters',
      metadata: { source_priority_tier: 1, extraction_quality_score: 0.94 }
    }),
    makeDomainCard('china_economy', {
      article_id: 'same-source-china',
      source_id: 'reuters',
      metadata: { source_priority_tier: 1, extraction_quality_score: 0.93 }
    }),
    makeDomainCard('climate_transition', {
      article_id: 'same-source-climate',
      source_id: 'reuters',
      metadata: { source_priority_tier: 1, extraction_quality_score: 0.95 }
    })
  ];

  const result = buildEditorialSelection({
    semanticCards: [...cards, ...sameSourceExtras],
    runTimestamp: FIXED_NOW.toISOString()
  });

  const selectedReuters = result.result.selected_items.filter((item) => item.source_id === 'reuters');
  assert.equal(selectedReuters.length <= 2, true);
  assert.equal(result.scoredCandidates.some((candidate) => candidate.exclusion_reason_codes.includes('source_cap_reached')), true);
});

test('cluster cap limits one event cluster with many high-score articles', () => {
  const clusterCards = [
    makeSemanticCard({
      article_id: 'chip-cluster-1',
      source_id: 'bloomberg',
      title: 'Nvidia suppliers rework export plans after new AI chip rule',
      event_type: 'policy_move',
      primary_entities: ['Nvidia'],
      secondary_entities: ['TSMC'],
      geography_primary: 'China',
      topic_labels: ['technology'],
      strategic_dimensions: ['AI_competition'],
      candidate_keywords: ['Nvidia', 'China', 'technology'],
      factual_summary: 'Nvidia suppliers reworked export plans after a new AI chip rule was announced.',
      why_it_matters: 'Nvidia supply-chain adjustments matter for AI competition because they shape how quickly advanced systems can be deployed.'
    }),
    makeSemanticCard({
      article_id: 'chip-cluster-2',
      source_id: 'financial-times',
      title: 'Nvidia partners reassess China packaging schedules after AI chip rule',
      event_type: 'policy_move',
      primary_entities: ['Nvidia'],
      secondary_entities: ['Packaging partners'],
      geography_primary: 'China',
      topic_labels: ['china_economy'],
      strategic_dimensions: ['digital_infrastructure'],
      candidate_keywords: ['Nvidia', 'China', 'china_economy'],
      factual_summary: 'Nvidia partners reassessed China packaging schedules after the AI chip rule changed shipping assumptions.',
      why_it_matters: 'China-facing production adjustments matter for digital infrastructure planning because they affect where advanced packaging capacity lands.'
    }),
    makeSemanticCard({
      article_id: 'chip-cluster-3',
      source_id: 'the-information',
      title: 'Nvidia software buyers rethink data-center contracts after AI chip rule',
      event_type: 'policy_move',
      primary_entities: ['Nvidia'],
      secondary_entities: ['Enterprise buyers'],
      geography_primary: 'China',
      topic_labels: ['digital_economy'],
      strategic_dimensions: ['platform_power'],
      candidate_keywords: ['Nvidia', 'China', 'digital_economy'],
      factual_summary: 'Enterprise buyers rethought data-center contracts after the AI chip rule changed procurement assumptions.',
      why_it_matters: 'Enterprise contract shifts matter for platform economics because they can redirect where AI spending lands.'
    })
  ];

  const result = buildEditorialSelection({
    semanticCards: [
      ...buildBalancedCards().filter((card) => !['technology', 'china_economy', 'digital_economy'].includes(card.topic_labels[0])),
      ...clusterCards
    ],
    runTimestamp: FIXED_NOW.toISOString()
  });

  const selectedFromCluster = result.result.selected_items.filter((item) => item.cluster_id === result.clusters.find((cluster) => cluster.member_article_ids.includes('chip-cluster-1')).cluster_id);
  assert.equal(selectedFromCluster.length <= 2, true);
  assert.equal(result.scoredCandidates.some((candidate) => candidate.article_id === 'chip-cluster-3' && candidate.exclusion_reason_codes.some((reason) => ['cluster_cap_reached', 'duplicate_angle_within_cluster'].includes(reason))), true);
});

test('weak-news day triggers degraded mode', () => {
  const cards = [
    makeDomainCard('global_macro', {
      article_id: 'weak-1',
      confidence_score: 0.48,
      metadata: { extraction_quality_score: 0.52, source_priority_tier: 2 }
    }),
    makeDomainCard('technology', {
      article_id: 'weak-2',
      confidence_score: 0.5,
      metadata: { extraction_quality_score: 0.55, source_priority_tier: 2 }
    }),
    makeDomainCard('policy_analysis', {
      article_id: 'weak-3',
      confidence_score: 0.49,
      metadata: { extraction_quality_score: 0.54, source_priority_tier: 2 }
    })
  ];

  const result = buildEditorialSelection({
    semanticCards: cards,
    runTimestamp: FIXED_NOW.toISOString()
  });

  assert.equal(result.result.degraded_mode, true);
  assert.equal(result.result.under_default_target, false);
  assert.equal(result.result.run_status, 'degraded');
  assert.equal(result.result.selected_count < 8, true);
});

test('selected_article_ids matches the final sorted selected_items order exactly', () => {
  const result = buildEditorialSelection({
    semanticCards: buildBalancedCards(),
    runTimestamp: FIXED_NOW.toISOString()
  });

  assert.deepEqual(
    result.result.selected_article_ids,
    result.result.selected_items.map((item) => item.article_id)
  );
});

test('under-default but above-minimum run is not marked degraded', () => {
  const result = buildEditorialSelection({
    semanticCards: buildBalancedCards().filter((card) => card.topic_labels[0] !== 'lifestyle_signals'),
    runTimestamp: FIXED_NOW.toISOString()
  });

  assert.equal(result.result.selected_count, 9);
  assert.equal(result.result.degraded_mode, false);
  assert.equal(result.result.under_default_target, true);
  assert.equal(result.result.run_status, 'under_default_target');
});

test('editorial result validation fails if selected_article_ids disagrees with final selected_items order', () => {
  const result = buildEditorialSelection({
    semanticCards: buildBalancedCards(),
    runTimestamp: FIXED_NOW.toISOString()
  });

  assert.throws(
    () => validateEditorialSelectionResult({
      ...result.result,
      selected_article_ids: [...result.result.selected_article_ids].reverse()
    }, EDITORIAL_RULES, result.clusters),
    /must match the final ordered "selected_items"/
  );
});

test('backfill logic does not automatically overfill geopolitics', () => {
  const cards = [
    ...buildBalancedCards().filter((card) => !['culture_design', 'lifestyle_signals'].includes(card.topic_labels[0])),
    makeDomainCard('geopolitics', {
      article_id: 'geo-extra-1',
      title: 'European diplomats harden sanctions language after summit impasse',
      source_id: 'reuters'
    }),
    makeDomainCard('geopolitics', {
      article_id: 'geo-extra-2',
      title: 'Regional envoys prepare another sanctions package after summit impasse',
      source_id: 'associated-press'
    })
  ];

  const result = buildEditorialSelection({
    semanticCards: cards,
    runTimestamp: FIXED_NOW.toISOString()
  });

  const geopoliticsCount = result.result.selected_items.filter((item) => item.primary_domain === 'geopolitics').length;
  assert.equal(geopoliticsCount <= 1, true);
});

test('soft-feature review with low relevance is excluded', () => {
  const cards = buildBalancedCards().filter((card) => card.topic_labels[0] !== 'lifestyle_signals');
  const reviewCard = makeDomainCard('lifestyle_signals', {
    article_id: 'soft-review-1',
    title: 'Gadget review: handheld device gets a minor refresh',
    user_relevance_signal: 'low',
    metadata: {
      article_type: 'review',
      extraction_quality_score: 0.72,
      source_priority_tier: 2
    }
  });

  const result = buildEditorialSelection({
    semanticCards: [...cards, reviewCard],
    runTimestamp: FIXED_NOW.toISOString()
  });

  const candidate = result.scoredCandidates.find((entry) => entry.article_id === 'soft-review-1');
  assert.equal(candidate.selected, false);
  assert.equal(candidate.exclusion_reason_codes.includes('low_value_soft_content'), true);
});

test('scored candidates include all score components', () => {
  const result = buildEditorialSelection({
    semanticCards: buildBalancedCards(),
    runTimestamp: FIXED_NOW.toISOString()
  });

  const candidate = result.scoredCandidates[0];
  for (const component of [
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
    assert.equal(component in candidate.score_breakdown, true);
  }
});

test('selection is deterministic under the same input and config', () => {
  const cards = buildBalancedCards();
  const first = buildEditorialSelection({
    semanticCards: cards,
    runTimestamp: FIXED_NOW.toISOString()
  });
  const second = buildEditorialSelection({
    semanticCards: cards,
    runTimestamp: FIXED_NOW.toISOString()
  });

  assert.deepEqual(first.result.selected_article_ids, second.result.selected_article_ids);
  assert.deepEqual(first.scoredCandidates, second.scoredCandidates);
});

test('exclusion reasons are recorded for non-selected candidates', () => {
  const cards = [
    ...buildBalancedCards(),
    makeDomainCard('technology', {
      article_id: 'excluded-low-score',
      confidence_score: 0.42,
      metadata: { extraction_quality_score: 0.4, source_priority_tier: 3 }
    })
  ];

  const result = buildEditorialSelection({
    semanticCards: cards,
    runTimestamp: FIXED_NOW.toISOString()
  });

  const excluded = result.scoredCandidates.find((candidate) => candidate.article_id === 'excluded-low-score');
  assert.equal(excluded.selected, false);
  assert.equal(excluded.exclusion_reason_codes.length > 0, true);
});

test('quota fill report and diagnostics files are generated', () => {
  withTempDir((directory) => {
    const result = buildEditorialSelection({
      semanticCards: buildBalancedCards(),
      runTimestamp: FIXED_NOW.toISOString(),
      outputDir: directory
    });

    assert.equal(existsSync(join(directory, 'cluster_map.json')), true);
    assert.equal(existsSync(join(directory, 'scored_candidates.json')), true);
    assert.equal(existsSync(join(directory, 'editorial_selection_debug.json')), true);
    assert.equal(existsSync(join(directory, 'topic_distribution.json')), true);
    assert.equal('per_domain' in result.result.quota_fill_report, true);

    const debug = JSON.parse(readFileSync(join(directory, 'editorial_selection_debug.json'), 'utf8'));
    assert.equal('quota_fill_status' in debug, true);
    assert.equal('per_source_counts' in debug, true);
    assert.equal('per_source_class_counts' in debug, true);
    assert.equal('exclusion_summary' in debug, true);
    assert.equal(debug.run_status, 'on_target');
    assert.equal(debug.degraded_mode, false);
    assert.equal(debug.under_default_target, false);
  });
});

test('diagnostics reflect under-target-normal run status semantics', () => {
  withTempDir((directory) => {
    const result = buildEditorialSelection({
      semanticCards: buildBalancedCards().filter((card) => card.topic_labels[0] !== 'lifestyle_signals'),
      runTimestamp: FIXED_NOW.toISOString(),
      outputDir: directory
    });

    const debug = JSON.parse(readFileSync(join(directory, 'editorial_selection_debug.json'), 'utf8'));
    assert.equal(result.result.selected_count, 9);
    assert.equal(debug.selected_count, 9);
    assert.equal(debug.degraded_mode, false);
    assert.equal(debug.under_default_target, true);
    assert.equal(debug.run_status, 'under_default_target');
  });
});

test('representative article selection works for a multi-member cluster', () => {
  const result = buildEditorialSelection({
    semanticCards: [
      ...buildBalancedCards().filter((card) => card.topic_labels[0] !== 'technology'),
      makeDomainCard('technology', {
        article_id: 'rep-high-confidence',
        confidence_score: 0.93,
        metadata: { extraction_quality_score: 0.95, source_priority_tier: 1 }
      }),
      makeDomainCard('technology', {
        article_id: 'rep-lower-confidence',
        title: 'Nvidia supplier expands AI packaging capacity after export-rule changes',
        confidence_score: 0.68,
        metadata: { extraction_quality_score: 0.74, source_priority_tier: 2 }
      })
    ],
    runTimestamp: FIXED_NOW.toISOString()
  });

  const cluster = result.clusters.find((entry) => entry.member_article_ids.includes('rep-high-confidence') && entry.member_article_ids.includes('rep-lower-confidence'));
  assert.equal(cluster.representative_article_id, 'rep-high-confidence');
});

test('final selected set respects hard caps', () => {
  const result = buildEditorialSelection({
    semanticCards: [
      ...buildBalancedCards(),
      makeDomainCard('global_macro', {
        article_id: 'macro-duplicate-1',
        source_id: 'bloomberg',
        title: 'Bond traders reset inflation bets after labor data surprise'
      }),
      makeDomainCard('global_macro', {
        article_id: 'macro-duplicate-2',
        source_id: 'ft',
        title: 'Investors reset inflation bets after labor data surprise'
      })
    ],
    runTimestamp: FIXED_NOW.toISOString()
  });

  const domainCounts = result.result.selected_items.reduce((counts, item) => {
    counts[item.primary_domain] = (counts[item.primary_domain] ?? 0) + 1;
    counts[item.source_id] = counts[item.source_id] ?? 0;
    return counts;
  }, {});

  assert.equal(Object.values(result.result.source_cap_report.counts).every((count) => count <= 2), true);
  assert.equal(Object.values(result.result.cluster_cap_report.counts).every((count) => count <= 2), true);
  assert.equal((domainCounts.global_macro ?? 0) <= 2, true);
});

test('editorial selection consumes semantic cards only and rejects backup-derived records', () => {
  assert.throws(
    () => buildEditorialSelection({
      semanticCards: [
        makeDomainCard('global_macro', {
          article_id: 'bad-backup-card',
          metadata: {
            candidate_disposition: 'backup'
          }
        })
      ],
      runTimestamp: FIXED_NOW.toISOString()
    }),
    /Phase 1 mainPool records/
  );
});
