import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { buildEditorialSelection, createSemanticCard, loadRenderingRules, loadSemanticTaxonomy, renderBriefing } from '../src/index.js';
import { FIXED_NOW, hoursAgoIso } from './fixtures/factories.js';

const TAXONOMY = loadSemanticTaxonomy(resolve(process.cwd(), 'config', 'semantic-taxonomy.json'));
const RENDERING_RULES_PATH = resolve(process.cwd(), 'config', 'rendering-rules.json');

function withTempDir(callback) {
  const directory = mkdtempSync(join(tmpdir(), 'dibs-rendering-'));
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
  const articleId = overrides.article_id ?? 'article-default';
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
    factual_summary: 'The Federal Reserve signaled caution on future rate moves as inflation continued to cool across the quarter.',
    why_it_matters: 'Federal Reserve signals matter for capital-markets positioning because they reset expectations for borrowing costs and risk appetite.',
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
      factual_summary: 'The Federal Reserve signaled caution on future rate moves as inflation continued to cool across the quarter.',
      why_it_matters: 'Federal Reserve signals matter for capital-markets positioning because they reset expectations for borrowing costs and risk appetite.',
      metadata: { source_display_name: 'Reuters' }
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
      factual_summary: 'NATO diplomats weighed a new ceasefire proposal after another round of overnight strikes and alliance consultations.',
      why_it_matters: 'NATO and Europe-facing conflict developments affect cross-border risk assumptions and alliance positioning.',
      metadata: { source_display_name: 'Foreign Policy' }
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
      factual_summary: 'Nvidia suppliers expanded AI packaging capacity after export-rule changes altered production planning and deployment timing.',
      why_it_matters: 'Nvidia-linked capacity shifts matter for AI competition because they can change deployment pace and infrastructure availability.',
      metadata: { source_display_name: 'Bloomberg' }
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
      factual_summary: 'Factories raised investment plans as Beijing pushed another round of manufacturing upgrades through regional industrial plans.',
      why_it_matters: 'Beijing-led factory investment matters for China-facing economic strategy because it changes capacity expectations and capital allocation.',
      metadata: { source_display_name: 'Caixin' }
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
      factual_summary: 'European utilities accelerated battery and grid plans after new clean-power rules were announced across multiple markets.',
      why_it_matters: 'European utilities and regulatory shifts matter for energy-transition planning because they change project timing and investment assumptions.',
      metadata: { source_display_name: 'Bloomberg Green' }
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
      factual_summary: 'New York City backed a faster bus-lane rollout after reviewing congestion impacts across the transit network.',
      why_it_matters: 'New York City transit decisions matter for urban governance because they set the baseline for later mobility and housing tradeoffs.',
      metadata: { source_display_name: 'CityLab' }
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
      factual_summary: 'Stripe acquired a procurement workflow startup as enterprise software budgets shifted toward integrated tools and tighter buying cycles.',
      why_it_matters: 'Stripe-led platform deals matter for digital business models because they can reshape how software spending is captured.',
      metadata: { source_display_name: 'TechCrunch' }
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
      factual_summary: 'Brookings argued that industrial subsidies need tighter accountability rules and clearer performance benchmarks for agencies.',
      why_it_matters: 'Brookings analysis matters for regulatory design because it reframes how future industrial-policy support may be evaluated.',
      metadata: { source_display_name: 'Brookings' }
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
      why_it_matters: 'London museum design choices matter for cultural institutions because they signal how public-space expectations are changing.',
      metadata: { source_display_name: 'Dezeen' }
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
      factual_summary: 'Tokyo coffee chains turned to neighborhood formats as commuting patterns kept shifting through the quarter and footfall changed.',
      why_it_matters: 'Tokyo retail-format changes matter for consumer behavior because they show how everyday spending patterns are being re-routed.',
      metadata: { source_display_name: 'Monocle' }
    }
  };

  const blueprint = blueprints[domain];
  return makeSemanticCard({
    ...blueprint,
    article_id: overrides.article_id ?? `${domain}-${blueprint.source_id}`,
    ...overrides
  });
}

function buildSelectedResult(domains = ['global_macro', 'technology', 'china_economy', 'climate_transition', 'urban_systems', 'digital_economy', 'policy_analysis', 'culture_design', 'lifestyle_signals']) {
  const semanticCards = domains.map((domain, index) => makeDomainCard(domain, {
    article_id: `${domain}-${index + 1}`,
    url: `https://example.com/${domain}-${index + 1}`
  }));
  return buildEditorialSelection({
    semanticCards,
    runTimestamp: FIXED_NOW.toISOString()
  }).result;
}

test('rendering preserves selected-item order inside blocks and output contracts', () => {
  const selectionResult = buildSelectedResult();
  const rendered = renderBriefing({
    selectionResult
  });
  const flattenedBlockOrder = rendered.blocks.flatMap((block) => block.entry_article_ids);

  assert.deepEqual(rendered.email.entry_article_ids, flattenedBlockOrder);
  assert.deepEqual(rendered.markdown.entry_article_ids, flattenedBlockOrder);
  for (const block of rendered.blocks) {
    const selectedIndexes = block.entry_article_ids.map((articleId) => selectionResult.selected_article_ids.indexOf(articleId));
    assert.deepEqual([...selectedIndexes].sort((left, right) => left - right), selectedIndexes);
  }
});

test('rendering block grouping is deterministic', () => {
  const selectionResult = buildSelectedResult();
  const first = renderBriefing({ selectionResult });
  const second = renderBriefing({ selectionResult });

  assert.deepEqual(first.blocks, second.blocks);
});

test('email renderer includes all selected items', () => {
  const selectionResult = buildSelectedResult();
  const rendered = renderBriefing({ selectionResult });

  assert.equal(rendered.email.entry_article_ids.length, selectionResult.selected_count);
  for (const item of selectionResult.selected_items) {
    assert.equal(rendered.email.content.includes(item.title), true);
    assert.equal(rendered.email.content.includes(item.source_display_name), true);
    assert.equal(rendered.email.content.includes('Keywords:'), false);
  }
});

test('headlines are preserved and source/byline are rendered separately', () => {
  const selectionResult = buildSelectedResult();
  const rendered = renderBriefing({ selectionResult });

  for (const item of selectionResult.selected_items) {
    assert.equal(rendered.email.content.includes(`${item.title} - ${item.source_display_name}`), false);
    assert.equal(rendered.email.content.includes(`${item.title} | ${item.source_display_name}`), false);
    assert.equal(rendered.telegram.content.includes(`${item.title} - ${item.source_display_name}`), false);
    assert.equal(rendered.telegram.content.includes(`${item.title} | ${item.source_display_name}`), false);
    assert.equal(rendered.telegram.content.includes(`${item.title} — ${item.source_display_name}`), false);
    assert.equal(rendered.email.content.includes(`Source: ${item.source_display_name}`), true);
  }
});

test('telegram and markdown outputs omit keywords', () => {
  const selectionResult = buildSelectedResult();
  const rendered = renderBriefing({ selectionResult });

  assert.equal(rendered.telegram.content.includes('Keywords:'), false);
  assert.equal(rendered.markdown.content.includes('Keywords:'), false);
});

test('rendered sections remain coherent for compact briefings', () => {
  const selectionResult = buildSelectedResult([
    'china_economy',
    'technology',
    'climate_transition',
    'global_macro',
    'policy_analysis',
    'urban_systems'
  ]);
  const rendered = renderBriefing({ selectionResult });

  assert.equal(rendered.blocks.length <= 5, true);
});

test('mixed-language briefing preserves Chinese and English content', () => {
  const englishCard = makeSemanticCard({
    article_id: 'en-1',
    title: 'AI suppliers raise capacity guidance',
    language: 'en',
    topic_labels: ['technology'],
    factual_summary: 'Suppliers raised capacity guidance as export rules shifted and deployment plans were updated across the quarter.',
    why_it_matters: 'AI supplier capacity matters for technology competition because it shapes deployment pace and pricing expectations.',
    metadata: { source_display_name: 'Reuters' }
  });
  const chineseCard = makeSemanticCard({
    article_id: 'zh-1',
    source_id: 'caixin',
    title: '中国制造业投资继续扩张',
    language: 'zh',
    topic_labels: ['china_economy'],
    factual_summary: '中国制造业投资继续扩张，地方政策加快推进重点项目。',
    why_it_matters: '这对中国经济与政策走向很关键，因为政策边界正在调整。',
    metadata: { source_display_name: 'Caixin' }
  });

  const selectionResult = buildEditorialSelection({
    semanticCards: [englishCard, chineseCard],
    runTimestamp: FIXED_NOW.toISOString()
  }).result;
  const rendered = renderBriefing({ selectionResult });

  assert.match(rendered.email.content, /中国制造业投资继续扩张/);
  assert.match(rendered.email.content, /AI suppliers raise capacity guidance/);
});

test('markdown renderer includes all selected items', () => {
  const selectionResult = buildSelectedResult();
  const rendered = renderBriefing({ selectionResult });

  assert.equal(rendered.markdown.entry_article_ids.length, selectionResult.selected_count);
  for (const item of selectionResult.selected_items) {
    assert.equal(rendered.markdown.content.includes(item.article_id), true);
    assert.equal(rendered.markdown.content.includes(item.title), true);
  }
});

test('telegram renderer respects configured length budget', () => {
  withTempDir((directory) => {
    const baseRules = JSON.parse(readFileSync(RENDERING_RULES_PATH, 'utf8'));
    baseRules.telegram.length_budget_chars = 900;
    const tempRulesPath = join(directory, 'rendering-rules.json');
    writeFileSync(tempRulesPath, JSON.stringify(baseRules, null, 2));

    const selectionResult = buildSelectedResult([
      'global_macro',
      'technology',
      'china_economy',
      'climate_transition',
      'policy_analysis',
      'lifestyle_signals'
    ]);
    const rendered = renderBriefing({
      selectionResult,
      renderingRulesPath: tempRulesPath
    });

    assert.equal(rendered.telegram.content.length <= 900, true);
  });
});

test('telegram renderer logs truncation or omission when needed', () => {
  withTempDir((directory) => {
    const baseRules = JSON.parse(readFileSync(RENDERING_RULES_PATH, 'utf8'));
    baseRules.telegram.length_budget_chars = 650;
    const tempRulesPath = join(directory, 'rendering-rules.json');
    writeFileSync(tempRulesPath, JSON.stringify(baseRules, null, 2));

    const selectionResult = buildSelectedResult();
    const rendered = renderBriefing({
      selectionResult,
      renderingRulesPath: tempRulesPath
    });

    assert.equal(
      rendered.diagnostics.compaction_counts.telegram_total_count > 0 || rendered.telegram.omitted_article_ids.length > 0,
      true
    );
    assert.equal(
      ['compacted_to_fit', 'omitted_entries_to_fit'].includes(rendered.diagnostics.telegram.length_budget_status),
      true
    );
  });
});

test('run-status is surfaced consistently in outputs and diagnostics', () => {
  const selectionResult = buildSelectedResult([
    'global_macro',
    'technology',
    'china_economy',
    'climate_transition',
    'urban_systems',
    'digital_economy',
    'policy_analysis',
    'lifestyle_signals',
    'culture_design'
  ]);
  const rendered = renderBriefing({ selectionResult });

  assert.equal(selectionResult.run_status, 'under_default_target');
  assert.equal(rendered.email.content.includes('Status: Under default target'), true);
  assert.equal(rendered.telegram.content.includes('Status: Under default target'), true);
  assert.equal(rendered.markdown.content.includes('Run status: Under default target'), true);
  assert.equal(rendered.diagnostics.run_status, 'under_default_target');
});

test('degraded runs render without breaking format contracts', () => {
  const selectionResult = buildSelectedResult(['global_macro', 'technology', 'policy_analysis']);
  const rendered = renderBriefing({ selectionResult });

  assert.equal(selectionResult.degraded_mode, true);
  assert.equal(rendered.email.entry_article_ids.length, selectionResult.selected_count);
  assert.equal(rendered.markdown.entry_article_ids.length, selectionResult.selected_count);
});

test('rendering does not mutate editorial selection data', () => {
  const selectionResult = buildSelectedResult();
  const snapshot = JSON.stringify(selectionResult);
  renderBriefing({ selectionResult });

  assert.equal(JSON.stringify(selectionResult), snapshot);
});

test('renderers fail clearly if required selected-set fields are missing', () => {
  const selectionResult = buildSelectedResult();
  const badSelectionResult = {
    ...selectionResult,
    selected_items: selectionResult.selected_items.map((item, index) => (index === 0 ? {
      ...item,
      source_display_name: ''
    } : item))
  };

  assert.throws(
    () => renderBriefing({ selectionResult: badSelectionResult }),
    /source_display_name/
  );
});

test('rendering artifacts and diagnostics are generated', () => {
  withTempDir((directory) => {
    const selectionResult = buildSelectedResult();
    const rendered = renderBriefing({
      selectionResult,
      outputDir: directory
    });

    assert.equal(existsSync(join(directory, 'briefing_email.txt')), true);
    assert.equal(existsSync(join(directory, 'briefing_telegram.txt')), true);
    assert.equal(existsSync(join(directory, 'briefing_archive.md')), true);
    assert.equal(existsSync(join(directory, 'rendering_debug.json')), true);

    const diagnostics = JSON.parse(readFileSync(join(directory, 'rendering_debug.json'), 'utf8'));
    assert.equal(diagnostics.run_status, selectionResult.run_status);
    assert.equal(diagnostics.per_format_item_counts.email, selectionResult.selected_count);
    assert.equal(rendered.diagnostics.omitted_block_count >= 0, true);
  });
});

test('rendering rules config loads with expected block mapping', () => {
  const rules = loadRenderingRules(RENDERING_RULES_PATH);

  assert.equal(rules.blocks.domain_to_block.global_macro, 'markets_policy');
  assert.equal(rules.blocks.domain_to_block.geopolitics, 'china_geopolitics');
});
