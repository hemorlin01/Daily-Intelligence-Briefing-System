import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { executeDibsRun, formatBriefingOperationalSummary, loadBriefingSummaryReports } from '../src/index.js';
import { FIXED_NOW, makeRawEntry } from './fixtures/factories.js';

const DELIVERY_RULES_PATH = resolve(process.cwd(), 'config', 'delivery-rules.json');

async function withTempDir(callback) {
  const directory = mkdtempSync(join(tmpdir(), 'dibs-round5-'));
  try {
    return await callback(directory);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function buildLongText(seed, count) {
  return Array.from({ length: count }, () => seed).join(' ');
}

function makeFeedRawEntry({ sourceId, title, url, snippet, summary, publishedAt, author }) {
  return makeRawEntry(sourceId, {
    title,
    url,
    canonical_text: '',
    snippet,
    summary,
    published_at: publishedAt,
    author,
    article_type: 'news',
    ingestion_method: 'rss'
  });
}

function buildDeliveryRulesOverride(directory) {
  const base = JSON.parse(readFileSync(DELIVERY_RULES_PATH, 'utf8'));
  base.artifacts.output_root = directory;
  base.artifacts.ledger_path = join(directory, 'delivery-ledger.json');
  const overridePath = join(directory, 'delivery-rules.json');
  writeFileSync(overridePath, JSON.stringify(base, null, 2));
  return overridePath;
}

test('healthy summary-only feed inputs do not collapse into a degraded two-item briefing', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = buildDeliveryRulesOverride(directory);
    const publishedAt = new Date(FIXED_NOW.getTime() - (1 * 36e5)).toISOString();

    const rawItems = [
      makeFeedRawEntry({
        sourceId: 'reuters',
        title: 'Markets watch inflation as rates steady',
        url: 'https://example.com/markets-inflation',
        snippet: buildLongText('Inflation and markets data point to macro shifts.', 12),
        summary: buildLongText('Global economy markets inflation rates trade.', 10),
        publishedAt,
        author: 'Reuters Staff'
      }),
      makeFeedRawEntry({
        sourceId: 'foreign-policy',
        title: 'Sanctions tighten after conflict escalation',
        url: 'https://example.com/sanctions-conflict',
        snippet: buildLongText('Diplomacy sanctions conflict war security.', 12),
        summary: buildLongText('Geopolitics conflict sanctions diplomacy.', 10),
        publishedAt,
        author: 'Foreign Policy'
      }),
      makeFeedRawEntry({
        sourceId: 'the-guardian',
        title: 'AI chip supply shifts as regulators tighten rules',
        url: 'https://example.com/ai-chip-supply',
        snippet: buildLongText('AI chip semiconductor compute policy regulation.', 12),
        summary: buildLongText('Artificial intelligence chips semiconductor policy.', 10),
        publishedAt,
        author: 'Reuters'
      }),
      makeFeedRawEntry({
        sourceId: 'caixin-global',
        title: 'Beijing weighs yuan support for China exporters',
        url: 'https://example.com/china-yuan',
        snippet: buildLongText('China Beijing yuan exports imports trade.', 12),
        summary: buildLongText('China economy Beijing yuan trade.', 10),
        publishedAt,
        author: 'Caixin'
      }),
      makeFeedRawEntry({
        sourceId: 'carbon-brief',
        title: 'Emissions rules accelerate climate transition planning',
        url: 'https://example.com/emissions-rules',
        snippet: buildLongText('Climate emissions renewable energy transition.', 12),
        summary: buildLongText('Climate transition emissions renewable.', 10),
        publishedAt,
        author: 'Carbon Brief'
      }),
      makeFeedRawEntry({
        sourceId: 'the-urbanist',
        title: 'City transit upgrades reshape urban housing plans',
        url: 'https://example.com/city-transit',
        snippet: buildLongText('City urban transit housing infrastructure.', 12),
        summary: buildLongText('Urban systems transit housing city.', 10),
        publishedAt,
        author: 'The Urbanist'
      }),
      makeFeedRawEntry({
        sourceId: 'techcrunch',
        title: 'Payments platform expands ecommerce reach',
        url: 'https://example.com/payments-platform',
        snippet: buildLongText('Platform ecommerce payments digital economy.', 12),
        summary: buildLongText('Digital economy platform ecommerce payments.', 10),
        publishedAt,
        author: 'TechCrunch'
      }),
      makeFeedRawEntry({
        sourceId: 'reuters',
        title: 'Policy analysis weighs new regulation package',
        url: 'https://example.com/policy-analysis',
        snippet: buildLongText('Policy regulation analysis think tank governance.', 12),
        summary: buildLongText('Policy analysis regulation governance.', 10),
        publishedAt,
        author: 'Brookings'
      }),
      makeFeedRawEntry({
        sourceId: 'the-guardian',
        title: 'Design museum architecture rethinks public space',
        url: 'https://example.com/design-museum',
        snippet: buildLongText('Design architecture museum culture.', 12),
        summary: buildLongText('Culture design architecture museum.', 10),
        publishedAt,
        author: 'Magnum Photos'
      }),
      makeFeedRawEntry({
        sourceId: 'techcrunch',
        title: 'Coffee travel trends reshape lifestyle spending',
        url: 'https://example.com/coffee-travel',
        snippet: buildLongText('Travel coffee lifestyle signals consumer shift.', 12),
        summary: buildLongText('Lifestyle travel coffee signals.', 10),
        publishedAt,
        author: 'Monocle'
      })
    ];

    const execution = await executeDibsRun({
      rawItems,
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath,
      dryRun: true,
      channels: []
    });

    const funnelPath = join(execution.outputDir, 'briefing_candidate_funnel.json');
    const funnel = JSON.parse(readFileSync(funnelPath, 'utf8'));

    assert.equal(funnel.raw_items_in, 10);
    assert.ok(funnel.post_filter_items >= funnel.final_selected_items);
    assert.ok(funnel.final_selected_items >= 5);
    assert.notEqual(funnel.final_selected_items, 2);
    assert.equal(funnel.run_status, 'on_target');

    const attributionPath = join(execution.outputDir, 'attribution_audit_report.json');
    const attribution = JSON.parse(readFileSync(attributionPath, 'utf8'));
    const guardian = attribution.audited_items.find((item) => item.source_id === 'the-guardian');
    assert.equal(guardian?.source_display_matches_governed_identity, true);
    assert.equal(guardian?.author_byline, 'Reuters');

    const summary = formatBriefingOperationalSummary({
      ...loadBriefingSummaryReports(execution.outputDir),
      runDir: execution.outputDir
    });
    assert.match(summary, /final selected items: \d+/);

    const contentQuality = JSON.parse(readFileSync(join(execution.outputDir, 'content_quality_report.json'), 'utf8'));
    assert.equal('summary_length_avg' in contentQuality, true);
    assert.equal('why_it_matters_length_avg' in contentQuality, true);

    const languageReport = JSON.parse(readFileSync(join(execution.outputDir, 'final_briefing_language_report.json'), 'utf8'));
    assert.equal('selected_items_by_language' in languageReport, true);
  });
});
