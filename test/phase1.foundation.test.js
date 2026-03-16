import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { RAW_ITEM_INPUT_CONTRACT, buildCandidatePools, loadSourceCatalog, loadThresholds, normalizeRawItem } from '../src/index.js';
import { FIXED_NOW, hoursAgoIso, makeRawEntry, makeRawItem } from './fixtures/factories.js';

const SOURCES_PATH = resolve(process.cwd(), 'config', 'sources.json');
const THRESHOLDS_PATH = resolve(process.cwd(), 'config', 'thresholds.json');
const APPROVED_SOURCE_UNIVERSE_PATH = resolve(process.cwd(), 'test', 'fixtures', 'approved-source-universe.json');

function withTempDir(callback) {
  const directory = mkdtempSync(join(tmpdir(), 'dibs-phase1-'));
  try {
    return callback(directory);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function writeJson(directory, fileName, value) {
  const filePath = join(directory, fileName);
  writeFileSync(filePath, JSON.stringify(value, null, 2));
  return filePath;
}

function loadSourcesFixture() {
  return JSON.parse(readFileSync(SOURCES_PATH, 'utf8'));
}

function loadApprovedSourceUniverse() {
  return JSON.parse(readFileSync(APPROVED_SOURCE_UNIVERSE_PATH, 'utf8'));
}

test('loads the full approved source universe from config', () => {
  const catalog = loadSourceCatalog(SOURCES_PATH);
  const approvedUniverse = loadApprovedSourceUniverse();
  const approvedNames = Object.values(approvedUniverse).flat();
  const configuredNames = Array.from(catalog.sources.values()).map((source) => source.display_name);

  assert.equal(catalog.sources.size, 133);
  assert.equal(new Set(configuredNames).size, 133);
  assert.deepEqual(new Set(configuredNames), new Set(approvedNames));
  assert.equal(Array.from(catalog.sources.values()).every((source) => source.active_status === 'active'), true);
});

test('exports an auditable raw-item input contract', () => {
  assert.equal(RAW_ITEM_INPUT_CONTRACT.boundary_name, 'phase1-normalization-input');
  assert.equal(RAW_ITEM_INPUT_CONTRACT.required_fields.some((field) => field.canonical_field === 'title'), true);
  assert.equal(RAW_ITEM_INPUT_CONTRACT.required_fields.some((field) => field.canonical_field === 'url'), true);
  assert.match(RAW_ITEM_INPUT_CONTRACT.snippet_only_handling, /backup_pool/);
});

test('deduplicates exact duplicate URLs', () => {
  const result = buildCandidatePools({
    rawItems: [
      makeRawEntry('reuters'),
      makeRawEntry('reuters', {
        canonical_text: 'Improved extraction '.repeat(120)
      })
    ],
    now: FIXED_NOW
  });

  assert.equal(result.mainPool.length, 1);
  assert.equal(result.deduplicationReport.actions.length, 1);
  assert.equal(result.deduplicationReport.actions[0].reason, 'exact_url');
});

test('deduplicates near-duplicate headlines', () => {
  const result = buildCandidatePools({
    rawItems: [
      makeRawEntry('bloomberg', {
        title: 'Nvidia supplier plans expansion after AI demand surge',
        url: 'https://www.bloomberg.com/news/articles/2026-03-16/nvidia-supplier-plans-expansion'
      }),
      makeRawEntry('bloomberg', {
        title: 'Nvidia supplier plans expansion amid AI demand surge',
        url: 'https://www.bloomberg.com/news/articles/2026-03-16/nvidia-supplier-expansion-plan'
      })
    ],
    now: FIXED_NOW
  });

  assert.equal(result.mainPool.length, 1);
  assert.equal(result.deduplicationReport.actions[0].reason, 'near_duplicate_title');
});

test('deduplicates syndicated wire duplicates', () => {
  const sharedBody = 'Leaders discussed a ceasefire proposal while diplomats sought support across regional capitals. '.repeat(50);
  const result = buildCandidatePools({
    rawItems: [
      makeRawEntry('reuters', {
        title: 'Diplomats seek support for new ceasefire proposal',
        url: 'https://www.reuters.com/world/diplomats-seek-support-ceasefire-proposal',
        canonical_text: sharedBody,
        is_original_reporting: true
      }),
      makeRawEntry('associated-press', {
        title: 'Diplomats seek backing for new ceasefire proposal',
        url: 'https://apnews.com/article/ceasefire-proposal-backing',
        canonical_text: sharedBody,
        is_syndicated_copy: true
      })
    ],
    now: FIXED_NOW
  });

  assert.equal(result.mainPool.length, 1);
  assert.equal(result.deduplicationReport.actions[0].reason, 'syndicated_duplicate');
  assert.equal(result.mainPool[0].source_id, 'reuters');
});

test('keeps items with missing canonical text when snippet is substantial', () => {
  const result = buildCandidatePools({
    rawItems: [
      makeRawEntry('techcrunch', {
        url: 'https://techcrunch.com/2026/03/16/ai-procurement-tools/',
        canonical_text: '',
        snippet: 'Enterprise buyers are looking for ways to compare AI vendors, procurement rules, contract obligations, and budget risk while spending rises across the sector in 2026.',
        summary: ''
      })
    ],
    now: FIXED_NOW
  });

  assert.equal(result.mainPool.length, 0);
  assert.equal(result.backupPool.length, 1);
  assert.equal(result.rejected.length, 0);
});

test('rejects invalid URLs', () => {
  const result = buildCandidatePools({
    rawItems: [
      makeRawEntry('reuters', {
        url: 'not-a-url'
      })
    ],
    now: FIXED_NOW
  });

  assert.equal(result.rejected.length, 1);
  assert.deepEqual(result.rejected[0].reasons, ['invalid_url']);
});

test('rejects stale items outside the allowed window', () => {
  const result = buildCandidatePools({
    rawItems: [
      makeRawEntry('reuters', {
        published_at: hoursAgoIso(52)
      })
    ],
    now: FIXED_NOW
  });

  assert.equal(result.rejected.length, 1);
  assert.equal(result.rejected[0].reasons[0], 'stale_item');
});

test('rejects items from inactive sources', () => {
  withTempDir((directory) => {
    const sources = loadSourcesFixture();
    const reuters = sources.sources.find((entry) => entry.source_id === 'reuters');
    reuters.active_status = 'inactive';
    const tempSourcesPath = writeJson(directory, 'sources.json', sources);

    const result = buildCandidatePools({
      rawItems: [makeRawEntry('reuters')],
      now: FIXED_NOW,
      sourcesPath: tempSourcesPath
    });

    assert.equal(result.rejected.length, 1);
    assert.equal(result.rejected[0].reasons[0], 'inactive_source');
  });
});

test('routes weak but usable items to the backup pool', () => {
  const result = buildCandidatePools({
    rawItems: [
      makeRawEntry('bloomberg-green', {
        canonical_text: 'Climate policy changed.',
        snippet: 'Climate policy changes could affect investment planning across utilities and industrial operators this year.',
        summary: ''
      })
    ],
    now: FIXED_NOW
  });

  assert.equal(result.mainPool.length, 0);
  assert.equal(result.backupPool.length, 1);
});

test('fails loudly on malformed source config', () => {
  withTempDir((directory) => {
    const badConfigPath = writeJson(directory, 'bad-sources.json', {
      catalog_version: 'broken',
      sources: [
        {
          source_id: 'broken-source',
          display_name: 'Broken Source'
        }
      ]
    });

    assert.throws(
      () => loadSourceCatalog(badConfigPath),
      /missing required field "source_class"/
    );
  });
});

test('fails loudly on duplicate source ids in source config', () => {
  withTempDir((directory) => {
    const sources = loadSourcesFixture();
    sources.sources.push({ ...sources.sources[0] });
    const duplicateConfigPath = writeJson(directory, 'duplicate-sources.json', sources);

    assert.throws(
      () => loadSourceCatalog(duplicateConfigPath),
      /duplicate source_id/
    );
  });
});

test('normalization is deterministic under the same input', () => {
  const catalog = loadSourceCatalog(SOURCES_PATH);
  const rules = loadThresholds(THRESHOLDS_PATH);
  const source = catalog.sources.get('reuters');
  const rawItem = makeRawItem({
    title: 'Original title should remain unchanged',
    published_at: '2026-03-16T08:00:00+08:00'
  });

  const first = normalizeRawItem({
    source,
    rawItem,
    rules,
    fetchedAt: FIXED_NOW.toISOString()
  });
  const second = normalizeRawItem({
    source,
    rawItem,
    rules,
    fetchedAt: FIXED_NOW.toISOString()
  });

  assert.deepEqual(first, second);
  assert.equal(first.title, 'Original title should remain unchanged');
});

test('writes candidate diagnostics files', () => {
  withTempDir((directory) => {
    const result = buildCandidatePools({
      rawItems: [
        makeRawEntry('reuters'),
        makeRawEntry('techcrunch', {
          title: 'Startups are rushing to build AI procurement tools',
          url: 'https://techcrunch.com/2026/03/16/ai-procurement-tools/',
          canonical_text: '',
          snippet: 'Enterprise buyers are looking for ways to compare AI vendors, procurement rules, contract obligations, and budget risk while spending rises across the sector in 2026.',
          summary: ''
        }),
        makeRawEntry('reuters', {
          url: 'not-a-url'
        })
      ],
      now: FIXED_NOW,
      outputDir: directory
    });

    const candidateReportPath = join(directory, 'candidate_pool_report.json');
    const deduplicationPath = join(directory, 'deduplication_report.json');
    const ingestionPath = join(directory, 'ingestion_debug.json');

    assert.equal(result.mainPool.length, 1);
    assert.equal(result.backupPool.length, 1);
    assert.equal(result.rejected.length, 1);
    assert.equal(existsSync(candidateReportPath), true);
    assert.equal(existsSync(deduplicationPath), true);
    assert.equal(existsSync(ingestionPath), true);

    const report = JSON.parse(readFileSync(candidateReportPath, 'utf8'));
    assert.equal(report.configured_source_count, 133);
    assert.equal(report.active_source_count, 133);
    assert.deepEqual(report.configured_source_class_counts, {
      global_hard_news: 16,
      technology_digital_economy: 27,
      policy_geopolitics_institutions: 14,
      business_consulting_insight: 4,
      climate_sustainability: 25,
      urban_infrastructure: 16,
      china_policy_economy: 20,
      culture_design_lifestyle: 7,
      academic_intellectual: 4
    });
    assert.equal(report.main_candidate_pool_size, 1);
    assert.equal(report.backup_pool_size, 1);
    assert.equal(report.rejected_item_count, 1);
    assert.equal(report.total_raw_items_fetched, 3);
    assert.equal(report.per_source_counts.reuters.main, 1);
    assert.equal(report.per_source_counts.techcrunch.backup, 1);
    assert.equal(report.per_source_counts['foreign-policy'].normalized, 0);
    assert.equal(report.per_source_class_counts.global_hard_news.configured_sources, 16);
    assert.equal(report.per_source_class_counts.global_hard_news.normalized_items, 2);
    assert.equal(report.per_source_class_counts.technology_digital_economy.backup_candidates, 1);
  });
});
