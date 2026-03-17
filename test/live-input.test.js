import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import {
  buildFeedInventory,
  executeDibsRun,
  formatLiveInputOperationalSummary,
  formatLiveInputRuntimeTable,
  generateLiveInputArtifacts,
  loadLiveInputRules,
  loadFeedOverrides,
  loadSourceCatalog
} from '../src/index.js';
import { FIXED_NOW } from './fixtures/factories.js';

async function withTempDir(callback) {
  const directory = mkdtempSync(join(tmpdir(), 'dibs-live-input-'));
  try {
    return await callback(directory);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function buildSampleRss({
  title = 'Sample feed title',
  url = 'https://example.com/article',
  description = 'Sample feed description',
  content = '',
  publishedAt = 'Mon, 16 Mar 2026 10:00:00 GMT',
  author = 'Feed Reporter'
} = {}) {
  const contentTag = content
    ? `<content:encoded><![CDATA[${content}]]></content:encoded>`
    : '';
  const pubDateTag = publishedAt ? `<pubDate>${publishedAt}</pubDate>` : '';

  return `<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:dc="http://purl.org/dc/elements/1.1/">
      <channel>
        <title>Sample Feed</title>
        <item>
          <title><![CDATA[${title}]]></title>
          <link>${url}</link>
          <description><![CDATA[${description}]]></description>
          ${contentTag}
          ${pubDateTag}
          <dc:creator><![CDATA[${author}]]></dc:creator>
          <guid>${url}</guid>
        </item>
      </channel>
    </rss>`;
}

function buildSampleAtom({
  title = 'Atom headline',
  url = 'https://example.com/atom-article',
  summary = 'Atom summary text.',
  content = '',
  updatedAt = '2026-03-16T10:00:00Z',
  author = 'Atom Reporter'
} = {}) {
  return `<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <title>Sample Atom Feed</title>
      <entry>
        <title>${title}</title>
        <link rel="alternate" href="${url}" />
        <id>${url}</id>
        <updated>${updatedAt}</updated>
        <summary type="html"><![CDATA[${summary}]]></summary>
        <content type="html"><![CDATA[${content}]]></content>
        <author><name>${author}</name></author>
      </entry>
    </feed>`;
}

function buildHtmlListing() {
  return `
    <html>
      <body>
        <article class="post">
          <a class="headline" href="https://example.com/listing-one">Listing One</a>
          <p class="summary">Structured summary one.</p>
          <time datetime="2026-03-16T09:00:00Z">2026-03-16</time>
        </article>
        <article class="post">
          <a class="headline" href="https://example.com/listing-two">Listing Two</a>
          <p class="summary">Structured summary two.</p>
          <time datetime="2026-03-16T08:00:00Z">2026-03-16</time>
        </article>
      </body>
    </html>`;
}

function buildInventoryRecord(sourceId, overrides = {}) {
  const feedUrl = overrides.feed_url ?? `https://feeds.example.com/${sourceId}.xml`;
  const feedDefinitions = overrides.feed_definitions ?? overrides.feeds ?? [
    {
      feed_id: 'home',
      label: 'Home',
      url: feedUrl,
      format: overrides.feed_format ?? 'rss',
      adapter_type: overrides.adapter_type ?? 'xml_feed',
      content_mode: overrides.content_mode ?? 'summary_only',
      expected_entry_type: overrides.expected_entry_type ?? 'news',
      active_status: 'active',
      source_id: sourceId,
      patterns: overrides.patterns ?? null
    }
  ];

  return {
    source_id: sourceId,
    canonical_source_key: sourceId,
    display_name: overrides.display_name ?? sourceId,
    source_name: overrides.display_name ?? sourceId,
    source_class: overrides.source_class ?? 'global_hard_news',
    language: overrides.language ?? 'en',
    region: overrides.region ?? 'global',
    primary_region: overrides.region ?? 'global',
    active_status: overrides.active_status ?? 'active',
    priority_tier: overrides.priority_tier ?? 2,
    fetch_method: overrides.fetch_method ?? 'rss',
    parser_type: overrides.parser_type ?? 'rss',
    ingestion_method: overrides.ingestion_method ?? 'rss',
    adapter_type: overrides.adapter_type ?? 'xml_feed',
    paywall_policy: overrides.paywall_policy ?? 'free',
    expected_article_type: overrides.expected_article_type ?? 'news',
    feed_support_status: overrides.feed_support_status ?? 'supported',
    support_level: overrides.support_level ?? 'validated_public_feed',
    notes: overrides.notes ?? 'test feed',
    feed_notes: overrides.notes ?? 'test feed',
    feed_url: feedDefinitions[0]?.url ?? null,
    feed_format: feedDefinitions[0]?.format ?? 'none',
    expected_entry_type: feedDefinitions[0]?.expected_entry_type ?? 'news',
    feed_definitions: feedDefinitions,
    feeds: feedDefinitions,
    feed_count: feedDefinitions.length
  };
}

function writeTempDeliveryRules(directory) {
  const rules = JSON.parse(readFileSync(resolve(process.cwd(), 'config', 'delivery-rules.json'), 'utf8'));
  const merged = {
    ...rules,
    artifacts: {
      ...rules.artifacts,
      output_root: join(directory, 'runs'),
      ledger_path: join(directory, 'state', 'delivery-ledger.json')
    },
    delivery: {
      email: {
        ...rules.delivery.email,
        mode: 'local-file'
      },
      telegram: {
        ...rules.delivery.telegram,
        mode: 'local-file'
      }
    }
  };

  const path = join(directory, 'delivery-rules.json');
  writeFileSync(path, JSON.stringify(merged, null, 2));
  return path;
}

test('full governed source inventory includes every Description source and every source has explicit ingestion metadata', () => {
  const inventory = buildFeedInventory();
  const sourceCatalog = loadSourceCatalog(resolve(process.cwd(), 'config', 'sources.json'));
  const overrides = loadFeedOverrides(resolve(process.cwd(), 'config', 'feed-overrides.json'));

  assert.equal(inventory.length, sourceCatalog.sources.size);
  assert.equal(Object.keys(overrides.sources).length, sourceCatalog.sources.size);

  for (const source of inventory) {
    assert.equal(typeof source.ingestion_method, 'string');
    assert.equal(typeof source.adapter_type, 'string');
    assert.equal(typeof source.support_level, 'string');
    assert.equal(typeof source.notes, 'string');
    assert.equal(Array.isArray(source.feed_definitions), true);
  }
});

test('inventory preserves explicit supported, partial, restricted, unsupported, and pending_review statuses', () => {
  const inventory = buildFeedInventory();

  assert.equal(inventory.find((source) => source.source_id === 'cnbc')?.feed_support_status, 'supported');
  assert.equal(inventory.find((source) => source.source_id === 'the-verge')?.feed_support_status, 'partial');
  assert.equal(inventory.find((source) => source.source_id === 'reuters')?.feed_support_status, 'restricted');
  assert.equal(inventory.find((source) => source.source_id === 'stratechery')?.feed_support_status, 'unsupported');
  assert.equal(inventory.find((source) => source.source_id === 'dao-insights')?.feed_support_status, 'pending_review');
});

test('live-input rules load with a valid success ladder', () => {
  const rules = loadLiveInputRules(resolve(process.cwd(), 'config', 'live-input-rules.json'));

  assert.equal(Array.isArray(rules.success_ladder_source_ids), true);
  assert.equal(rules.success_ladder_source_ids.includes('bbc-news'), true);
  assert.equal(rules.fetch.max_attempts >= 1, true);
});

test('sources with active feed definitions do not keep source-level ingestion_method as none', () => {
  const inventory = buildFeedInventory();
  const inconsistent = inventory.filter((source) => source.feed_definitions.length > 0 && source.ingestion_method === 'none');

  assert.deepEqual(inconsistent, []);
  assert.equal(inventory.find((source) => source.source_id === 'works-in-progress')?.ingestion_method, 'atom');
  assert.equal(inventory.find((source) => source.source_id === 'brookings')?.ingestion_method, 'rss');
});

test('endpoint truth-pass downgraded clearly weak configured endpoints', () => {
  const inventory = buildFeedInventory();

  for (const sourceId of ['carnegie-endowment', 'council-on-foreign-relations', 'caixin-global']) {
    const source = inventory.find((entry) => entry.source_id === sourceId);
    assert.equal(source?.support_status, 'pending_review');
    assert.equal(source?.feed_definitions.length, 0);
  }
});

test('feed override validation fails on invalid support status', async () => {
  await withTempDir(async (directory) => {
    const path = join(directory, 'feed-overrides.json');
    writeFileSync(path, JSON.stringify({
      defaults: {
        support_status_by_fetch_method: { rss: 'supported' },
        default_notes_by_status: { supported: 'ok' },
        support_level_by_status: { supported: 'validated_public_feed' },
        ingestion_method_defaults_by_fetch_method: { rss: 'rss' },
        adapter_type_defaults_by_fetch_method: { rss: 'xml_feed' }
      },
      sources: {
        example: {
          feed_support_status: 'made_up_status'
        }
      }
    }));

    assert.throws(() => loadFeedOverrides(path), /feed_support_status/);
  });
});

test('supported RSS feeds parse into raw items and summary-only entries remain honest', async () => {
  await withTempDir(async (directory) => {
    const inventory = [
      buildInventoryRecord('techcrunch', {
        display_name: 'TechCrunch',
        source_class: 'technology_digital_economy'
      })
    ];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => buildSampleRss({
          title: 'Tech infrastructure financing expands',
          url: 'https://example.com/tech-infra',
          description: 'Feed-provided summary only.',
          content: ''
        })
      })
    });

    assert.equal(result.rawItems.length, 1);
    assert.equal(result.rawItems[0].item.title, 'Tech infrastructure financing expands');
    assert.equal(result.rawItems[0].item.canonical_text, '');
    assert.equal(result.rawItems[0].item.summary, 'Feed-provided summary only.');
    assert.deepEqual(result.rawItems[0].item.generator_warnings, ['feed_summary_only']);
    assert.equal(result.rawItems[0].item.ingestion_method, 'rss');
  });
});

test('validation ledger is written and distinguishes configured support from runtime validation', async () => {
  await withTempDir(async (directory) => {
    const inventory = [buildInventoryRecord('techcrunch', { feed_support_status: 'supported' })];
    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: false,
        status: 503,
        text: async () => 'unavailable'
      })
    });

    assert.equal(existsSync(result.paths.validationLedger), true);
    assert.equal(result.reports.validationLedger.sources[0].configured_status, 'supported');
    assert.equal(result.reports.validationLedger.sources[0].validation_status, 'fetch_failed');
    assert.equal(result.reports.validationLedger.feeds[0].validation_status, 'fetch_failed');
  });
});

test('fetch layer sends feed-friendly headers and follows redirects', async () => {
  await withTempDir(async (directory) => {
    const inventory = [buildInventoryRecord('techcrunch', { feed_support_status: 'supported' })];
    const seenRequests = [];

    await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async (url, options) => {
        seenRequests.push({ url, options });
        return {
          ok: true,
          status: 200,
          redirected: true,
          url: 'https://feeds.example.com/final.xml',
          headers: {
            get(name) {
              return name.toLowerCase() === 'content-type' ? 'application/rss+xml; charset=utf-8' : null;
            }
          },
          arrayBuffer: async () => Buffer.from(buildSampleRss())
        };
      }
    });

    assert.equal(seenRequests.length, 1);
    assert.equal(seenRequests[0].options.redirect, 'follow');
    assert.match(seenRequests[0].options.headers['user-agent'], /DIBS/);
    assert.match(seenRequests[0].options.headers.accept, /rss\+xml/);
    assert.match(seenRequests[0].options.headers['accept-language'], /en-US/);
  });
});

test('timeout failures are classified precisely', async () => {
  await withTempDir(async (directory) => {
    const inventory = [buildInventoryRecord('techcrunch')];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      timeoutMs: 5,
      fetchImpl: async (_url, options) => new Promise((_, reject) => {
        options.signal.addEventListener('abort', () => {
          const error = new DOMException('This operation was aborted', 'AbortError');
          reject(error);
        });
      })
    });

    assert.equal(result.reports.fetch.feeds[0].failure_category, 'timeout');
    assert.equal(result.reports.validationLedger.sources[0].last_error_category, 'timeout');
  });
});

test('validation ledger clears current error fields after a successful run', async () => {
  await withTempDir(async (directory) => {
    const inventory = [buildInventoryRecord('techcrunch')];

    await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: new Date('2026-03-16T08:00:00Z'),
      fetchImpl: async () => ({
        ok: false,
        status: 503,
        text: async () => 'unavailable'
      })
    });

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: new Date('2026-03-16T10:00:00Z'),
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        headers: {
          get(name) {
            return name.toLowerCase() === 'content-type' ? 'application/rss+xml; charset=utf-8' : null;
          }
        },
        arrayBuffer: async () => Buffer.from(buildSampleRss({
          title: 'AI procurement platform raises funding',
          url: 'https://example.com/funding',
          description: 'Funding rounds shift procurement decisions.',
          content: 'Funding rounds shift procurement decisions and platform economics. '.repeat(10)
        }))
      })
    });

    const feedLedger = result.reports.validationLedger.feeds[0];
    const sourceLedger = result.reports.validationLedger.sources[0];

    assert.equal(feedLedger.validation_status, 'validated_raw_items');
    assert.equal(feedLedger.last_error_code, null);
    assert.equal(sourceLedger.validation_status, 'validated_raw_items');
    assert.equal(sourceLedger.last_error_code, null);
    assert.equal(sourceLedger.last_failure_code, 'HTTP_5XX');
  });
});

test('content-type tolerance allows XML bodies served with text/plain', async () => {
  await withTempDir(async (directory) => {
    const inventory = [buildInventoryRecord('techcrunch')];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        url: 'https://feeds.example.com/plain.xml',
        redirected: false,
        headers: {
          get(name) {
            return name.toLowerCase() === 'content-type' ? 'text/plain; charset=utf-8' : null;
          }
        },
        arrayBuffer: async () => Buffer.from(buildSampleRss())
      })
    });

    assert.equal(result.rawItems.length, 1);
    assert.equal(result.reports.fetch.feeds[0].status, 'success');
  });
});

test('Atom feeds parse into raw items', async () => {
  await withTempDir(async (directory) => {
    const inventory = [
      buildInventoryRecord('the-register', {
        display_name: 'The Register',
        source_class: 'technology_digital_economy',
        feed_format: 'atom',
        feed_definitions: [
          {
            feed_id: 'headlines',
            label: 'Headlines',
            url: 'https://feeds.example.com/the-register.atom',
            format: 'atom',
            adapter_type: 'xml_feed',
            content_mode: 'mixed',
            expected_entry_type: 'news',
            active_status: 'active',
            source_id: 'the-register'
          }
        ]
      })
    ];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => buildSampleAtom({
          title: 'Atom entry title',
          url: 'https://example.com/atom-entry',
          summary: 'Atom summary text',
          content: 'This atom entry contains enough body text to survive mixed-content handling. '.repeat(6)
        })
      })
    });

    assert.equal(result.rawItems.length, 1);
    assert.equal(result.rawItems[0].item.title, 'Atom entry title');
    assert.equal(result.rawItems[0].item.url, 'https://example.com/atom-entry');
    assert.equal(result.rawItems[0].item.ingestion_method, 'atom');
    assert.match(result.rawItems[0].item.canonical_text, /enough body text/);
  });
});

test('html listing adapter dispatch normalizes governed page entries', async () => {
  await withTempDir(async (directory) => {
    const inventory = [
      buildInventoryRecord('platformer', {
        display_name: 'Platformer',
        source_class: 'technology_digital_economy',
        feed_support_status: 'partial',
        ingestion_method: 'html_listing',
        adapter_type: 'html_listing',
        feed_definitions: [
          {
            feed_id: 'archive',
            label: 'Archive',
            url: 'https://example.com/archive',
            format: 'html',
            adapter_type: 'html_listing',
            content_mode: 'summary_only',
            expected_entry_type: 'analysis',
            active_status: 'active',
            source_id: 'platformer',
            patterns: {
              entry_pattern: '<article class=\"post\">([\\s\\S]*?)<\\/article>',
              link_pattern: '<a class=\"headline\" href=\"([^\"]+)\"',
              title_pattern: '<a class=\"headline\" href=\"[^\"]+\">([\\s\\S]*?)<\\/a>',
              summary_pattern: '<p class=\"summary\">([\\s\\S]*?)<\\/p>',
              date_pattern: 'datetime=\"([^\"]+)\"'
            }
          }
        ]
      })
    ];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => buildHtmlListing()
      })
    });

    assert.equal(result.rawItems.length, 2);
    assert.equal(result.rawItems[0].item.url, 'https://example.com/listing-one');
    assert.equal(result.rawItems[1].item.title, 'Listing Two');
    assert.equal(result.rawItems[0].item.ingestion_method, 'html_listing');
  });
});

test('malformed feed responses fail cleanly', async () => {
  await withTempDir(async (directory) => {
    const inventory = [buildInventoryRecord('techcrunch')];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => '<rss><channel><item><title>Broken'
      })
    });

    assert.equal(result.rawItems.length, 0);
    assert.equal(result.reports.fetch.parse_failures, 1);
    assert.equal(result.reports.fetch.feeds[0].status, 'parse_failed');
  });
});

test('html landing pages are classified as non_xml_response instead of vague parser failure', async () => {
  await withTempDir(async (directory) => {
    const inventory = [buildInventoryRecord('brookings')];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        url: 'https://www.brookings.edu/',
        redirected: true,
        headers: {
          get(name) {
            return name.toLowerCase() === 'content-type' ? 'text/html; charset=utf-8' : null;
          }
        },
        arrayBuffer: async () => Buffer.from('<!DOCTYPE html><html><body>Homepage</body></html>')
      })
    });

    assert.equal(result.reports.fetch.parse_failures, 1);
    assert.equal(result.reports.fetch.feeds[0].failure_category, 'non_xml_response');
  });
});

test('missing published date is preserved honestly with a generator warning', async () => {
  await withTempDir(async (directory) => {
    const inventory = [buildInventoryRecord('techcrunch')];
    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => buildSampleRss({
          title: 'No published date',
          url: 'https://example.com/no-date',
          description: 'A feed entry without a publication timestamp.',
          publishedAt: ''
        })
      })
    });

    assert.equal(result.rawItems.length, 1);
    assert.equal(result.rawItems[0].item.published_at, '');
    assert.equal(result.rawItems[0].item.generator_warnings.includes('missing_publication_time'), true);
  });
});

test('duplicate raw items are removed by source and URL', async () => {
  await withTempDir(async (directory) => {
    const inventory = [
      buildInventoryRecord('techcrunch', {
        feed_definitions: [
          {
            feed_id: 'home',
            label: 'Home',
            url: 'https://feeds.example.com/one.xml',
            format: 'rss',
            adapter_type: 'xml_feed',
            content_mode: 'summary_only',
            expected_entry_type: 'news',
            active_status: 'active',
            source_id: 'techcrunch'
          },
          {
            feed_id: 'duplicates',
            label: 'Duplicates',
            url: 'https://feeds.example.com/two.xml',
            format: 'rss',
            adapter_type: 'xml_feed',
            content_mode: 'mixed',
            expected_entry_type: 'news',
            active_status: 'active',
            source_id: 'techcrunch'
          }
        ]
      })
    ];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async (url) => ({
        ok: true,
        status: 200,
        text: async () => url.includes('one')
          ? buildSampleRss({
            title: 'Duplicate story',
            url: 'https://example.com/dup-story',
            description: 'Short summary.'
          })
          : buildSampleRss({
            title: 'Duplicate story',
            url: 'https://example.com/dup-story',
            description: 'Short summary.',
            content: 'This duplicate carries the fuller body text and should be kept by the deduper. '.repeat(6)
          })
      })
    });

    assert.equal(result.rawItems.length, 1);
    assert.match(result.rawItems[0].item.canonical_text, /fuller body text/);
    assert.equal(result.reports.fetch.duplicate_raw_item_count, 1);
  });
});

test('Chinese-language feed entries preserve text and language metadata', async () => {
  await withTempDir(async (directory) => {
    const inventory = [
      buildInventoryRecord('caixin-green-esg', {
        display_name: '财新绿色ESG',
        language: 'zh',
        source_class: 'climate_sustainability',
        feed_support_status: 'partial'
      })
    ];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => buildSampleRss({
          title: '绿色投资继续扩张',
          url: 'https://example.com/zh-story',
          description: '中国绿色投资项目继续扩张。',
          content: '中国绿色投资项目继续扩张，地方政府和产业基金正在加快新的清洁能源部署。'.repeat(8)
        })
      })
    });

    assert.equal(result.rawItems.length, 1);
    assert.equal(result.rawItems[0].item.language, 'zh');
    assert.equal(result.rawItems[0].item.title, '绿色投资继续扩张');
    assert.match(result.rawItems[0].item.summary, /绿色投资/);
  });
});

test('diagnostics include governed coverage counts, gap visibility, and raw-item distributions', async () => {
  await withTempDir(async (directory) => {
    const result = await generateLiveInputArtifacts({
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => buildSampleRss()
      })
    });

    assert.equal(result.reports.governedInventory.total_governed_sources > 0, true);
    assert.equal(result.reports.governedInventory.counts_by_support_status.supported > 0, true);
    assert.equal(Array.isArray(result.reports.coverageGap.sources_with_zero_feed_definitions), true);
    assert.equal(typeof result.reports.validationLedger.counts_by_source_validation_status, 'object');
    assert.equal(typeof result.reports.runtimeReport, 'object');
    assert.equal(Array.isArray(result.reports.runtimeReport.feed_rows), true);
    assert.equal(typeof result.reports.rawItemSourceDistribution.item_counts_per_source, 'object');
    assert.equal(typeof result.reports.rawItemLanguageDistribution.item_counts_per_language, 'object');
    assert.equal(typeof result.reports.rawItemClassDistribution.item_counts_per_source_class, 'object');
  });
});

test('mixed success and failure fetch runs are represented cleanly', async () => {
  await withTempDir(async (directory) => {
    const inventory = [
      buildInventoryRecord('techcrunch', {
        feed_definitions: [
          {
            feed_id: 'good',
            label: 'Good',
            url: 'https://feeds.example.com/good.xml',
            format: 'rss',
            adapter_type: 'xml_feed',
            content_mode: 'summary_only',
            expected_entry_type: 'news',
            active_status: 'active',
            source_id: 'techcrunch'
          },
          {
            feed_id: 'bad',
            label: 'Bad',
            url: 'https://feeds.example.com/bad.xml',
            format: 'rss',
            adapter_type: 'xml_feed',
            content_mode: 'summary_only',
            expected_entry_type: 'news',
            active_status: 'active',
            source_id: 'techcrunch'
          }
        ]
      })
    ];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async (url) => url.includes('good')
        ? {
          ok: true,
          status: 200,
          text: async () => buildSampleRss()
        }
        : {
          ok: false,
          status: 502,
          text: async () => 'bad gateway'
        }
    });

    assert.equal(result.reports.fetch.fetch_successes, 1);
    assert.equal(result.reports.fetch.fetch_failures, 1);
    assert.equal(result.reports.fetch.fetch_failures_by_category.http_5xx, 1);
    assert.equal(result.reports.generation.warning_flags.includes('one_or_more_feeds_failed'), true);
  });
});

test('per-feed validation aggregates to a truthful per-source validation status', async () => {
  await withTempDir(async (directory) => {
    const inventory = [
      buildInventoryRecord('brookings', {
        display_name: 'Brookings',
        source_class: 'policy_geopolitics_institutions',
        feed_support_status: 'partial',
        feed_definitions: [
          {
            feed_id: 'working',
            label: 'Working Feed',
            url: 'https://feeds.example.com/working.xml',
            format: 'rss',
            adapter_type: 'xml_feed',
            content_mode: 'mixed',
            expected_entry_type: 'analysis',
            active_status: 'active',
            source_id: 'brookings'
          },
          {
            feed_id: 'failing',
            label: 'Failing Feed',
            url: 'https://feeds.example.com/failing.xml',
            format: 'rss',
            adapter_type: 'xml_feed',
            content_mode: 'summary_only',
            expected_entry_type: 'analysis',
            active_status: 'active',
            source_id: 'brookings'
          }
        ]
      })
    ];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async (url) => url.includes('working')
        ? {
          ok: true,
          status: 200,
          text: async () => buildSampleRss({
            title: 'Industrial policy monitor',
            url: 'https://example.com/industrial-policy-monitor',
            description: 'Working feed summary.',
            content: 'Working feed content with enough material to generate a usable raw item. '.repeat(6)
          })
        }
        : {
          ok: false,
          status: 500,
          text: async () => 'broken'
        }
    });

    const sourceLedger = result.reports.validationLedger.sources.find((entry) => entry.source_id === 'brookings');
    const feedStatuses = Object.fromEntries(result.reports.validationLedger.feeds.map((entry) => [entry.feed_id, entry.validation_status]));

    assert.equal(feedStatuses.working, 'validated_raw_items');
    assert.equal(feedStatuses.failing, 'fetch_failed');
    assert.equal(sourceLedger?.validation_status, 'validated_raw_items');
    assert.equal(sourceLedger?.verified_working, true);
    assert.match(sourceLedger?.validation_notes ?? '', /Some feed definitions still failed/);
  });
});

test('refined zero-entry and raw-item summary semantics stay separate', async () => {
  await withTempDir(async (directory) => {
    const inventory = [
      buildInventoryRecord('bbc-news'),
      buildInventoryRecord('brookings')
    ];

    const result = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async (url) => ({
        ok: true,
        status: 200,
        url,
        redirected: false,
        headers: {
          get() {
            return 'application/rss+xml; charset=utf-8';
          }
        },
        arrayBuffer: async () => Buffer.from(
          url.includes('bbc-news')
            ? buildSampleRss({
              title: 'A valid entry',
              url: 'https://example.com/valid-entry',
              description: 'Summary'
            })
            : '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel></channel></rss>'
        )
      })
    });

    assert.equal(result.reports.fetch.attempted_sources_total, 2);
    assert.equal(result.reports.fetch.attempted_sources_with_zero_entries, 1);
    assert.equal(result.reports.fetch.attempted_sources_with_raw_items, 1);
  });
});

test('validation ledger tracks consecutive failures across runs in the same output directory', async () => {
  await withTempDir(async (directory) => {
    const inventory = [buildInventoryRecord('techcrunch', { feed_support_status: 'supported' })];

    const first = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: false,
        status: 503,
        text: async () => 'unavailable'
      })
    });
    const second = await generateLiveInputArtifacts({
      inventory,
      outputDir: directory,
      now: new Date('2026-03-16T12:00:00Z'),
      fetchImpl: async () => ({
        ok: false,
        status: 503,
        text: async () => 'unavailable'
      })
    });

    assert.equal(first.reports.validationLedger.feeds[0].consecutive_failures, 1);
    assert.equal(second.reports.validationLedger.feeds[0].consecutive_failures, 2);
    assert.equal(second.reports.validationLedger.sources[0].consecutive_failures, 2);
  });
});

test('workflow-facing live-input artifacts are written to disk', async () => {
  await withTempDir(async (directory) => {
    const result = await generateLiveInputArtifacts({
      outputDir: directory,
      now: FIXED_NOW,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => buildSampleRss()
      })
    });

    assert.equal(existsSync(result.paths.rawItems), true);
    assert.equal(existsSync(result.paths.governedInventoryReport), true);
    assert.equal(existsSync(result.paths.inventoryReport), true);
    assert.equal(existsSync(result.paths.fetchReport), true);
    assert.equal(existsSync(result.paths.generationReport), true);
    assert.equal(existsSync(result.paths.coverageGapReport), true);
    assert.equal(existsSync(result.paths.validationLedger), true);
    assert.equal(existsSync(result.paths.runtimeReport), true);
    assert.equal(existsSync(result.paths.operationalSummary), true);
    assert.equal(existsSync(result.paths.rawItemSourceDistribution), true);
    assert.equal(existsSync(result.paths.rawItemLanguageDistribution), true);
    assert.equal(existsSync(result.paths.rawItemClassDistribution), true);
  });
});

test('workflow-summary-facing output is compact and human-readable', async () => {
  const summaryText = formatLiveInputOperationalSummary({
    governed_sources_total: 133,
    configured_sources_total: 70,
    non_configured_sources_total: 63,
    attempted_sources_total: 70,
    configured_sources_by_support_status: { supported: 10, partial: 5, pending_review: 3 },
    validation_counts_by_source_status: { validated_raw_items: 2, fetch_failed: 8, not_configured: 123 },
    validation_counts_by_feed_status: { validated_raw_items: 2, fetch_failed: 10 },
    configured_feed_definitions: 12,
    feed_requests_attempted: 10,
    http_fetch_successes: 3,
    http_fetch_failures: 7,
    fetch_failures_by_category: { http_404: 4, timeout: 3 },
    parse_successes: 2,
    parse_failures: 1,
    attempted_sources_with_zero_entries: 1,
    attempted_sources_with_entries_but_zero_normalized_items: 2,
    attempted_sources_with_raw_items: 7,
    raw_items_generated: 4,
    top_raw_item_source_classes: [
      { source_class: 'global_hard_news', count: 2 },
      { source_class: 'technology_digital_economy', count: 1 }
    ],
    verified_working_sources: [],
    success_ladder: [],
    warning_flags: ['one_or_more_feeds_failed']
  });

  assert.match(summaryText, /DIBS Live Input Summary/);
  assert.match(summaryText, /governed sources: 133/);
  assert.match(summaryText, /configured sources: 70/);
  assert.match(summaryText, /support_status: supported=10, partial=5, pending_review=3/);
  assert.match(summaryText, /failure categories: http_404=4, timeout=3/);
  assert.match(summaryText, /warning flags: one_or_more_feeds_failed/);
});

test('runtime table highlights success ladder outcomes and sample failures', () => {
  const runtimeText = formatLiveInputRuntimeTable({
    success_ladder: [
      {
        source_id: 'bbc-news',
        feed_fetch_success: true,
        parse_success: true,
        raw_item_success: true,
        raw_item_count: 12,
        validation_status: 'validated_raw_items',
        failure_categories: []
      }
    ],
    feed_rows: [
      {
        source_id: 'brookings',
        feed_id: 'all',
        error_classification: 'non_xml_response',
        http_status: 200
      }
    ]
  });

  assert.match(runtimeText, /Core Success Ladder/);
  assert.match(runtimeText, /bbc-news: fetch=yes, parse=yes, raw_items=12/);
  assert.match(runtimeText, /brookings\/all: non_xml_response \(http 200\)/);
});

test('orchestration can consume generated raw-items instead of the demo fixture', async () => {
  await withTempDir(async (directory) => {
    const inventory = [
      buildInventoryRecord('reuters', { display_name: 'Reuters', source_class: 'global_hard_news' }),
      buildInventoryRecord('techcrunch', { display_name: 'TechCrunch', source_class: 'technology_digital_economy' }),
      buildInventoryRecord('carbon-brief', { display_name: 'Carbon Brief', source_class: 'climate_sustainability', expected_entry_type: 'analysis' }),
      buildInventoryRecord('brookings', { display_name: 'Brookings Institution', source_class: 'policy_geopolitics_institutions', expected_entry_type: 'research' }),
      buildInventoryRecord('caixin-global', { display_name: 'Caixin Global', source_class: 'china_policy_economy' })
    ];

    const fetchBodies = new Map([
      ['https://feeds.example.com/reuters.xml', buildSampleRss({
        title: 'Rates outlook shifts after inflation cools',
        url: 'https://example.com/reuters-rates',
        description: 'Inflation data altered the rates outlook for global markets.',
        content: 'Inflation data altered the rates outlook for global markets and shaped currency expectations across major economies. '.repeat(6)
      })],
      ['https://feeds.example.com/techcrunch.xml', buildSampleRss({
        title: 'Cloud providers expand AI data center spending',
        url: 'https://example.com/techcrunch-ai',
        description: 'Cloud providers are raising AI infrastructure spending.',
        content: 'Cloud providers are raising AI infrastructure spending after enterprise demand accelerated in the latest quarter. '.repeat(6)
      })],
      ['https://feeds.example.com/carbon-brief.xml', buildSampleRss({
        title: 'Grid investment becomes the bottleneck for clean power',
        url: 'https://example.com/carbon-brief-grid',
        description: 'Grid buildout is becoming the bottleneck for clean power deployment.',
        content: 'Grid buildout is becoming the bottleneck for clean power deployment as project queues lengthen across multiple markets. '.repeat(6)
      })],
      ['https://feeds.example.com/brookings.xml', buildSampleRss({
        title: 'Brookings argues industrial subsidies need accountability rules',
        url: 'https://example.com/brookings-industrial-policy',
        description: 'Brookings calls for tighter accountability rules for industrial subsidies.',
        content: 'Brookings calls for tighter accountability rules for industrial subsidies and more explicit performance measurement by agencies. '.repeat(6)
      })],
      ['https://feeds.example.com/caixin-global.xml', buildSampleRss({
        title: 'Provincial factories raise investment plans under new upgrade push',
        url: 'https://example.com/caixin-factories',
        description: 'Provincial factories raised investment plans under a new upgrade push.',
        content: 'Provincial factories raised investment plans under a new upgrade push as local officials targeted manufacturing capacity upgrades. '.repeat(6)
      })]
    ]);

    const generated = await generateLiveInputArtifacts({
      inventory,
      outputDir: join(directory, 'inputs'),
      now: FIXED_NOW,
      fetchImpl: async (url) => ({
        ok: true,
        status: 200,
        text: async () => fetchBodies.get(url)
      })
    });

    const deliveryRulesPath = writeTempDeliveryRules(directory);
    const run = await executeDibsRun({
      rawItems: generated.rawItems,
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath,
      dryRun: true
    });

    assert.equal(typeof run.runBundle.run_id, 'string');
    assert.equal(Array.isArray(run.runBundle.selected_article_ids), true);
    assert.equal(run.deliveryStatus.final_outcome, 'dry_run');
  });
});

test('deterministic behavior holds under fixed sample feed inputs and reports', async () => {
  await withTempDir(async (directory) => {
    const inventory = [buildInventoryRecord('techcrunch')];
    const fetchImpl = async () => ({
      ok: true,
      status: 200,
      text: async () => buildSampleRss({
        title: 'Consistent feed item',
        url: 'https://example.com/consistent-item',
        description: 'Deterministic summary text.'
      })
    });

    const first = await generateLiveInputArtifacts({
      inventory,
      outputDir: join(directory, 'first'),
      now: FIXED_NOW,
      fetchImpl
    });
    const second = await generateLiveInputArtifacts({
      inventory,
      outputDir: join(directory, 'second'),
      now: FIXED_NOW,
      fetchImpl
    });

    assert.deepEqual(first.rawItems, second.rawItems);
    assert.deepEqual(first.reports.generation, second.reports.generation);
    assert.deepEqual(first.reports.fetch, second.reports.fetch);
  });
});
