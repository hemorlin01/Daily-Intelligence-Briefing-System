import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { TextDecoder } from 'node:util';
import { loadFeedOverrides, loadLiveInputRules, loadSourceCatalog } from '../config/load-config.js';
import { dispatchIngestionAdapter } from './adapters.js';

const DEFAULT_SOURCES_PATH = resolve(process.cwd(), 'config', 'sources.json');
const DEFAULT_FEED_OVERRIDES_PATH = resolve(process.cwd(), 'config', 'feed-overrides.json');
const DEFAULT_RULES_PATH = resolve(process.cwd(), 'config', 'live-input-rules.json');
const DEFAULT_OUTPUT_DIR = resolve(process.cwd(), 'artifacts', 'inputs');
const FETCHABLE_SUPPORT_STATUSES = new Set(['supported', 'partial', 'unstable']);
const XMLISH_CONTENT_TYPE_PATTERN = /(rss|atom|xml)/i;
const HTML_CONTENT_TYPE_PATTERN = /html/i;
const RETRYABLE_FAILURE_CATEGORIES = new Set(['dns_error', 'timeout', 'tls_error', 'connection_error', 'too_many_redirects', 'http_429', 'http_5xx']);
const SUCCESS_VALIDATION_STAGES = new Set(['validated_raw_items', 'normalized_zero_items', 'fetched_zero_entries']);
const SOURCE_VALIDATION_STATUS_PRIORITY = [
  'validated_raw_items',
  'normalized_zero_items',
  'fetched_zero_entries',
  'parse_failed',
  'fetch_failed',
  'not_attempted',
  'not_configured'
];
const FEED_VALIDATION_STATUS_PRIORITY = [
  'validated_raw_items',
  'normalized_zero_items',
  'fetched_zero_entries',
  'parse_failed',
  'fetch_failed',
  'not_attempted'
];

function writeJson(path, value) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
  return path;
}

function readJsonIfExists(path) {
  if (!existsSync(path)) {
    return null;
  }

  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch {
    return null;
  }
}

function incrementCounter(target, key, amount = 1) {
  target[key] = (target[key] ?? 0) + amount;
}

function maxIsoTimestamp(values) {
  const filtered = values.filter((value) => typeof value === 'string' && value.length > 0);
  if (filtered.length === 0) {
    return null;
  }
  filtered.sort();
  return filtered[filtered.length - 1];
}

function countBy(items, selector) {
  const counts = {};
  for (const item of items) {
    incrementCounter(counts, selector(item) ?? 'unknown');
  }
  return counts;
}

function buildHeaders(fetchRules) {
  return {
    'user-agent': fetchRules.fetch.user_agent,
    accept: 'application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, text/html;q=0.8, */*;q=0.7',
    'accept-language': fetchRules.fetch.accept_language
  };
}

function normalizeSupportStatus(source, overrides) {
  return overrides.defaults.support_status_by_fetch_method[source.fetch_method] ?? 'pending_review';
}

function deriveFeedIngestionMethod(feed, sourceIngestionMethod = 'none') {
  if (feed.adapter_type === 'html_listing') {
    return 'html_listing';
  }
  if (feed.format === 'atom') {
    return 'atom';
  }
  if (feed.format === 'rss') {
    return 'rss';
  }
  return sourceIngestionMethod;
}

function deriveSourceIngestionMethod(override, feeds, source, overrides) {
  if (typeof override.ingestion_method === 'string' && override.ingestion_method !== 'none') {
    return override.ingestion_method;
  }
  if (feeds.length === 0) {
    return override.ingestion_method
      ?? overrides.defaults.ingestion_method_defaults_by_fetch_method[source.fetch_method]
      ?? 'none';
  }
  if (feeds.some((feed) => feed.adapter_type === 'html_listing')) {
    return 'html_listing';
  }
  if (feeds.every((feed) => feed.format === 'atom')) {
    return 'atom';
  }
  return 'rss';
}

function deriveSourceAdapterType(override, feeds, source, overrides) {
  if (typeof override.adapter_type === 'string' && override.adapter_type !== 'none') {
    return override.adapter_type;
  }
  if (feeds.length === 0) {
    return override.adapter_type
      ?? overrides.defaults.adapter_type_defaults_by_fetch_method[source.fetch_method]
      ?? 'none';
  }
  const adapterTypes = [...new Set(feeds.map((feed) => feed.adapter_type))];
  return adapterTypes.length === 1 ? adapterTypes[0] : 'xml_feed';
}

function normalizeInventoryRecord(record) {
  const supportStatus = record.support_status ?? record.feed_support_status ?? 'pending_review';
  const feedDefinitions = (record.feed_definitions ?? record.feeds ?? []).map((feed, index) => {
    const format = feed.format ?? feed.feed_format ?? 'rss';
    const adapterType = feed.adapter_type ?? record.adapter_type ?? (format === 'html' ? 'html_listing' : 'xml_feed');
    return Object.freeze({
      source_id: feed.source_id ?? record.source_id,
      feed_id: feed.feed_id ?? `feed-${index + 1}`,
      label: feed.label ?? `Feed ${index + 1}`,
      url: feed.url ?? feed.feed_url ?? null,
      format,
      adapter_type: adapterType,
      ingestion_method: feed.ingestion_method
        ?? deriveFeedIngestionMethod(
          { format, adapter_type: adapterType },
          record.ingestion_method ?? 'none'
        ),
      content_mode: feed.content_mode ?? 'summary_only',
      expected_entry_type: feed.expected_entry_type ?? record.expected_article_type ?? 'news',
      parser_type: feed.parser_type ?? record.parser_type ?? 'rss',
      active_status: feed.active_status ?? 'active',
      notes: feed.notes ?? record.notes ?? '',
      order_index: feed.order_index ?? index,
      patterns: feed.patterns ?? null
    });
  });

  const ingestionMethod = record.ingestion_method && record.ingestion_method !== 'none'
    ? record.ingestion_method
    : feedDefinitions.length === 0
      ? 'none'
      : feedDefinitions.some((feed) => feed.adapter_type === 'html_listing')
        ? 'html_listing'
        : feedDefinitions.every((feed) => feed.format === 'atom')
          ? 'atom'
          : 'rss';

  const adapterType = record.adapter_type && record.adapter_type !== 'none'
    ? record.adapter_type
    : feedDefinitions.length === 0
      ? 'none'
      : [...new Set(feedDefinitions.map((feed) => feed.adapter_type))].length === 1
        ? feedDefinitions[0].adapter_type
        : 'xml_feed';

  return Object.freeze({
    ...record,
    support_status: supportStatus,
    feed_support_status: supportStatus,
    support_level: record.support_level ?? 'configured_governed_source',
    notes: record.notes ?? record.feed_notes ?? '',
    primary_region: record.primary_region ?? record.region ?? 'global',
    region: record.region ?? record.primary_region ?? 'global',
    ingestion_method: ingestionMethod,
    adapter_type: adapterType,
    validation_status: record.validation_status
      ?? (feedDefinitions.length === 0 ? 'not_configured' : 'not_attempted'),
    feed_definitions: feedDefinitions,
    feeds: feedDefinitions,
    feed_count: feedDefinitions.length,
    feed_url: record.feed_url ?? feedDefinitions[0]?.url ?? null,
    feed_format: record.feed_format ?? feedDefinitions[0]?.format ?? 'none',
    expected_entry_type: record.expected_entry_type
      ?? feedDefinitions[0]?.expected_entry_type
      ?? record.expected_article_type
      ?? 'news'
  });
}

function normalizeInventoryRecords(records) {
  return Object.freeze(records.map((record) => normalizeInventoryRecord(record)));
}

function materializeFeeds(source, override = {}) {
  const sourceIngestionMethod = override.ingestion_method ?? 'none';
  const feedDefinitions = override.feed_definitions ?? override.feeds ?? [];
  return Object.freeze(feedDefinitions.map((feed, index) => {
    const format = feed.format ?? feed.feed_format ?? 'rss';
    const adapterType = feed.adapter_type ?? override.adapter_type ?? (format === 'html' ? 'html_listing' : 'xml_feed');
    return Object.freeze({
      source_id: source.source_id,
      feed_id: feed.feed_id,
      label: feed.label,
      url: feed.url ?? feed.feed_url,
      format,
      adapter_type: adapterType,
      ingestion_method: feed.ingestion_method ?? deriveFeedIngestionMethod({ format, adapter_type: adapterType }, sourceIngestionMethod),
      content_mode: feed.content_mode ?? 'summary_only',
      expected_entry_type: feed.expected_entry_type ?? source.expected_article_type,
      parser_type: feed.parser_type ?? source.parser_type,
      active_status: feed.active_status ?? 'active',
      notes: feed.notes ?? override.notes ?? '',
      order_index: index,
      patterns: feed.patterns ?? null
    });
  }));
}

function scoreRawItemCompleteness(rawItem) {
  let score = 0;
  if (rawItem.item.canonical_text) {
    score += 4;
  }
  if (rawItem.item.summary) {
    score += 2;
  }
  if (rawItem.item.published_at) {
    score += 1;
  }
  if (rawItem.item.author) {
    score += 1;
  }
  return score;
}

function dedupeRawItems(rawItems) {
  const unique = new Map();
  const duplicates = [];

  for (const rawItem of rawItems) {
    const key = `${rawItem.source_id}::${rawItem.item.url}`;
    const existing = unique.get(key);
    if (!existing) {
      unique.set(key, rawItem);
      continue;
    }

    const existingScore = scoreRawItemCompleteness(existing);
    const candidateScore = scoreRawItemCompleteness(rawItem);
    if (candidateScore > existingScore) {
      unique.set(key, rawItem);
      duplicates.push({
        key,
        kept_article_title: rawItem.item.title,
        dropped_article_title: existing.item.title,
        reason: 'higher_completeness'
      });
    } else {
      duplicates.push({
        key,
        kept_article_title: existing.item.title,
        dropped_article_title: rawItem.item.title,
        reason: 'duplicate_url'
      });
    }
  }

  return {
    rawItems: Array.from(unique.values()),
    duplicateActions: duplicates
  };
}

function isLikelyBodyText(value) {
  return typeof value === 'string' && value.trim().length >= 280;
}

function buildFeedInventoryRecord(source, override, overrides) {
  const supportStatus = override.feed_support_status ?? normalizeSupportStatus(source, overrides);
  const supportLevel = override.support_level ?? overrides.defaults.support_level_by_status[supportStatus];
  const notes = override.notes ?? override.feed_notes ?? overrides.defaults.default_notes_by_status[supportStatus];
  const feedDefinitions = materializeFeeds(source, override);
  const ingestionMethod = deriveSourceIngestionMethod(override, feedDefinitions, source, overrides);
  const adapterType = deriveSourceAdapterType(override, feedDefinitions, source, overrides);

  if (FETCHABLE_SUPPORT_STATUSES.has(supportStatus) && feedDefinitions.length === 0) {
    throw new Error(`Feed inventory source "${source.source_id}" is marked ${supportStatus} but has no feed records`);
  }

  return Object.freeze({
    source_id: source.source_id,
    canonical_source_key: source.source_id,
    display_name: source.display_name,
    source_name: source.display_name,
    source_class: source.source_class,
    language: source.language,
    region: source.primary_region,
    primary_region: source.primary_region,
    active_status: source.active_status,
    priority_tier: source.priority_tier,
    fetch_method: source.fetch_method,
    parser_type: source.parser_type,
    ingestion_method: ingestionMethod,
    adapter_type: adapterType,
    paywall_policy: source.paywall_policy,
    expected_article_type: source.expected_article_type,
    support_status: supportStatus,
    feed_support_status: supportStatus,
    support_level: supportLevel,
    notes,
    feed_notes: notes,
    feed_url: feedDefinitions[0]?.url ?? null,
    feed_format: feedDefinitions[0]?.format ?? 'none',
    expected_entry_type: feedDefinitions[0]?.expected_entry_type ?? source.expected_article_type,
    feed_definitions: feedDefinitions,
    feeds: feedDefinitions,
    feed_count: feedDefinitions.length,
    validation_status: feedDefinitions.length === 0 ? 'not_configured' : 'not_attempted'
  });
}

export function buildFeedInventory({
  sourcesPath = DEFAULT_SOURCES_PATH,
  feedOverridesPath = DEFAULT_FEED_OVERRIDES_PATH
} = {}) {
  const sourceCatalog = loadSourceCatalog(sourcesPath);
  const overrides = loadFeedOverrides(feedOverridesPath);
  const inventory = [];

  for (const source of sourceCatalog.sources.values()) {
    const override = overrides.sources[source.source_id] ?? {};
    inventory.push(buildFeedInventoryRecord(source, override, overrides));
  }

  for (const sourceId of Object.keys(overrides.sources)) {
    if (!sourceCatalog.sources.has(sourceId)) {
      throw new Error(`Feed overrides reference unknown source_id "${sourceId}"`);
    }
  }

  inventory.sort((left, right) => left.source_id.localeCompare(right.source_id));
  return Object.freeze(inventory);
}

function buildGovernedInventoryReport(inventory, now, validationLedger = null) {
  const validationBySource = new Map((validationLedger?.sources ?? []).map((source) => [source.source_id, source.validation_status]));
  return {
    generated_at: now,
    total_governed_sources: inventory.length,
    counts_by_source_class: countBy(inventory, (source) => source.source_class),
    counts_by_language: countBy(inventory, (source) => source.language),
    counts_by_region: countBy(inventory, (source) => source.primary_region),
    counts_by_priority_tier: countBy(inventory, (source) => String(source.priority_tier)),
    counts_by_support_status: countBy(inventory, (source) => source.support_status),
    counts_by_ingestion_method: countBy(inventory, (source) => source.ingestion_method),
    zero_feed_definition_count: inventory.filter((source) => source.feed_definitions.length === 0).length,
    sources: inventory.map((source) => ({
      source_id: source.source_id,
      display_name: source.display_name,
      source_class: source.source_class,
      language: source.language,
      primary_region: source.primary_region,
      priority_tier: source.priority_tier,
      fetch_method: source.fetch_method,
      parser_type: source.parser_type,
      ingestion_method: source.ingestion_method,
      adapter_type: source.adapter_type,
      support_status: source.support_status,
      validation_status: validationBySource.get(source.source_id) ?? source.validation_status,
      support_level: source.support_level,
      active_status: source.active_status,
      feed_count: source.feed_definitions.length,
      notes: source.notes
    }))
  };
}

function buildFeedInventoryReport(inventory, now) {
  const feedRecords = [];
  for (const source of inventory) {
    for (const feed of source.feed_definitions) {
      feedRecords.push({
        source_id: source.source_id,
        display_name: source.display_name,
        source_class: source.source_class,
        language: source.language,
        support_status: source.support_status,
        support_level: source.support_level,
        source_ingestion_method: source.ingestion_method,
        source_adapter_type: source.adapter_type,
        feed_id: feed.feed_id,
        label: feed.label,
        url: feed.url,
        format: feed.format,
        adapter_type: feed.adapter_type,
        ingestion_method: feed.ingestion_method,
        content_mode: feed.content_mode,
        active_status: feed.active_status,
        expected_entry_type: feed.expected_entry_type
      });
    }
  }

  return {
    generated_at: now,
    total_sources_with_feed_records: inventory.filter((source) => source.feed_definitions.length > 0).length,
    total_feed_records: feedRecords.length,
    counts_by_support_status: countBy(feedRecords, (feed) => feed.support_status),
    counts_by_format: countBy(feedRecords, (feed) => feed.format),
    counts_by_adapter_type: countBy(feedRecords, (feed) => feed.adapter_type),
    counts_by_ingestion_method: countBy(feedRecords, (feed) => feed.ingestion_method),
    counts_by_content_mode: countBy(feedRecords, (feed) => feed.content_mode),
    feeds: feedRecords
  };
}

function sleep(ms) {
  return new Promise((resolvePromise) => {
    setTimeout(resolvePromise, ms);
  });
}

function getHeaderValue(headers, name) {
  if (!headers) {
    return null;
  }
  if (typeof headers.get === 'function') {
    return headers.get(name);
  }
  if (typeof headers === 'object') {
    return headers[name] ?? headers[name.toLowerCase()] ?? null;
  }
  return null;
}

function getCharsetFromContentType(contentType) {
  const match = contentType?.match(/charset=([^;]+)/i);
  return match?.[1]?.trim() ?? 'utf-8';
}

async function readResponseBody(response, maxResponseBytes) {
  if (typeof response.arrayBuffer === 'function') {
    const buffer = await response.arrayBuffer();
    const bytesReceived = buffer.byteLength;
    if (bytesReceived > maxResponseBytes) {
      return {
        body: '',
        bytesReceived,
        tooLarge: true
      };
    }

    const contentType = getHeaderValue(response.headers, 'content-type') ?? '';
    let decoder;
    try {
      decoder = new TextDecoder(getCharsetFromContentType(contentType));
    } catch {
      decoder = new TextDecoder('utf-8');
    }
    return {
      body: decoder.decode(buffer),
      bytesReceived,
      tooLarge: false
    };
  }

  const body = await response.text();
  return {
    body,
    bytesReceived: Buffer.byteLength(body, 'utf8'),
    tooLarge: false
  };
}

function looksLikeXml(body) {
  if (typeof body !== 'string') {
    return false;
  }
  return /<(?:\?xml\b|rss\b|feed\b|rdf:RDF\b|channel\b)/i.test(body);
}

function looksLikeHtml(body) {
  if (typeof body !== 'string') {
    return false;
  }
  return /<(?:!doctype\s+html\b|html\b|head\b|body\b)/i.test(body);
}

function classifyHttpFailure(status) {
  if (status === 401) {
    return 'http_401';
  }
  if (status === 403) {
    return 'http_403';
  }
  if (status === 404) {
    return 'http_404';
  }
  if (status === 429) {
    return 'http_429';
  }
  if (status >= 500 && status <= 599) {
    return 'http_5xx';
  }
  return 'http_error';
}

function classifyFetchException(error) {
  const name = error?.name ?? '';
  const code = error?.code ?? error?.cause?.code ?? '';
  const message = `${error?.message ?? ''} ${error?.cause?.message ?? ''}`.toLowerCase();

  if (name === 'AbortError' || code === 'ABORT_ERR' || code === 'ETIMEDOUT' || message.includes('timeout')) {
    return 'timeout';
  }
  if (code === 'ENOTFOUND' || code === 'EAI_AGAIN' || message.includes('getaddrinfo')) {
    return 'dns_error';
  }
  if (
    code === 'CERT_HAS_EXPIRED'
    || code === 'DEPTH_ZERO_SELF_SIGNED_CERT'
    || code === 'ERR_TLS_CERT_ALTNAME_INVALID'
    || code === 'UNABLE_TO_VERIFY_LEAF_SIGNATURE'
    || message.includes('tls')
    || message.includes('certificate')
  ) {
    return 'tls_error';
  }
  if (message.includes('redirect')) {
    return 'too_many_redirects';
  }
  if (
    code === 'ECONNRESET'
    || code === 'ECONNREFUSED'
    || code === 'ECONNABORTED'
    || code === 'EHOSTUNREACH'
    || code === 'ENETUNREACH'
    || message.includes('socket')
    || message.includes('other side closed')
  ) {
    return 'connection_error';
  }
  return 'connection_error';
}

function buildResultError(category, message, httpStatus = null) {
  return {
    code: category.toUpperCase(),
    category,
    message,
    http_status: httpStatus
  };
}

function shouldRetry(category, attempt, fetchRules) {
  return RETRYABLE_FAILURE_CATEGORIES.has(category) && attempt < fetchRules.fetch.max_attempts;
}

function buildFetchNote({ finalUrl, requestedUrl, contentType, category, redirected }) {
  if (redirected && finalUrl && finalUrl !== requestedUrl) {
    return `Redirected to ${finalUrl}${category ? ` (${category})` : ''}${contentType ? ` with content-type ${contentType}` : ''}`;
  }
  if (category === 'invalid_content_type' || category === 'non_xml_response') {
    return `${category} from ${requestedUrl}${contentType ? ` (${contentType})` : ''}`;
  }
  if (contentType) {
    return `content-type ${contentType}`;
  }
  return '';
}

async function fetchFeed({ feed, source, fetchImpl, timeoutMs, fetchRules }) {
  let lastResult = null;

  for (let attempt = 1; attempt <= fetchRules.fetch.max_attempts; attempt += 1) {
    const controller = new AbortController();
    const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetchImpl(feed.url, {
        method: 'GET',
        headers: buildHeaders(fetchRules),
        signal: controller.signal,
        redirect: 'follow'
      });
      const contentType = getHeaderValue(response.headers, 'content-type');
      const { body, bytesReceived, tooLarge } = await readResponseBody(response, fetchRules.fetch.max_response_bytes);
      const finalUrl = response.url ?? feed.url;
      const redirected = Boolean(response.redirected) || finalUrl !== feed.url;

      if (!response.ok) {
        const failureCategory = classifyHttpFailure(response.status);
        lastResult = {
          source_id: source.source_id,
          source_name: source.display_name,
          feed_id: feed.feed_id,
          feed_label: feed.label,
          feed_url: feed.url,
          final_url: finalUrl,
          redirected,
          attempt_count: attempt,
          status: 'fetch_failed',
          http_status: response.status,
          content_type: contentType,
          bytes_received: bytesReceived,
          entry_count: 0,
          entries: [],
          raw_item_count: 0,
          failure_category: failureCategory,
          note: buildFetchNote({ finalUrl, requestedUrl: feed.url, contentType, category: failureCategory, redirected }),
          error: buildResultError(failureCategory, `Feed request failed with status ${response.status}`, response.status)
        };
      } else if (tooLarge) {
        lastResult = {
          source_id: source.source_id,
          source_name: source.display_name,
          feed_id: feed.feed_id,
          feed_label: feed.label,
          feed_url: feed.url,
          final_url: finalUrl,
          redirected,
          attempt_count: attempt,
          status: 'parse_failed',
          http_status: response.status,
          content_type: contentType,
          bytes_received: bytesReceived,
          entry_count: 0,
          entries: [],
          raw_item_count: 0,
          failure_category: 'response_too_large',
          note: buildFetchNote({ finalUrl, requestedUrl: feed.url, contentType, category: 'response_too_large', redirected }),
          error: buildResultError('response_too_large', `Response exceeded ${fetchRules.fetch.max_response_bytes} bytes`, response.status)
        };
      } else if (!body || body.trim().length === 0) {
        lastResult = {
          source_id: source.source_id,
          source_name: source.display_name,
          feed_id: feed.feed_id,
          feed_label: feed.label,
          feed_url: feed.url,
          final_url: finalUrl,
          redirected,
          attempt_count: attempt,
          status: 'parse_failed',
          http_status: response.status,
          content_type: contentType,
          bytes_received: bytesReceived,
          entry_count: 0,
          entries: [],
          raw_item_count: 0,
          failure_category: 'empty_response',
          note: buildFetchNote({ finalUrl, requestedUrl: feed.url, contentType, category: 'empty_response', redirected }),
          error: buildResultError('empty_response', 'Feed response body was empty', response.status)
        };
      } else {
        const xmlExpected = feed.adapter_type === 'xml_feed';
        const bodyLooksXml = looksLikeXml(body);
        const bodyLooksHtml = looksLikeHtml(body);
        const contentTypeLooksXml = XMLISH_CONTENT_TYPE_PATTERN.test(contentType ?? '');
        const contentTypeLooksHtml = HTML_CONTENT_TYPE_PATTERN.test(contentType ?? '');

        if (xmlExpected && !bodyLooksXml) {
          const failureCategory = contentTypeLooksHtml || bodyLooksHtml ? 'non_xml_response' : 'invalid_content_type';
          lastResult = {
            source_id: source.source_id,
            source_name: source.display_name,
            feed_id: feed.feed_id,
            feed_label: feed.label,
            feed_url: feed.url,
            final_url: finalUrl,
            redirected,
            attempt_count: attempt,
            status: 'parse_failed',
            http_status: response.status,
            content_type: contentType,
            bytes_received: bytesReceived,
            entry_count: 0,
            entries: [],
            raw_item_count: 0,
            failure_category: failureCategory,
            note: buildFetchNote({ finalUrl, requestedUrl: feed.url, contentType, category: failureCategory, redirected }),
            error: buildResultError(failureCategory, 'Response did not contain a machine-readable XML feed', response.status)
          };
        } else {
          try {
            const entries = dispatchIngestionAdapter({
              body,
              feedDefinition: feed
            });
            return {
              source_id: source.source_id,
              source_name: source.display_name,
              feed_id: feed.feed_id,
              feed_label: feed.label,
              feed_url: feed.url,
              final_url: finalUrl,
              redirected,
              attempt_count: attempt,
              status: 'success',
              http_status: response.status,
              content_type: contentType,
              bytes_received: bytesReceived,
              content_type_warning: xmlExpected && !contentTypeLooksXml ? 'nonstandard_xml_content_type_tolerated' : null,
              entry_count: entries.length,
              entries,
              raw_item_count: 0,
              failure_category: null,
              note: buildFetchNote({ finalUrl, requestedUrl: feed.url, contentType, category: contentTypeLooksXml ? null : 'nonstandard_xml_content_type_tolerated', redirected }),
              error: null
            };
          } catch (error) {
            lastResult = {
              source_id: source.source_id,
              source_name: source.display_name,
              feed_id: feed.feed_id,
              feed_label: feed.label,
              feed_url: feed.url,
              final_url: finalUrl,
              redirected,
              attempt_count: attempt,
              status: 'parse_failed',
              http_status: response.status,
              content_type: contentType,
              bytes_received: bytesReceived,
              entry_count: 0,
              entries: [],
              raw_item_count: 0,
              failure_category: 'parser_failure',
              note: buildFetchNote({ finalUrl, requestedUrl: feed.url, contentType, category: 'parser_failure', redirected }),
              error: buildResultError('parser_failure', error.message, response.status)
            };
          }
        }
      }
    } catch (error) {
      const failureCategory = classifyFetchException(error);
      lastResult = {
        source_id: source.source_id,
        source_name: source.display_name,
        feed_id: feed.feed_id,
        feed_label: feed.label,
        feed_url: feed.url,
        final_url: feed.url,
        redirected: false,
        attempt_count: attempt,
        status: 'fetch_failed',
        http_status: null,
        content_type: null,
        bytes_received: 0,
        entry_count: 0,
        entries: [],
        raw_item_count: 0,
        failure_category: failureCategory,
        note: failureCategory,
        error: buildResultError(failureCategory, error?.cause?.message ?? error?.message ?? 'Unknown feed fetch error')
      };
    } finally {
      clearTimeout(timeoutHandle);
    }

    if (!shouldRetry(lastResult.failure_category, attempt, fetchRules)) {
      return lastResult;
    }
    await sleep(fetchRules.fetch.retry_backoff_ms * attempt);
  }

  return lastResult;
}

function normalizeFeedEntryToRawItem(source, feed, entry) {
  const title = entry.title?.trim() ?? '';
  const url = entry.url?.trim() ?? '';
  const publishedAt = entry.published_at?.trim() ?? '';
  const summary = entry.summary?.trim() ?? '';
  const content = entry.content?.trim() ?? '';
  const author = entry.author?.trim() ?? '';
  const warnings = [];

  if (!content || feed.content_mode === 'summary_only') {
    warnings.push('feed_summary_only');
  }
  if (!publishedAt) {
    warnings.push('missing_publication_time');
  }

  return {
    source_id: source.source_id,
    item: {
      title,
      url,
      canonical_text: feed.content_mode === 'full_text' || feed.content_mode === 'mixed'
        ? (isLikelyBodyText(content) ? content : '')
        : '',
      snippet: summary,
      summary,
      published_at: publishedAt,
      author,
      article_type: feed.expected_entry_type ?? source.expected_article_type,
      ingestion_method: feed.ingestion_method,
      source_ingestion_method: source.ingestion_method,
      language: source.language,
      source_class: source.source_class,
      feed_id: feed.feed_id,
      feed_url: feed.url,
      feed_format: feed.format,
      paywall_flag: source.paywall_policy !== 'free',
      source_display_name: source.display_name,
      source_region: source.primary_region,
      source_support_level: source.support_level,
      source_support_status: source.support_status,
      generator_warnings: warnings
    }
  };
}

function initializeRuntimeTrackers(inventory) {
  const sourceRuntime = new Map();
  const feedRuntime = new Map();

  for (const source of inventory) {
    sourceRuntime.set(source.source_id, {
      source_id: source.source_id,
      source_name: source.display_name,
      support_status: source.support_status,
      attempted_feeds: 0,
      http_fetch_successes: 0,
      parse_successes: 0,
      feeds_with_entries: 0,
      feeds_with_raw_items: 0,
      total_entries_observed: 0,
      total_raw_items_observed: 0,
      normalization_drop_count: 0,
      failure_categories: {},
      errors: []
    });

    for (const feed of source.feed_definitions) {
      feedRuntime.set(`${source.source_id}::${feed.feed_id}`, {
        source_id: source.source_id,
        feed_id: feed.feed_id,
        attempted: false,
        http_fetch_success: false,
        parse_success: false,
        entry_count: 0,
        raw_item_count: 0,
        normalization_drop_count: 0,
        last_http_status: null,
        last_content_type: null,
        final_url: feed.url,
        bytes_received: 0,
        error: null,
        failure_category: null,
        validation_status: 'not_attempted'
      });
    }
  }

  return { sourceRuntime, feedRuntime };
}

function determineFeedValidationStatus(feedState) {
  if (!feedState.attempted) {
    return 'not_attempted';
  }
  if (feedState.raw_item_count > 0) {
    return 'validated_raw_items';
  }
  if (feedState.parse_success && feedState.entry_count > 0) {
    return 'normalized_zero_items';
  }
  if (feedState.parse_success && feedState.entry_count === 0) {
    return 'fetched_zero_entries';
  }
  if (feedState.http_fetch_success) {
    return 'parse_failed';
  }
  return 'fetch_failed';
}

function determineSourceValidationStatus(source, runtime) {
  if (source.feed_definitions.length === 0) {
    return 'not_configured';
  }
  if (runtime.attempted_feeds === 0) {
    return 'not_attempted';
  }
  if (runtime.total_raw_items_observed > 0) {
    return 'validated_raw_items';
  }
  if (runtime.parse_successes > 0 && runtime.total_entries_observed > 0) {
    return 'normalized_zero_items';
  }
  if (runtime.parse_successes > 0 && runtime.total_entries_observed === 0) {
    return 'fetched_zero_entries';
  }
  if (runtime.http_fetch_successes > 0) {
    return 'parse_failed';
  }
  return 'fetch_failed';
}

function loadPreviousLedger(path) {
  const previous = readJsonIfExists(path);
  const sources = new Map();
  const feeds = new Map();
  if (!previous) {
    return { sources, feeds };
  }

  for (const source of previous.sources ?? []) {
    sources.set(source.source_id, source);
  }
  for (const feed of previous.feeds ?? []) {
    feeds.set(`${feed.source_id}::${feed.feed_id}`, feed);
  }

  return { sources, feeds };
}

function currentErrorFields(runtime, previous) {
  if (runtime.error) {
    return {
      last_error_code: runtime.error.code ?? null,
      last_error_category: runtime.failure_category ?? runtime.error.category ?? null,
      last_error_summary: runtime.error.message ?? null
    };
  }

  return {
    last_error_code: null,
    last_error_category: null,
    last_error_summary: null
  };
}

function lastFailureFields(runtime, previous) {
  if (runtime.error) {
    return {
      last_failure_at: new Date().toISOString(),
      last_failure_code: runtime.error.code ?? null,
      last_failure_category: runtime.failure_category ?? runtime.error.category ?? null,
      last_failure_summary: runtime.error.message ?? null,
      last_failure_http_status: runtime.last_http_status ?? runtime.error.http_status ?? null
    };
  }

  return {
    last_failure_at: previous?.last_failure_at ?? null,
    last_failure_code: previous?.last_failure_code ?? previous?.last_error_code ?? null,
    last_failure_category: previous?.last_failure_category ?? previous?.last_error_category ?? null,
    last_failure_summary: previous?.last_failure_summary ?? previous?.last_error_summary ?? null,
    last_failure_http_status: previous?.last_failure_http_status ?? previous?.last_http_status ?? null
  };
}

function buildFeedValidationLedger({
  inventory,
  feedResults,
  feedRuntime,
  previousLedger,
  now
}) {
  const resultsByKey = new Map(feedResults.map((result) => [`${result.source_id}::${result.feed_id}`, result]));
  const feedEntries = [];

  for (const source of inventory) {
    for (const feed of source.feed_definitions) {
      const key = `${source.source_id}::${feed.feed_id}`;
      const runtime = feedRuntime.get(key);
      const previous = previousLedger.feeds.get(key);
      const result = resultsByKey.get(key);
      const validationStatus = determineFeedValidationStatus(runtime);
      const hadFailure = validationStatus === 'fetch_failed' || validationStatus === 'parse_failed';
      const currentError = currentErrorFields(runtime, previous);
      const failureState = lastFailureFields(runtime, previous);

      feedEntries.push({
        source_id: source.source_id,
        source_name: source.display_name,
        feed_id: feed.feed_id,
        feed_label: feed.label,
        feed_url: feed.url,
        final_url: runtime.final_url ?? previous?.final_url ?? feed.url,
        adapter_type: feed.adapter_type,
        ingestion_method: feed.ingestion_method,
        configured_status: source.support_status,
        validation_status: validationStatus,
        verified_working: validationStatus === 'validated_raw_items',
        last_attempted_at: runtime.attempted ? now : previous?.last_attempted_at ?? null,
        last_fetch_success_at: runtime.http_fetch_success ? now : previous?.last_fetch_success_at ?? null,
        last_parse_success_at: runtime.parse_success ? now : previous?.last_parse_success_at ?? null,
        last_raw_item_success_at: runtime.raw_item_count > 0 ? now : previous?.last_raw_item_success_at ?? null,
        last_error_code: currentError.last_error_code,
        last_error_category: currentError.last_error_category,
        last_error_summary: currentError.last_error_summary,
        last_failure_at: failureState.last_failure_at,
        last_failure_code: failureState.last_failure_code,
        last_failure_category: failureState.last_failure_category,
        last_failure_summary: failureState.last_failure_summary,
        last_failure_http_status: failureState.last_failure_http_status,
        last_http_status: runtime.last_http_status ?? previous?.last_http_status ?? null,
        last_content_type: runtime.last_content_type ?? previous?.last_content_type ?? null,
        last_item_count_observed: runtime.raw_item_count || previous?.last_item_count_observed || 0,
        last_entry_count_observed: runtime.entry_count || previous?.last_entry_count_observed || 0,
        consecutive_failures: hadFailure
          ? (previous?.consecutive_failures ?? 0) + 1
          : runtime.attempted
            ? 0
            : (previous?.consecutive_failures ?? 0),
        validation_notes: validationStatus === 'validated_raw_items'
          ? `Generated ${runtime.raw_item_count} usable raw item(s) in the latest run.`
          : validationStatus === 'normalized_zero_items'
            ? 'Feed parse succeeded, but entries did not survive normalization into usable raw items.'
            : validationStatus === 'fetched_zero_entries'
              ? 'Feed fetch and parse succeeded, but zero entries were observed.'
              : result?.error?.message ?? previous?.validation_notes ?? 'No successful validation attempt has produced usable raw items yet.'
      });
    }
  }

  return feedEntries.sort((left, right) => `${left.source_id}::${left.feed_id}`.localeCompare(`${right.source_id}::${right.feed_id}`));
}

function buildSourceValidationLedger({
  inventory,
  sourceRuntime,
  previousLedger,
  feedLedger,
  now
}) {
  const feedLedgerBySource = new Map();
  for (const feedEntry of feedLedger) {
    const entries = feedLedgerBySource.get(feedEntry.source_id) ?? [];
    entries.push(feedEntry);
    feedLedgerBySource.set(feedEntry.source_id, entries);
  }

  const sourceEntries = [];
  for (const source of inventory) {
    const runtime = sourceRuntime.get(source.source_id);
    const previous = previousLedger.sources.get(source.source_id);
    const feedEntries = feedLedgerBySource.get(source.source_id) ?? [];
    const validationStatus = determineSourceValidationStatus(source, runtime);
    const failureStatuses = new Set(['fetch_failed', 'parse_failed']);
    const mixedFailures = feedEntries.some((entry) => failureStatuses.has(entry.validation_status));
    const latestFeedFailure = [...feedEntries]
      .filter((entry) => entry.last_failure_at)
      .sort((left, right) => String(right.last_failure_at).localeCompare(String(left.last_failure_at)))[0];
    const currentSourceError = failureStatuses.has(validationStatus)
      ? {
          last_error_code: runtime.errors.at(-1)?.code ?? latestFeedFailure?.last_error_code ?? previous?.last_error_code ?? null,
          last_error_category: runtime.errors.at(-1)?.category ?? latestFeedFailure?.last_error_category ?? previous?.last_error_category ?? null,
          last_error_summary: runtime.errors.at(-1)?.message ?? latestFeedFailure?.last_error_summary ?? previous?.last_error_summary ?? null
        }
      : {
          last_error_code: null,
          last_error_category: null,
          last_error_summary: null
        };
    const sourceFailureState = failureStatuses.has(validationStatus)
      ? {
          last_failure_at: now,
          last_failure_code: currentSourceError.last_error_code,
          last_failure_category: currentSourceError.last_error_category,
          last_failure_summary: currentSourceError.last_error_summary,
          last_failure_http_status: runtime.errors.at(-1)?.http_status ?? latestFeedFailure?.last_http_status ?? previous?.last_failure_http_status ?? previous?.last_http_status ?? null
        }
      : {
          last_failure_at: previous?.last_failure_at ?? latestFeedFailure?.last_failure_at ?? null,
          last_failure_code: previous?.last_failure_code ?? latestFeedFailure?.last_failure_code ?? previous?.last_error_code ?? null,
          last_failure_category: previous?.last_failure_category ?? latestFeedFailure?.last_failure_category ?? previous?.last_error_category ?? null,
          last_failure_summary: previous?.last_failure_summary ?? latestFeedFailure?.last_failure_summary ?? previous?.last_error_summary ?? null,
          last_failure_http_status: previous?.last_failure_http_status ?? latestFeedFailure?.last_failure_http_status ?? previous?.last_http_status ?? null
        };

    sourceEntries.push({
      source_id: source.source_id,
      source_name: source.display_name,
      adapter_type: source.adapter_type,
      ingestion_method: source.ingestion_method,
      configured_status: source.support_status,
      validation_status: validationStatus,
      verified_working: validationStatus === 'validated_raw_items',
      configured_feed_count: source.feed_definitions.length,
      attempted_feed_count: runtime.attempted_feeds,
      last_attempted_at: runtime.attempted_feeds > 0 ? now : previous?.last_attempted_at ?? null,
      last_fetch_success_at: maxIsoTimestamp(feedEntries.map((entry) => entry.last_fetch_success_at).concat(previous?.last_fetch_success_at ?? [])),
      last_parse_success_at: maxIsoTimestamp(feedEntries.map((entry) => entry.last_parse_success_at).concat(previous?.last_parse_success_at ?? [])),
      last_raw_item_success_at: maxIsoTimestamp(feedEntries.map((entry) => entry.last_raw_item_success_at).concat(previous?.last_raw_item_success_at ?? [])),
      last_error_code: currentSourceError.last_error_code,
      last_error_category: currentSourceError.last_error_category,
      last_error_summary: currentSourceError.last_error_summary,
      last_failure_at: sourceFailureState.last_failure_at,
      last_failure_code: sourceFailureState.last_failure_code,
      last_failure_category: sourceFailureState.last_failure_category,
      last_failure_summary: sourceFailureState.last_failure_summary,
      last_failure_http_status: sourceFailureState.last_failure_http_status,
      last_http_status: runtime.errors.at(-1)?.http_status ?? previous?.last_http_status ?? null,
      last_item_count_observed: runtime.total_raw_items_observed || previous?.last_item_count_observed || 0,
      consecutive_failures: failureStatuses.has(validationStatus)
        ? (previous?.consecutive_failures ?? 0) + 1
        : runtime.attempted_feeds > 0
          ? 0
          : (previous?.consecutive_failures ?? 0),
      validation_notes: validationStatus === 'validated_raw_items'
        ? `Generated ${runtime.total_raw_items_observed} usable raw item(s) across ${runtime.feeds_with_raw_items} feed(s).${mixedFailures ? ' Some feed definitions still failed in the same run.' : ''}`
        : validationStatus === 'normalized_zero_items'
          ? 'At least one feed parsed with entries, but no usable raw items survived normalization.'
          : validationStatus === 'fetched_zero_entries'
            ? 'At least one feed fetched and parsed successfully, but zero entries were observed.'
            : validationStatus === 'not_configured'
              ? 'No active governed feed definitions are configured for this source.'
              : runtime.errors.at(-1)?.message ?? previous?.validation_notes ?? 'No successful validation attempt has produced usable raw items yet.'
    });
  }

  return sourceEntries.sort((left, right) => left.source_id.localeCompare(right.source_id));
}

function buildValidationLedger({
  inventory,
  feedResults,
  feedRuntime,
  sourceRuntime,
  previousLedger,
  now
}) {
  const feeds = buildFeedValidationLedger({
    inventory,
    feedResults,
    feedRuntime,
    previousLedger,
    now
  });
  const sources = buildSourceValidationLedger({
    inventory,
    sourceRuntime,
    previousLedger,
    feedLedger: feeds,
    now
  });

  return {
    generated_at: now,
    validation_status_model: {
      not_configured: 'No active governed feed definitions are configured for the source or feed.',
      not_attempted: 'A governed feed is configured, but this run did not attempt it.',
      fetch_failed: 'The run attempted the feed, but HTTP/network fetch did not succeed.',
      parse_failed: 'The feed was fetched over HTTP, but parsing failed.',
      fetched_zero_entries: 'Fetch and parse succeeded, but zero feed entries were observed.',
      normalized_zero_items: 'Entries were observed, but no usable raw items survived normalization.',
      validated_raw_items: 'The run produced one or more usable raw items from the feed/source.'
    },
    counts_by_source_validation_status: countBy(sources, (source) => source.validation_status),
    counts_by_feed_validation_status: countBy(feeds, (feed) => feed.validation_status),
    sources,
    feeds
  };
}

function buildFetchReport({
  now,
  inventory,
  results,
  rawItems,
  malformedEntryCount,
  duplicateActions,
  sourcesWithEntriesButNoUsableItems,
  sourcesReturningZeroEntries,
  validationLedger
}) {
  const itemCountsPerSource = countBy(rawItems, (item) => item.source_id);
  const itemCountsPerSourceClass = countBy(rawItems, (item) => item.item.source_class);
  const sourceValidationCounts = validationLedger.counts_by_source_validation_status;
  const failureCategoryCounts = countBy(
    results.filter((result) => result.failure_category),
    (result) => result.failure_category
  );

  return {
    generated_at: now,
    governed_sources_total: inventory.length,
    configured_sources_total: inventory.filter((source) => source.feed_definitions.length > 0).length,
    non_configured_sources_total: sourceValidationCounts.not_configured ?? 0,
    attempted_sources_total: validationLedger.sources.filter((source) => source.attempted_feed_count > 0).length,
    attempted_sources_with_zero_entries: validationLedger.sources.filter((source) => source.validation_status === 'fetched_zero_entries').length,
    attempted_sources_with_entries_but_zero_normalized_items: validationLedger.sources.filter((source) => source.validation_status === 'normalized_zero_items').length,
    attempted_sources_with_raw_items: validationLedger.sources.filter((source) => source.validation_status === 'validated_raw_items').length,
    configured_feed_definitions: inventory.reduce((count, source) => count + source.feed_definitions.length, 0),
    attempted_feed_definitions: results.length,
    http_fetch_successes: results.filter((result) => result.http_status !== null && result.status !== 'fetch_failed').length,
    http_fetch_failures: results.filter((result) => result.status === 'fetch_failed').length,
    fetch_successes: results.filter((result) => result.http_status !== null && result.status !== 'fetch_failed').length,
    fetch_failures: results.filter((result) => result.status === 'fetch_failed').length,
    parse_successes: results.filter((result) => result.status === 'success').length,
    parse_failures: results.filter((result) => result.status === 'parse_failed').length,
    feeds_with_entries: validationLedger.feeds.filter((feed) => feed.last_entry_count_observed > 0 && feed.validation_status !== 'not_attempted').length,
    feeds_yielding_usable_raw_items: validationLedger.feeds.filter((feed) => feed.validation_status === 'validated_raw_items').length,
    fetch_failures_by_category: failureCategoryCounts,
    runtime_failures_by_category: failureCategoryCounts,
    malformed_entry_count: malformedEntryCount,
    duplicate_raw_item_count: duplicateActions.length,
    generated_raw_item_count: rawItems.length,
    item_counts_per_source: itemCountsPerSource,
    item_counts_per_source_class: itemCountsPerSourceClass,
    sources_returning_zero_entries: Array.from(sourcesReturningZeroEntries).sort(),
    sources_with_entries_but_no_usable_raw_items: Array.from(sourcesWithEntriesButNoUsableItems).sort(),
    feeds: results.map((result) => ({
      source_id: result.source_id,
      source_name: result.source_name,
      feed_id: result.feed_id,
      feed_label: result.feed_label,
      feed_url: result.feed_url,
      final_url: result.final_url ?? result.feed_url,
      redirected: result.redirected ?? false,
      status: result.status,
      http_status: result.http_status,
      content_type: result.content_type ?? null,
      bytes_received: result.bytes_received ?? 0,
      attempt_count: result.attempt_count ?? 1,
      failure_category: result.failure_category ?? null,
      entry_count: result.entry_count,
      raw_item_count: result.raw_item_count ?? 0,
      note: result.note ?? '',
      error: result.error
    })),
    duplicate_actions: duplicateActions
  };
}

function buildCoverageGapReport({
  now,
  inventory,
  fetchReport,
  validationLedger
}) {
  return {
    generated_at: now,
    total_governed_sources: inventory.length,
    sources_by_support_status: Object.fromEntries(
      Object.entries(countBy(inventory, (source) => source.support_status))
        .sort(([left], [right]) => left.localeCompare(right))
    ),
    sources_by_validation_status: Object.fromEntries(
      Object.entries(validationLedger.counts_by_source_validation_status)
        .sort(([left], [right]) => left.localeCompare(right))
    ),
    sources_with_zero_feed_definitions: inventory
      .filter((source) => source.feed_definitions.length === 0)
      .map((source) => source.source_id),
    fetch_failed_sources: validationLedger.sources
      .filter((source) => source.validation_status === 'fetch_failed')
      .map((source) => source.source_id),
    parse_failed_sources: validationLedger.sources
      .filter((source) => source.validation_status === 'parse_failed')
      .map((source) => source.source_id),
    zero_entry_sources: fetchReport.sources_returning_zero_entries,
    sources_with_entries_but_no_usable_raw_items: fetchReport.sources_with_entries_but_no_usable_raw_items,
    unsupported_or_pending_sources: inventory
      .filter((source) => !FETCHABLE_SUPPORT_STATUSES.has(source.support_status))
      .map((source) => ({
        source_id: source.source_id,
        display_name: source.display_name,
        source_class: source.source_class,
        language: source.language,
        primary_region: source.primary_region,
        support_status: source.support_status,
        notes: source.notes
      }))
  };
}

function buildGenerationReport({
  now,
  inventory,
  rawItems,
  fetchReport,
  validationLedger,
  liveInputRules
}) {
  const supportStatusCounts = countBy(inventory, (source) => source.support_status);
  const warnings = [];

  if (fetchReport.http_fetch_failures > 0 || fetchReport.parse_failures > 0) {
    warnings.push('one_or_more_feeds_failed');
  }
  if (fetchReport.sources_with_entries_but_no_usable_raw_items.length > 0) {
    warnings.push('one_or_more_sources_generated_only_unusable_entries');
  }
  if (rawItems.length === 0) {
    warnings.push('no_live_raw_items_generated');
  }

  return {
    generated_at: now,
    total_governed_sources: inventory.length,
    support_status_counts: supportStatusCounts,
    validation_status_counts: validationLedger.counts_by_source_validation_status,
    feed_validation_status_counts: validationLedger.counts_by_feed_validation_status,
    configured_sources_total: fetchReport.configured_sources_total,
    non_configured_sources_total: fetchReport.non_configured_sources_total,
    attempted_sources_total: fetchReport.attempted_sources_total,
    attempted_sources_with_zero_entries: fetchReport.attempted_sources_with_zero_entries,
    attempted_sources_with_entries_but_zero_normalized_items: fetchReport.attempted_sources_with_entries_but_zero_normalized_items,
    attempted_sources_with_raw_items: fetchReport.attempted_sources_with_raw_items,
    configured_feed_count: inventory.reduce((count, source) => count + source.feed_definitions.length, 0),
    attempted_feed_count: fetchReport.attempted_feed_definitions,
    http_fetch_successes: fetchReport.http_fetch_successes,
    http_fetch_failures: fetchReport.http_fetch_failures,
    fetch_failures_by_category: fetchReport.fetch_failures_by_category,
    runtime_failures_by_category: fetchReport.runtime_failures_by_category,
    parse_successes: fetchReport.parse_successes,
    parse_failures: fetchReport.parse_failures,
    feeds_with_entries: fetchReport.feeds_with_entries,
    feeds_yielding_usable_raw_items: fetchReport.feeds_yielding_usable_raw_items,
    generated_raw_item_count: rawItems.length,
    malformed_entry_count: fetchReport.malformed_entry_count,
    duplicate_raw_item_count: fetchReport.duplicate_raw_item_count,
    live_validated_feed_definition_count: validationLedger.feeds.filter((feed) => feed.validation_status === 'validated_raw_items').length,
    operationally_validated_source_count: validationLedger.sources.filter((source) => source.validation_status === 'validated_raw_items').length,
    success_ladder_source_ids: liveInputRules.success_ladder_source_ids,
    warning_flags: warnings
  };
}

function buildRuntimeDebugReport({
  now,
  validationLedger,
  fetchReport,
  liveInputRules,
  rawItemClassDistribution
}) {
  const feedRows = fetchReport.feeds
    .map((feed) => ({
      source_id: feed.source_id,
      source_name: feed.source_name,
      feed_id: feed.feed_id,
      feed_label: feed.feed_label,
      requested_url: feed.feed_url,
      final_url: feed.final_url,
      http_status: feed.http_status,
      content_type: feed.content_type,
      bytes_received: feed.bytes_received,
      validation_status: validationLedger.feeds.find((entry) => entry.source_id === feed.source_id && entry.feed_id === feed.feed_id)?.validation_status ?? 'not_attempted',
      error_classification: feed.failure_category,
      entry_count: feed.entry_count,
      normalized_raw_item_count: feed.raw_item_count,
      note: feed.note
    }))
    .sort((left, right) => `${left.source_id}::${left.feed_id}`.localeCompare(`${right.source_id}::${right.feed_id}`));

  const sourceRows = validationLedger.sources
    .map((source) => ({
      source_id: source.source_id,
      source_name: source.source_name,
      configured_status: source.configured_status,
      validation_status: source.validation_status,
      verified_working: source.verified_working,
      attempted_feed_count: source.attempted_feed_count,
      last_http_status: source.last_http_status,
      last_error_category: source.last_error_category ?? null,
      last_item_count_observed: source.last_item_count_observed,
      note: source.validation_notes
    }))
    .sort((left, right) => left.source_id.localeCompare(right.source_id));

  const verifiedWorkingSources = sourceRows.filter((source) => source.verified_working);
  const successLadder = liveInputRules.success_ladder_source_ids.map((sourceId) => {
    const source = sourceRows.find((entry) => entry.source_id === sourceId) ?? {
      source_id: sourceId,
      source_name: sourceId,
      configured_status: 'missing',
      validation_status: 'not_configured',
      verified_working: false,
      last_item_count_observed: 0,
      last_error_category: 'not_configured'
    };
    const feedRowsForSource = feedRows.filter((feed) => feed.source_id === sourceId);
    return {
      source_id: source.source_id,
      source_name: source.source_name,
      configured_status: source.configured_status,
      validation_status: source.validation_status,
      verified_working: source.verified_working,
      feed_fetch_success: feedRowsForSource.some((feed) => feed.http_status !== null && feed.validation_status !== 'fetch_failed'),
      parse_success: feedRowsForSource.some((feed) => SUCCESS_VALIDATION_STAGES.has(feed.validation_status)),
      raw_item_success: source.validation_status === 'validated_raw_items',
      raw_item_count: source.last_item_count_observed ?? 0,
      failure_categories: [...new Set(feedRowsForSource.map((feed) => feed.error_classification).filter(Boolean))]
    };
  });

  return {
    generated_at: now,
    source_rows: sourceRows,
    feed_rows: feedRows,
    verified_working_sources: verifiedWorkingSources,
    success_ladder: successLadder,
    top_raw_item_source_classes: rawItemClassDistribution.item_counts_per_source_class
  };
}

function buildOperationalSummary({
  generationReport,
  fetchReport,
  rawItemClassDistribution,
  validationLedger,
  runtimeReport
}) {
  const topRawItemClasses = Object.entries(rawItemClassDistribution.item_counts_per_source_class)
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, 3)
    .map(([sourceClass, count]) => ({ source_class: sourceClass, count }));

  return {
    governed_sources_total: generationReport.total_governed_sources,
    configured_sources_total: generationReport.configured_sources_total,
    non_configured_sources_total: generationReport.non_configured_sources_total,
    attempted_sources_total: generationReport.attempted_sources_total,
    configured_sources_by_support_status: generationReport.support_status_counts,
    validation_counts_by_source_status: validationLedger.counts_by_source_validation_status,
    validation_counts_by_feed_status: validationLedger.counts_by_feed_validation_status,
    configured_feed_definitions: generationReport.configured_feed_count,
    feed_requests_attempted: fetchReport.attempted_feed_definitions,
    http_fetch_successes: fetchReport.http_fetch_successes,
    http_fetch_failures: fetchReport.http_fetch_failures,
    fetch_failures_by_category: fetchReport.fetch_failures_by_category,
    runtime_failure_categories: fetchReport.runtime_failures_by_category,
    parse_successes: fetchReport.parse_successes,
    parse_failures: fetchReport.parse_failures,
    attempted_sources_with_zero_entries: fetchReport.attempted_sources_with_zero_entries,
    attempted_sources_with_entries_but_zero_normalized_items: fetchReport.attempted_sources_with_entries_but_zero_normalized_items,
    attempted_sources_with_raw_items: fetchReport.attempted_sources_with_raw_items,
    raw_items_generated: generationReport.generated_raw_item_count,
    top_raw_item_source_classes: topRawItemClasses,
    verified_working_sources: runtimeReport.verified_working_sources.slice(0, 10),
    success_ladder: runtimeReport.success_ladder,
    warning_flags: generationReport.warning_flags
  };
}

export async function generateLiveInputArtifacts({
  inventory = null,
  sourcesPath = DEFAULT_SOURCES_PATH,
  feedOverridesPath = DEFAULT_FEED_OVERRIDES_PATH,
  rulesPath = DEFAULT_RULES_PATH,
  outputDir = DEFAULT_OUTPUT_DIR,
  fetchImpl = globalThis.fetch,
  liveInputRules = null,
  now = new Date(),
  timeoutMs = null
} = {}) {
  if (typeof fetchImpl !== 'function') {
    throw new Error('generateLiveInputArtifacts requires a fetch-compatible implementation');
  }

  const fetchRules = liveInputRules ?? loadLiveInputRules(rulesPath);

  const inventoryRecords = normalizeInventoryRecords(
    inventory ?? buildFeedInventory({
      sourcesPath,
      feedOverridesPath
    })
  );
  const timestamp = now.toISOString();
  const previousLedgerPath = resolve(outputDir, 'governed_source_validation_ledger.json');
  const previousLedger = loadPreviousLedger(previousLedgerPath);
  const feedResults = [];
  const rawItems = [];
  const sourcesReturningZeroEntries = new Set();
  const sourcesWithEntriesButNoUsableItems = new Set();
  const { sourceRuntime, feedRuntime } = initializeRuntimeTrackers(inventoryRecords);
  let malformedEntryCount = 0;

  for (const source of inventoryRecords) {
    if (source.active_status !== 'active' || !FETCHABLE_SUPPORT_STATUSES.has(source.support_status)) {
      continue;
    }

    let sourceHadEntries = false;

    for (const feed of source.feed_definitions) {
      if (feed.active_status !== 'active') {
        continue;
      }

      const sourceState = sourceRuntime.get(source.source_id);
      const feedState = feedRuntime.get(`${source.source_id}::${feed.feed_id}`);
      sourceState.attempted_feeds += 1;
      feedState.attempted = true;

      const result = await fetchFeed({
        feed,
        source,
        fetchImpl,
        timeoutMs: timeoutMs ?? fetchRules.fetch.timeout_ms,
        fetchRules
      });
      feedResults.push(result);
      feedState.last_http_status = result.http_status;
      feedState.last_content_type = result.content_type ?? null;
      feedState.final_url = result.final_url ?? feed.url;
      feedState.bytes_received = result.bytes_received ?? 0;
      feedState.failure_category = result.failure_category ?? null;

      if (result.status === 'fetch_failed') {
        feedState.error = result.error;
        sourceState.errors.push({
          code: result.error?.code ?? 'FETCH_FAILED',
          category: result.failure_category ?? result.error?.category ?? 'connection_error',
          message: result.error?.message ?? 'Feed fetch failed',
          http_status: result.http_status
        });
        incrementCounter(sourceState.failure_categories, result.failure_category ?? 'fetch_failed');
        continue;
      }

      feedState.http_fetch_success = true;
      sourceState.http_fetch_successes += 1;

      if (result.status === 'parse_failed') {
        feedState.error = result.error;
        sourceState.errors.push({
          code: result.error?.code ?? 'PARSE_FAILED',
          category: result.failure_category ?? result.error?.category ?? 'parser_failure',
          message: result.error?.message ?? 'Feed parse failed',
          http_status: result.http_status
        });
        incrementCounter(sourceState.failure_categories, result.failure_category ?? 'parse_failed');
        continue;
      }

      feedState.parse_success = true;
      feedState.entry_count = result.entry_count;
      sourceState.parse_successes += 1;
      sourceState.total_entries_observed += result.entry_count;

      if (result.entry_count === 0) {
        sourcesReturningZeroEntries.add(source.source_id);
      } else {
        sourceHadEntries = true;
        sourceState.feeds_with_entries += 1;
      }

      let feedUsableItems = 0;
      let feedNormalizationDrops = 0;

      for (const entry of result.entries) {
        const rawEntry = normalizeFeedEntryToRawItem(source, feed, entry);
        if (!rawEntry.item.title || !rawEntry.item.url) {
          malformedEntryCount += 1;
          feedNormalizationDrops += 1;
          sourceState.normalization_drop_count += 1;
          continue;
        }

        rawItems.push(rawEntry);
        feedUsableItems += 1;
      }

      feedState.raw_item_count = feedUsableItems;
      result.raw_item_count = feedUsableItems;
      feedState.normalization_drop_count = feedNormalizationDrops;
      sourceState.total_raw_items_observed += feedUsableItems;
      if (feedUsableItems > 0) {
        sourceState.feeds_with_raw_items += 1;
      } else if (result.entry_count > 0) {
        result.failure_category = 'normalization_zero_items';
        result.note = 'Entries parsed, but no usable raw items survived normalization';
        incrementCounter(sourceState.failure_categories, 'normalization_zero_items');
      }
    }

    if (sourceHadEntries && sourceRuntime.get(source.source_id).total_raw_items_observed === 0) {
      sourcesWithEntriesButNoUsableItems.add(source.source_id);
    }
  }

  for (const source of inventoryRecords) {
    const state = sourceRuntime.get(source.source_id);
    for (const feed of source.feed_definitions) {
      const feedState = feedRuntime.get(`${source.source_id}::${feed.feed_id}`);
      feedState.validation_status = determineFeedValidationStatus(feedState);
    }
    state.validation_status = determineSourceValidationStatus(source, state);
  }

  const deduped = dedupeRawItems(rawItems);
  const validationLedger = buildValidationLedger({
    inventory: inventoryRecords,
    feedResults,
    feedRuntime,
    sourceRuntime,
    previousLedger,
    now: timestamp
  });
  const rawItemSourceDistribution = {
    generated_at: timestamp,
    item_counts_per_source: countBy(deduped.rawItems, (item) => item.source_id)
  };
  const rawItemLanguageDistribution = {
    generated_at: timestamp,
    item_counts_per_language: countBy(deduped.rawItems, (item) => item.item.language)
  };
  const rawItemClassDistribution = {
    generated_at: timestamp,
    item_counts_per_source_class: countBy(deduped.rawItems, (item) => item.item.source_class)
  };
  const fetchReport = buildFetchReport({
    now: timestamp,
    inventory: inventoryRecords,
    results: feedResults,
    rawItems: deduped.rawItems,
    malformedEntryCount,
    duplicateActions: deduped.duplicateActions,
    sourcesWithEntriesButNoUsableItems,
    sourcesReturningZeroEntries,
    validationLedger
  });
  const generationReport = buildGenerationReport({
    now: timestamp,
    inventory: inventoryRecords,
    rawItems: deduped.rawItems,
    fetchReport,
    validationLedger,
    liveInputRules: fetchRules
  });
  const runtimeReport = buildRuntimeDebugReport({
    now: timestamp,
    validationLedger,
    fetchReport,
    liveInputRules: fetchRules,
    rawItemClassDistribution
  });
  const coverageGapReport = buildCoverageGapReport({
    now: timestamp,
    inventory: inventoryRecords,
    fetchReport,
    validationLedger
  });
  const operationalSummary = buildOperationalSummary({
    generationReport,
    fetchReport,
    rawItemClassDistribution,
    validationLedger,
    runtimeReport
  });
  const governedInventoryReport = buildGovernedInventoryReport(inventoryRecords, timestamp, validationLedger);
  const feedInventoryReport = buildFeedInventoryReport(inventoryRecords, timestamp);

  const rawItemsPath = writeJson(resolve(outputDir, 'raw-items.json'), deduped.rawItems);
  const timestampedRawItemsPath = writeJson(resolve(outputDir, `raw-items-${timestamp.slice(0, 10)}.json`), deduped.rawItems);
  const governedInventoryReportPath = writeJson(resolve(outputDir, 'governed_source_inventory_report.json'), governedInventoryReport);
  const feedInventoryReportPath = writeJson(resolve(outputDir, 'feed_inventory_report.json'), feedInventoryReport);
  const feedFetchReportPath = writeJson(resolve(outputDir, 'feed_fetch_report.json'), fetchReport);
  const liveInputGenerationReportPath = writeJson(resolve(outputDir, 'live_input_generation_report.json'), generationReport);
  const sourceCoverageGapReportPath = writeJson(resolve(outputDir, 'source_coverage_gap_report.json'), coverageGapReport);
  const rawItemSourceDistributionPath = writeJson(resolve(outputDir, 'raw_item_source_distribution.json'), rawItemSourceDistribution);
  const rawItemLanguageDistributionPath = writeJson(resolve(outputDir, 'raw_item_language_distribution.json'), rawItemLanguageDistribution);
  const rawItemClassDistributionPath = writeJson(resolve(outputDir, 'raw_item_class_distribution.json'), rawItemClassDistribution);
  const validationLedgerPath = writeJson(resolve(outputDir, 'governed_source_validation_ledger.json'), validationLedger);
  const runtimeReportPath = writeJson(resolve(outputDir, 'live_input_runtime_report.json'), runtimeReport);
  const operationalSummaryPath = writeJson(resolve(outputDir, 'live_input_operational_summary.json'), operationalSummary);

  return {
    inventory: inventoryRecords,
    rawItems: deduped.rawItems,
    reports: {
      governedInventory: governedInventoryReport,
      inventory: feedInventoryReport,
      fetch: fetchReport,
      generation: generationReport,
      coverageGap: coverageGapReport,
      validationLedger,
      runtimeReport,
      rawItemSourceDistribution,
      rawItemLanguageDistribution,
      rawItemClassDistribution,
      operationalSummary
    },
    paths: {
      rawItems: rawItemsPath,
      timestampedRawItems: timestampedRawItemsPath,
      governedInventoryReport: governedInventoryReportPath,
      inventoryReport: feedInventoryReportPath,
      fetchReport: feedFetchReportPath,
      generationReport: liveInputGenerationReportPath,
      coverageGapReport: sourceCoverageGapReportPath,
      validationLedger: validationLedgerPath,
      runtimeReport: runtimeReportPath,
      operationalSummary: operationalSummaryPath,
      rawItemSourceDistribution: rawItemSourceDistributionPath,
      rawItemLanguageDistribution: rawItemLanguageDistributionPath,
      rawItemClassDistribution: rawItemClassDistributionPath
    }
  };
}

export function rankValidationStatus(status) {
  const index = SOURCE_VALIDATION_STATUS_PRIORITY.indexOf(status);
  return index === -1 ? SOURCE_VALIDATION_STATUS_PRIORITY.length : index;
}

export function rankFeedValidationStatus(status) {
  const index = FEED_VALIDATION_STATUS_PRIORITY.indexOf(status);
  return index === -1 ? FEED_VALIDATION_STATUS_PRIORITY.length : index;
}
