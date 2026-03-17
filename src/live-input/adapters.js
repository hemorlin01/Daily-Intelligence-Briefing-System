import { parseFeedXml } from './parse-feed.js';

function decodeHtml(value) {
  return value
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCodePoint(Number.parseInt(code, 16)))
    .replace(/&#([0-9]+);/g, (_, code) => String.fromCodePoint(Number.parseInt(code, 10)))
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&apos;/gi, "'");
}

function stripHtml(value) {
  return decodeHtml(value)
    .replace(/<script\b[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractPatternValue(block, patternSource) {
  if (!patternSource) {
    return '';
  }
  const pattern = new RegExp(patternSource, 'i');
  const match = block.match(pattern);
  return stripHtml(match?.groups?.value ?? match?.[1] ?? '');
}

function extractHtmlListingEntries(body, feedDefinition) {
  const patterns = feedDefinition.patterns ?? {};
  if (!patterns.entry_pattern || !patterns.link_pattern || !patterns.title_pattern) {
    throw new Error('html_listing adapter requires entry_pattern, link_pattern, and title_pattern');
  }

  const entryPattern = new RegExp(patterns.entry_pattern, 'gi');
  const entries = [];
  const entryBlocks = [...body.matchAll(entryPattern)];
  for (const match of entryBlocks) {
    const block = match?.groups?.value ?? match?.[1] ?? match[0];
    const url = extractPatternValue(block, patterns.link_pattern);
    const title = extractPatternValue(block, patterns.title_pattern);
    entries.push({
      url,
      title,
      summary: extractPatternValue(block, patterns.summary_pattern),
      author: extractPatternValue(block, patterns.author_pattern),
      published_at: extractPatternValue(block, patterns.date_pattern),
      content: ''
    });
  }

  if (entries.length === 0) {
    throw new Error('HTML listing contained no parsable entries');
  }

  return entries;
}

const ADAPTERS = Object.freeze({
  xml_feed: ({ body, feedDefinition }) => parseFeedXml(body, feedDefinition.format ?? 'rss'),
  html_listing: ({ body, feedDefinition }) => extractHtmlListingEntries(body, feedDefinition)
});

export function dispatchIngestionAdapter({ body, feedDefinition }) {
  const adapterType = feedDefinition.adapter_type ?? 'xml_feed';
  const adapter = ADAPTERS[adapterType];
  if (!adapter) {
    throw new Error(`Unsupported ingestion adapter "${adapterType}"`);
  }
  return adapter({ body, feedDefinition });
}
