function decodeEntities(value) {
  if (typeof value !== 'string') {
    return '';
  }

  return value
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCodePoint(Number.parseInt(code, 16)))
    .replace(/&#([0-9]+);/g, (_, code) => String.fromCodePoint(Number.parseInt(code, 10)))
    .replace(/&nbsp;/gi, ' ')
    .replace(/&ndash;/gi, '-')
    .replace(/&mdash;/gi, '--')
    .replace(/&ldquo;|&rdquo;/gi, '"')
    .replace(/&lsquo;|&rsquo;/gi, "'")
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&apos;/gi, "'");
}

function stripHtml(value) {
  return decodeEntities(value)
    .replace(/<script\b[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function escapeTag(tagName) {
  return tagName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function extractFirstTagValue(block, tagNames) {
  for (const tagName of tagNames) {
    const pattern = new RegExp(`<${escapeTag(tagName)}\\b[^>]*>([\\s\\S]*?)<\\/${escapeTag(tagName)}>`, 'i');
    const match = block.match(pattern);
    if (match) {
      return decodeEntities(match[1]).trim();
    }
  }
  return '';
}

function extractAtomLink(block) {
  const matches = [...block.matchAll(/<link\b([^>]*?)\/?>/gi)];
  for (const match of matches) {
    const attributes = match[1] ?? '';
    const href = attributes.match(/\bhref=["']([^"']+)["']/i)?.[1];
    const rel = attributes.match(/\brel=["']([^"']+)["']/i)?.[1] ?? 'alternate';
    if (href && (rel === 'alternate' || rel === 'self')) {
      return href;
    }
  }
  return '';
}

function normalizePublishedAt(value) {
  if (!value) {
    return '';
  }
  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.valueOf())) {
    return '';
  }
  return timestamp.toISOString();
}

function normalizeUrl(value) {
  const normalized = stripHtml(value);
  if (!normalized) {
    return '';
  }
  try {
    return new URL(normalized).toString();
  } catch {
    return normalized;
  }
}

function hasRootTag(xml, tagNames) {
  return tagNames.some((tagName) => new RegExp(`<${escapeTag(tagName)}\\b`, 'i').test(xml));
}

function hasClosingTag(xml, tagNames) {
  return tagNames.some((tagName) => new RegExp(`<\\/${escapeTag(tagName)}>`, 'i').test(xml));
}

function parseRssFeed(xml) {
  const items = [...xml.matchAll(/<item\b[\s\S]*?<\/item>/gi)];
  return items.map((match) => {
    const block = match[0];
    const link = extractFirstTagValue(block, ['link']);
    const guid = extractFirstTagValue(block, ['guid', 'id']);
    return {
      title: stripHtml(extractFirstTagValue(block, ['title'])),
      url: normalizeUrl(link || guid),
      summary: stripHtml(extractFirstTagValue(block, ['description', 'summary'])),
      content: stripHtml(extractFirstTagValue(block, ['content:encoded', 'content', 'content:content'])),
      author: stripHtml(extractFirstTagValue(block, ['dc:creator', 'author', 'creator'])),
      published_at: normalizePublishedAt(extractFirstTagValue(block, ['pubDate', 'published', 'date', 'dc:date'])),
      entry_id: stripHtml(guid)
    };
  });
}

function parseAtomFeed(xml) {
  const entries = [...xml.matchAll(/<entry\b[\s\S]*?<\/entry>/gi)];
  return entries.map((match) => {
    const block = match[0];
    return {
      title: stripHtml(extractFirstTagValue(block, ['title'])),
      url: normalizeUrl(decodeEntities(extractAtomLink(block)).trim()),
      summary: stripHtml(extractFirstTagValue(block, ['summary', 'subtitle'])),
      content: stripHtml(extractFirstTagValue(block, ['content'])),
      author: stripHtml(extractFirstTagValue(block, ['name', 'author'])),
      published_at: normalizePublishedAt(extractFirstTagValue(block, ['updated', 'published'])),
      entry_id: stripHtml(extractFirstTagValue(block, ['id']))
    };
  });
}

export function parseFeedXml(xml, feedFormat = 'rss') {
  if (typeof xml !== 'string' || xml.trim().length === 0) {
    throw new Error('Feed XML must be a non-empty string');
  }

  if (feedFormat === 'rss') {
    const entries = parseRssFeed(xml);
    if (entries.length === 0 && !hasRootTag(xml, ['rss', 'rdf:RDF'])) {
      throw new Error('RSS feed contained no parsable <item> entries');
    }
    if (entries.length === 0 && !hasClosingTag(xml, ['rss', 'rdf:RDF'])) {
      throw new Error('RSS feed XML appears malformed or truncated');
    }
    return entries;
  }

  if (feedFormat === 'atom') {
    const entries = parseAtomFeed(xml);
    if (entries.length === 0 && !hasRootTag(xml, ['feed'])) {
      throw new Error('Atom feed contained no parsable <entry> entries');
    }
    if (entries.length === 0 && !hasClosingTag(xml, ['feed'])) {
      throw new Error('Atom feed XML appears malformed or truncated');
    }
    return entries;
  }

  throw new Error(`Unsupported feed format "${feedFormat}"`);
}
