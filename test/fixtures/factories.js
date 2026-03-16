export const FIXED_NOW = new Date('2026-03-16T12:00:00.000Z');

function repeatText(seed, count) {
  return Array.from({ length: count }, () => seed).join(' ');
}

export function hoursAgoIso(hours) {
  return new Date(FIXED_NOW.getTime() - (hours * 36e5)).toISOString();
}

export function makeRawItem(overrides = {}) {
  return {
    title: 'Markets steady as central banks monitor inflation',
    url: 'https://www.example.com/articles/market-update',
    canonical_text: repeatText('Central banks monitored inflation while markets stayed steady across major regions.', 30),
    snippet: repeatText('Markets stayed steady as investors watched inflation signals.', 8),
    summary: repeatText('Investors watched inflation and central bank signals.', 8),
    published_at: hoursAgoIso(3),
    author: 'Staff Reporter',
    article_type: 'news',
    ...overrides
  };
}

export function makeRawEntry(sourceId, overrides = {}) {
  return {
    source_id: sourceId,
    item: makeRawItem(overrides)
  };
}
