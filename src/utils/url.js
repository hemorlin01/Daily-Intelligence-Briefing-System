export function isValidUrl(value) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    return false;
  }

  try {
    const parsed = new URL(value);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

export function canonicalizeUrl(value, policy = 'standard') {
  if (!isValidUrl(value)) {
    return null;
  }

  const parsed = new URL(value);
  parsed.hash = '';
  parsed.hostname = parsed.hostname.toLowerCase();

  if (parsed.hostname.startsWith('www.')) {
    parsed.hostname = parsed.hostname.slice(4);
  }

  if ((parsed.protocol === 'https:' && parsed.port === '443') || (parsed.protocol === 'http:' && parsed.port === '80')) {
    parsed.port = '';
  }

  if (policy !== 'preserve-query') {
    const paramsToDrop = [
      'fbclid',
      'gclid',
      'mc_cid',
      'mc_eid',
      'ref',
      'source',
      'utm_campaign',
      'utm_content',
      'utm_medium',
      'utm_source',
      'utm_term'
    ];

    for (const key of paramsToDrop) {
      parsed.searchParams.delete(key);
    }
  }

  if (parsed.pathname.length > 1) {
    parsed.pathname = parsed.pathname.replace(/\/+$/, '');
  }

  return parsed.toString();
}
