export function safeTrim(value) {
  return typeof value === 'string' ? value.trim() : '';
}

export function hasValue(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

export function normalizeWhitespace(value) {
  return hasValue(value) ? value.replace(/\s+/g, ' ').trim() : '';
}

export function normalizeTitleForComparison(title) {
  return normalizeWhitespace(title)
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function tokenize(value) {
  const normalized = normalizeTitleForComparison(value);
  return normalized.length === 0 ? [] : normalized.split(' ');
}

export function buildTokenFingerprint(value, maxLength = 600) {
  return tokenize(normalizeWhitespace(value).slice(0, maxLength));
}

export function diceCoefficient(left, right) {
  const normalizedLeft = normalizeTitleForComparison(left);
  const normalizedRight = normalizeTitleForComparison(right);
  if (!normalizedLeft || !normalizedRight) {
    return 0;
  }

  if (normalizedLeft === normalizedRight) {
    return 1;
  }

  const bigrams = (text) => {
    const grams = [];
    for (let index = 0; index < text.length - 1; index += 1) {
      grams.push(text.slice(index, index + 2));
    }
    return grams;
  };

  const leftBigrams = bigrams(normalizedLeft);
  const rightBigrams = bigrams(normalizedRight);
  const rightCounts = new Map();

  for (const gram of rightBigrams) {
    rightCounts.set(gram, (rightCounts.get(gram) ?? 0) + 1);
  }

  let overlap = 0;
  for (const gram of leftBigrams) {
    const count = rightCounts.get(gram) ?? 0;
    if (count > 0) {
      overlap += 1;
      rightCounts.set(gram, count - 1);
    }
  }

  return (2 * overlap) / (leftBigrams.length + rightBigrams.length);
}

export function jaccardSimilarity(leftTokens, rightTokens) {
  const leftSet = new Set(leftTokens);
  const rightSet = new Set(rightTokens);
  if (leftSet.size === 0 || rightSet.size === 0) {
    return 0;
  }

  let intersection = 0;
  for (const token of leftSet) {
    if (rightSet.has(token)) {
      intersection += 1;
    }
  }

  const union = new Set([...leftSet, ...rightSet]).size;
  return union === 0 ? 0 : intersection / union;
}
