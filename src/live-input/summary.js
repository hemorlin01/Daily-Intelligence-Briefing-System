function orderedPairs(counts) {
  return Object.entries(counts ?? {})
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .map(([key, value]) => `${key}=${value}`);
}

export function formatLiveInputOperationalSummary(summary) {
  const topClasses = (summary.top_raw_item_source_classes ?? [])
    .map((entry) => `${entry.source_class}=${entry.count}`)
    .join(', ');
  const warningFlags = summary.warning_flags?.length ? summary.warning_flags.join(', ') : 'none';

  return [
    'DIBS Live Input Summary',
    `governed sources: ${summary.governed_sources_total}`,
    `configured sources: ${summary.configured_sources_total}`,
    `non-configured sources: ${summary.non_configured_sources_total}`,
    `attempted sources: ${summary.attempted_sources_total}`,
    `support_status: ${orderedPairs(summary.configured_sources_by_support_status).join(', ')}`,
    `validation_status (sources): ${orderedPairs(summary.validation_counts_by_source_status).join(', ')}`,
    `validation_status (feeds): ${orderedPairs(summary.validation_counts_by_feed_status).join(', ')}`,
    `configured feed definitions: ${summary.configured_feed_definitions}`,
    `feed requests attempted: ${summary.feed_requests_attempted}`,
    `http fetch successes: ${summary.http_fetch_successes}`,
    `http fetch failures: ${summary.http_fetch_failures}`,
    `failure categories: ${orderedPairs(summary.runtime_failure_categories ?? summary.fetch_failures_by_category).join(', ') || 'none'}`,
    `parse successes: ${summary.parse_successes}`,
    `parse failures: ${summary.parse_failures}`,
    `attempted sources with zero entries: ${summary.attempted_sources_with_zero_entries}`,
    `attempted sources with entries but zero normalized items: ${summary.attempted_sources_with_entries_but_zero_normalized_items}`,
    `attempted sources with raw items: ${summary.attempted_sources_with_raw_items}`,
    `raw items generated: ${summary.raw_items_generated}`,
    `top raw-item classes: ${topClasses || 'none'}`,
    `warning flags: ${warningFlags}`
  ].join('\n');
}

export function formatLiveInputRuntimeTable(runtimeReport) {
  const lines = ['Core Success Ladder'];
  for (const source of runtimeReport.success_ladder ?? []) {
    const failureText = source.failure_categories?.length ? source.failure_categories.join('|') : 'none';
    lines.push(
      `${source.source_id}: fetch=${source.feed_fetch_success ? 'yes' : 'no'}, parse=${source.parse_success ? 'yes' : 'no'}, raw_items=${source.raw_item_success ? source.raw_item_count : 0}, validation_status=${source.validation_status}, failures=${failureText}`
    );
  }

  const sampleFailures = (runtimeReport.feed_rows ?? [])
    .filter((feed) => feed.error_classification)
    .slice(0, 12)
    .map((feed) => `${feed.source_id}/${feed.feed_id}: ${feed.error_classification}${feed.http_status ? ` (http ${feed.http_status})` : ''}`);

  lines.push('Sample Feed Failures');
  if (sampleFailures.length === 0) {
    lines.push('none');
  } else {
    lines.push(...sampleFailures);
  }

  return lines.join('\n');
}
