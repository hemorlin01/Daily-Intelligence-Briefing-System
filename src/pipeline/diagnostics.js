import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

function incrementCounter(target, key) {
  target[key] = (target[key] ?? 0) + 1;
}

function buildExtractionDistribution(records) {
  const buckets = {
    '0.00-0.24': 0,
    '0.25-0.49': 0,
    '0.50-0.74': 0,
    '0.75-1.00': 0
  };

  for (const record of records) {
    const score = record.extraction_quality_score;
    if (score < 0.25) {
      buckets['0.00-0.24'] += 1;
    } else if (score < 0.5) {
      buckets['0.25-0.49'] += 1;
    } else if (score < 0.75) {
      buckets['0.50-0.74'] += 1;
    } else {
      buckets['0.75-1.00'] += 1;
    }
  }

  return buckets;
}

export function buildCandidatePoolReport({
  runTimestamp,
  sourceCatalog,
  rawItems,
  normalizedItems,
  mainPool,
  backupPool,
  rejected,
  deduplicationActions,
  warnings
}) {
  const rejectionReasonsBreakdown = {};
  const perSourceCounts = {};
  const perSourceClassCounts = {};
  const deduplicationSummary = {};
  const configuredSourceClassCounts = {};

  for (const source of sourceCatalog.sources.values()) {
    perSourceCounts[source.source_id] = {
      display_name: source.display_name,
      source_class: source.source_class,
      active_status: source.active_status,
      normalized: 0,
      main: 0,
      backup: 0,
      rejected: 0
    };

    const classEntry = perSourceClassCounts[source.source_class] ?? {
      configured_sources: 0,
      normalized_items: 0,
      main_candidates: 0,
      backup_candidates: 0,
      rejected_items: 0
    };
    classEntry.configured_sources += 1;
    perSourceClassCounts[source.source_class] = classEntry;
    incrementCounter(configuredSourceClassCounts, source.source_class);
  }

  for (const item of normalizedItems) {
    const sourceEntry = perSourceCounts[item.source_id] ?? {
      display_name: item.source_display_name ?? item.source_id,
      source_class: item.source_class ?? 'unknown',
      active_status: item.source_id === 'unknown' ? 'inactive' : 'unknown',
      normalized: 0,
      main: 0,
      backup: 0,
      rejected: 0
    };
    sourceEntry.normalized += 1;
    perSourceCounts[item.source_id] = sourceEntry;

    const classEntry = perSourceClassCounts[item.source_class ?? 'unknown'] ?? {
      configured_sources: 0,
      normalized_items: 0,
      main_candidates: 0,
      backup_candidates: 0,
      rejected_items: 0
    };
    classEntry.normalized_items += 1;
    perSourceClassCounts[item.source_class ?? 'unknown'] = classEntry;
  }

  for (const item of mainPool) {
    perSourceCounts[item.source_id].main += 1;
    perSourceClassCounts[item.source_class].main_candidates += 1;
  }

  for (const item of backupPool) {
    perSourceCounts[item.source_id].backup += 1;
    perSourceClassCounts[item.source_class].backup_candidates += 1;
  }

  for (const item of rejected) {
    if (perSourceCounts[item.record.source_id]) {
      perSourceCounts[item.record.source_id].rejected += 1;
    }
    if (perSourceClassCounts[item.record.source_class]) {
      perSourceClassCounts[item.record.source_class].rejected_items += 1;
    }
    for (const reason of item.reasons) {
      incrementCounter(rejectionReasonsBreakdown, reason);
    }
  }

  for (const action of deduplicationActions) {
    incrementCounter(deduplicationSummary, action.reason);
  }

  return {
    run_timestamp: runTimestamp,
    configured_source_count: sourceCatalog.sources.size,
    active_source_count: Array.from(sourceCatalog.sources.values()).filter((source) => source.active_status === 'active').length,
    configured_source_class_counts: configuredSourceClassCounts,
    total_raw_items_fetched: rawItems.length,
    total_normalized_items: normalizedItems.length,
    main_candidate_pool_size: mainPool.length,
    backup_pool_size: backupPool.length,
    rejected_item_count: rejected.length,
    rejection_reasons_breakdown: rejectionReasonsBreakdown,
    per_source_counts: perSourceCounts,
    per_source_class_counts: perSourceClassCounts,
    extraction_quality_distribution: buildExtractionDistribution(normalizedItems),
    deduplication_actions_summary: deduplicationSummary,
    stale_item_count: rejectionReasonsBreakdown.stale_item ?? 0,
    malformed_item_count: rejected.filter((item) => item.malformed).length,
    warnings
  };
}

export function writeDiagnostics({ outputDir, candidatePoolReport, ingestionDebug, deduplicationReport }) {
  mkdirSync(outputDir, { recursive: true });

  writeFileSync(
    resolve(outputDir, 'candidate_pool_report.json'),
    JSON.stringify(candidatePoolReport, null, 2)
  );
  writeFileSync(
    resolve(outputDir, 'ingestion_debug.json'),
    JSON.stringify(ingestionDebug, null, 2)
  );
  writeFileSync(
    resolve(outputDir, 'deduplication_report.json'),
    JSON.stringify(deduplicationReport, null, 2)
  );
}
