import { resolve } from 'node:path';
import { loadSourceCatalog, loadThresholds } from '../config/load-config.js';
import { buildCandidatePoolReport, writeDiagnostics } from './diagnostics.js';
import { deduplicateCandidates } from './deduplicate.js';
import { classifyCandidate } from './filter.js';
import { normalizeRawItem } from './normalize.js';

const DEFAULT_SOURCES_PATH = resolve(process.cwd(), 'config', 'sources.json');
const DEFAULT_THRESHOLDS_PATH = resolve(process.cwd(), 'config', 'thresholds.json');

function buildWarnings(report) {
  const warnings = [];
  if (report.main_candidate_pool_size === 0) {
    warnings.push('main_candidate_pool_empty');
  }
  if (report.backup_pool_size > report.main_candidate_pool_size) {
    warnings.push('backup_pool_larger_than_main_pool');
  }
  if ((report.rejection_reasons_breakdown.stale_item ?? 0) > 0) {
    warnings.push('stale_items_rejected');
  }
  if ((report.rejection_reasons_breakdown.non_article_like ?? 0) > 0) {
    warnings.push('non_article_like_items_rejected');
  }
  return warnings;
}

export function buildCandidatePools({
  rawItems,
  now = new Date(),
  fetchedAt = now.toISOString(),
  outputDir = null,
  sourcesPath = DEFAULT_SOURCES_PATH,
  thresholdsPath = DEFAULT_THRESHOLDS_PATH
}) {
  const catalog = loadSourceCatalog(sourcesPath);
  const rules = loadThresholds(thresholdsPath);
  const normalizedItems = [];
  const rejected = [];
  const candidateEntries = [];

  for (const entry of rawItems) {
    const source = catalog.sources.get(entry.source_id);
    const record = normalizeRawItem({
      source: source ?? {
        source_id: entry.source_id,
        display_name: entry.source_id,
        source_class: 'unknown',
        language: 'unknown',
        priority_tier: 3,
        fetch_method: 'manual',
        canonicalization_policy: 'standard',
        paywall_policy: 'free',
        expected_article_type: 'news',
        reliability_status: 'experimental',
        default_topic_affinities: [],
        active_status: 'inactive'
      },
      rawItem: entry.item,
      rules,
      fetchedAt
    });

    normalizedItems.push(record);
    const decision = classifyCandidate({ record, source, rules, now });
    if (decision.disposition === 'rejected') {
      rejected.push({
        record,
        reasons: decision.reasons,
        warnings: decision.warnings,
        malformed: decision.malformed
      });
      continue;
    }

    candidateEntries.push({
      record: {
        ...record,
        candidate_disposition: decision.disposition,
        candidate_warnings: decision.warnings,
        age_hours: decision.age_hours
      },
      disposition: decision.disposition
    });
  }

  const deduplicated = deduplicateCandidates(candidateEntries.map((entry) => entry.record), rules);
  const mainPool = deduplicated.candidates.filter((record) => record.candidate_disposition === 'main');
  const backupPool = deduplicated.candidates.filter((record) => record.candidate_disposition === 'backup');
  const deduplicationActions = deduplicated.actions;

  const ingestionDebug = {
    run_timestamp: fetchedAt,
    normalized_items: normalizedItems,
    rejected_items: rejected
  };
  const deduplicationReport = {
    run_timestamp: fetchedAt,
    actions: deduplicationActions
  };
  const reportWithoutWarnings = buildCandidatePoolReport({
    runTimestamp: fetchedAt,
    sourceCatalog: catalog,
    rawItems,
    normalizedItems,
    mainPool,
    backupPool,
    rejected,
    deduplicationActions,
    warnings: []
  });
  const candidatePoolReport = {
    ...reportWithoutWarnings,
    warnings: buildWarnings(reportWithoutWarnings)
  };

  if (outputDir) {
    writeDiagnostics({
      outputDir,
      candidatePoolReport,
      ingestionDebug,
      deduplicationReport
    });
  }

  return {
    runTimestamp: fetchedAt,
    catalogVersion: catalog.catalogVersion,
    mainPool,
    backupPool,
    rejected,
    normalizedItems,
    candidatePoolReport,
    ingestionDebug,
    deduplicationReport
  };
}
