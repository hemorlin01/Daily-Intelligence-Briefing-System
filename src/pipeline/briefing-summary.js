import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';

function readJson(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

export function findLatestRunDirectory(outputRoot = resolve(process.cwd(), 'artifacts', 'runs')) {
  const directories = readdirSync(outputRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => {
      const fullPath = join(outputRoot, entry.name);
      return {
        fullPath,
        modified: statSync(fullPath).mtimeMs
      };
    })
    .sort((left, right) => right.modified - left.modified);

  return directories[0]?.fullPath ?? null;
}

function formatCounts(label, counts) {
  const entries = Object.entries(counts ?? {});
  if (entries.length === 0) {
    return `${label}: none`;
  }

  return `${label}: ${entries
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .map(([key, count]) => `${key}=${count}`)
    .join(', ')}`;
}

export function loadBriefingSummaryReports(runDir) {
  return {
    candidateFunnel: readJson(join(runDir, 'briefing_candidate_funnel.json')),
    selectionReport: readJson(join(runDir, 'briefing_selection_report.json')),
    statusReport: readJson(join(runDir, 'briefing_status_report.json')),
    languageReport: readJson(join(runDir, 'final_briefing_language_report.json')),
    contentQualityReport: readJson(join(runDir, 'content_quality_report.json'))
  };
}

export function formatBriefingOperationalSummary({ candidateFunnel, selectionReport, statusReport, languageReport, contentQualityReport, runDir }) {
  const sectionSummary = selectionReport.selected_sections.length === 0
    ? 'none'
    : selectionReport.selected_sections.map((section) => `${section.label}=${section.item_count}`).join(', ');
  const topSources = selectionReport.top_selected_governed_sources.length === 0
    ? 'none'
    : selectionReport.top_selected_governed_sources.map((entry) => `${entry.key}=${entry.count}`).join(', ');
  const degradedReasons = (statusReport.degraded_reasons ?? []).length === 0
    ? 'none'
    : statusReport.degraded_reasons.join(', ');
  const languageCounts = languageReport?.selected_items_by_language
    ? formatCounts('language counts', languageReport.selected_items_by_language)
    : 'language counts: none';
  const summaryLengthAvg = contentQualityReport
    ? `avg summary length (en/zh): ${contentQualityReport.summary_length_avg.en}/${contentQualityReport.summary_length_avg.zh}`
    : 'avg summary length (en/zh): n/a';

  return [
    'DIBS Briefing Summary',
    `run directory: ${runDir}`,
    `raw items: ${candidateFunnel.raw_items_in}`,
    `post-filter main candidates: ${candidateFunnel.post_filter_items}`,
    `backup candidates: ${candidateFunnel.backup_pool_items}`,
    `semantic cards: ${candidateFunnel.semantic_cards}`,
    `post-ranking candidates: ${candidateFunnel.post_ranking_items}`,
    `above score threshold: ${candidateFunnel.above_threshold_items}`,
    `section allocation candidates: ${candidateFunnel.section_allocation_candidates}`,
    `final selected items: ${candidateFunnel.final_selected_items}`,
    `final sections: ${candidateFunnel.final_sections}`,
    `run status: ${statusReport.run_status}`,
    statusReport.run_status === 'degraded' ? `degraded reasons: ${degradedReasons}` : 'degraded reasons: none',
    languageCounts,
    summaryLengthAvg,
    formatCounts('section counts', selectionReport.selected_domain_counts),
    `top selected governed sources: ${topSources}`,
    formatCounts('warning flags', Object.fromEntries((statusReport.warning_flags ?? []).map((flag) => [flag, 1])))
  ].join('\n');
}
