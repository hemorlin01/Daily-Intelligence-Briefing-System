import { resolve } from 'node:path';
import { generateLiveInputArtifacts } from '../live-input/generate-live-inputs.js';

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) {
      continue;
    }

    const key = token.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith('--')) {
      args[key] = true;
      continue;
    }

    args[key] = next;
    index += 1;
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const now = args.now ? new Date(args.now) : new Date();
  const result = await generateLiveInputArtifacts({
    sourcesPath: args['sources']
      ? resolve(process.cwd(), args.sources)
      : resolve(process.cwd(), 'config', 'sources.json'),
    feedOverridesPath: args['feed-overrides']
      ? resolve(process.cwd(), args['feed-overrides'])
      : resolve(process.cwd(), 'config', 'feed-overrides.json'),
    rulesPath: args.rules
      ? resolve(process.cwd(), args.rules)
      : resolve(process.cwd(), 'config', 'live-input-rules.json'),
    outputDir: args.output
      ? resolve(process.cwd(), args.output)
      : resolve(process.cwd(), 'artifacts', 'inputs'),
    now
  });

  process.stdout.write(JSON.stringify({
    raw_items_path: result.paths.rawItems,
    generated_raw_item_count: result.rawItems.length,
    support_status_counts: result.reports.generation.support_status_counts,
    validation_status_counts: result.reports.generation.validation_status_counts,
    configured_feed_count: result.reports.generation.configured_feed_count,
    attempted_feed_count: result.reports.generation.attempted_feed_count,
    attempted_sources_total: result.reports.generation.attempted_sources_total,
    attempted_sources_with_zero_entries: result.reports.generation.attempted_sources_with_zero_entries,
    attempted_sources_with_entries_but_zero_normalized_items: result.reports.generation.attempted_sources_with_entries_but_zero_normalized_items,
    attempted_sources_with_raw_items: result.reports.generation.attempted_sources_with_raw_items,
    fetch_failures_by_category: result.reports.generation.fetch_failures_by_category,
    runtime_failure_categories: result.reports.generation.runtime_failures_by_category,
    live_validated_feed_definition_count: result.reports.generation.live_validated_feed_definition_count,
    operationally_validated_source_count: result.reports.generation.operationally_validated_source_count,
    governed_inventory_report_path: result.paths.governedInventoryReport,
    inventory_report_path: result.paths.inventoryReport,
    fetch_report_path: result.paths.fetchReport,
    generation_report_path: result.paths.generationReport,
    coverage_gap_report_path: result.paths.coverageGapReport,
    validation_ledger_path: result.paths.validationLedger,
    runtime_report_path: result.paths.runtimeReport,
    operational_summary_path: result.paths.operationalSummary
  }, null, 2));
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
