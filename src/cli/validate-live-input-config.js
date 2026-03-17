import { resolve } from 'node:path';
import { loadLiveInputRules } from '../config/load-config.js';
import { buildFeedInventory } from '../live-input/generate-live-inputs.js';

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

function buildCounts(inventory, field) {
  const counts = {};
  for (const source of inventory) {
    const key = String(source[field]);
    counts[key] = (counts[key] ?? 0) + 1;
  }
  return counts;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const inventory = buildFeedInventory({
    sourcesPath: args.sources
      ? resolve(process.cwd(), args.sources)
      : resolve(process.cwd(), 'config', 'sources.json'),
    feedOverridesPath: args['feed-overrides']
      ? resolve(process.cwd(), args['feed-overrides'])
      : resolve(process.cwd(), 'config', 'feed-overrides.json')
  });
  const liveInputRules = loadLiveInputRules(
    args.rules
      ? resolve(process.cwd(), args.rules)
      : resolve(process.cwd(), 'config', 'live-input-rules.json')
  );
  const sourceIds = new Set(inventory.map((source) => source.source_id));
  const unknownSuccessLadderSources = liveInputRules.success_ladder_source_ids.filter((sourceId) => !sourceIds.has(sourceId));

  if (unknownSuccessLadderSources.length > 0) {
    throw new Error(`live-input rules reference unknown success ladder source ids: ${unknownSuccessLadderSources.join(', ')}`);
  }

  process.stdout.write(JSON.stringify({
    total_governed_sources: inventory.length,
    counts_by_support_status: buildCounts(inventory, 'support_status'),
    counts_by_source_class: buildCounts(inventory, 'source_class'),
    counts_by_ingestion_method: buildCounts(inventory, 'ingestion_method'),
    configured_feed_count: inventory.reduce((count, source) => count + source.feed_definitions.length, 0),
    success_ladder_source_ids: liveInputRules.success_ladder_source_ids,
    sources_with_inconsistent_ingestion_metadata: inventory
      .filter((source) => source.feed_definitions.length > 0 && source.ingestion_method === 'none')
      .map((source) => source.source_id),
    zero_feed_definition_sources: inventory.filter((source) => source.feed_definitions.length === 0).map((source) => source.source_id)
  }, null, 2));
}

try {
  main();
} catch (error) {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
}
