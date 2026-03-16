import { mkdirSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { buildCandidatePools } from '../pipeline/build-candidate-pools.js';

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) {
      continue;
    }

    const key = token.slice(2);
    const value = argv[index + 1];
    args[key] = value;
    index += 1;
  }
  return args;
}

const args = parseArgs(process.argv.slice(2));
if (!args.input || !args.output) {
  throw new Error('Usage: node src/cli/run-phase1.js --input <raw-items.json> --output <directory> [--now <iso-time>]');
}

const rawItems = JSON.parse(readFileSync(resolve(process.cwd(), args.input), 'utf8'));
const outputDir = resolve(process.cwd(), args.output);
mkdirSync(outputDir, { recursive: true });

const result = buildCandidatePools({
  rawItems,
  now: args.now ? new Date(args.now) : new Date(),
  outputDir
});

process.stdout.write(JSON.stringify({
  run_timestamp: result.runTimestamp,
  main_candidate_pool_size: result.mainPool.length,
  backup_pool_size: result.backupPool.length,
  rejected_item_count: result.rejected.length,
  diagnostics_directory: outputDir
}, null, 2));
