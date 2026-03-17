import { resolve } from 'node:path';
import { findLatestRunDirectory, formatBriefingOperationalSummary, loadBriefingSummaryReports } from '../pipeline/briefing-summary.js';

function parseArgs(argv) {
  const args = { runDir: null };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--run-dir') {
      args.runDir = argv[index + 1] ?? null;
      index += 1;
    }
  }
  return args;
}

const args = parseArgs(process.argv.slice(2));
const runDir = args.runDir
  ? resolve(process.cwd(), args.runDir)
  : findLatestRunDirectory();

if (!runDir) {
  throw new Error('No run directory was found to summarize.');
}

const reports = loadBriefingSummaryReports(runDir);
console.log(formatBriefingOperationalSummary({
  ...reports,
  runDir
}));
