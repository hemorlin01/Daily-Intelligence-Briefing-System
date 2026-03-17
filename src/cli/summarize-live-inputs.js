import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { formatLiveInputOperationalSummary, formatLiveInputRuntimeTable } from '../live-input/summary.js';

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

function main() {
  const args = parseArgs(process.argv.slice(2));
  const summaryPath = args.summary
    ? resolve(process.cwd(), args.summary)
    : resolve(process.cwd(), 'artifacts', 'inputs', 'live_input_operational_summary.json');
  const runtimePath = args.runtime
    ? resolve(process.cwd(), args.runtime)
    : resolve(process.cwd(), 'artifacts', 'inputs', 'live_input_runtime_report.json');
  const summary = JSON.parse(readFileSync(summaryPath, 'utf8'));
  const runtime = JSON.parse(readFileSync(runtimePath, 'utf8'));
  process.stdout.write(`${formatLiveInputOperationalSummary(summary)}\n\n${formatLiveInputRuntimeTable(runtime)}\n`);
}

try {
  main();
} catch (error) {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
}
