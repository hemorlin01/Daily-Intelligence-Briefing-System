import dotenv from 'dotenv';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { executeDibsRun, executeScheduledDibsRun, retryRunBundleDelivery } from '../pipeline/execute-dibs-run.js';

dotenv.config({ path: resolve(process.cwd(), '.env'), quiet: true });

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
  const deliveryRulesPath = args['delivery-rules']
    ? resolve(process.cwd(), args['delivery-rules'])
    : resolve(process.cwd(), 'config', 'delivery-rules.json');

  const channels = args.channels
    ? args.channels.split(',').map((value) => value.trim()).filter(Boolean)
    : ['email', 'telegram'];

  if (args['retry-run-bundle']) {
    const result = await retryRunBundleDelivery({
      runBundlePath: resolve(process.cwd(), args['retry-run-bundle']),
      deliveryRulesPath,
      dryRun: Boolean(args['dry-run']),
      replay: Boolean(args.replay),
      channels
    });

    process.stdout.write(JSON.stringify({
      run_id: result.runBundle.run_id,
      final_outcome: result.deliveryStatus.final_outcome,
      channels: result.deliveryStatus.channels
    }, null, 2));
    return;
  }

  if (!args.input) {
    throw new Error('Usage: node src/cli/run-dibs.js --input <raw-items.json> [--scheduled] [--dry-run] [--replay] [--channels email,telegram] [--retry-run-bundle <run_bundle.json>]');
  }

  const rawItems = JSON.parse(readFileSync(resolve(process.cwd(), args.input), 'utf8'));
  const now = args.now ? new Date(args.now) : new Date();
  const commonOptions = {
    rawItems,
    now,
    runTimestamp: now.toISOString(),
    deliveryRulesPath,
    renderingRulesPath: args['rendering-rules']
      ? resolve(process.cwd(), args['rendering-rules'])
      : resolve(process.cwd(), 'config', 'rendering-rules.json'),
    dryRun: Boolean(args['dry-run']),
    replay: Boolean(args.replay),
    channels
  };

  const result = args.scheduled
    ? await executeScheduledDibsRun(commonOptions)
    : await executeDibsRun(commonOptions);

  process.stdout.write(JSON.stringify(result.scheduled === false ? {
    scheduled: false,
    scheduler_debug_path: result.schedulerDebugPath,
    decision: result.schedulerDebug
  } : {
    run_id: result.runBundle.run_id,
    final_outcome: result.deliveryStatus.final_outcome,
    output_dir: result.outputDir,
    channels: result.deliveryStatus.channels
  }, null, 2));
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
