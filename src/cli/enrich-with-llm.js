import dotenv from 'dotenv';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { enrichRawItems } from '../llm/enrich-raw-items.js';

dotenv.config({ path: resolve(process.cwd(), '.env'), quiet: true });

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = true;
    } else {
      args[key] = next;
      i += 1;
    }
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  const inputPath = args.input
    ? resolve(process.cwd(), args.input)
    : resolve(process.cwd(), 'artifacts', 'inputs', 'raw-items.json');

  const outputPath = args.output || inputPath;

  const apiKey = args['api-key'] || process.env.DEEPSEEK_API_KEY || process.env.LLM_API_KEY;
  const apiUrl = args['api-url'] || process.env.LLM_API_URL || 'https://api.deepseek.com/v1/chat/completions';
  const model = args.model || process.env.LLM_MODEL || 'deepseek-chat';
  const dryRun = Boolean(args['dry-run']);

  if (!apiKey && !dryRun) {
    process.stderr.write('Error: DEEPSEEK_API_KEY is required. Set it via --api-key or DEEPSEEK_API_KEY env var.\n');
    process.stderr.write('For dry-run: --dry-run\n');
    process.exit(1);
  }

  const rawItems = JSON.parse(readFileSync(inputPath, 'utf8'));
  console.log(`[enrich] Loaded ${rawItems.length} raw items from ${inputPath}`);

  const result = await enrichRawItems(rawItems, {
    apiKey,
    apiUrl,
    model,
    dryRun,
    onProgress: (done, total) => {
      console.log(`[enrich] Progress: ${done}/${total} batches`);
    }
  });

  if (!dryRun) {
    mkdirSync(dirname(resolve(process.cwd(), outputPath)), { recursive: true });
    writeFileSync(resolve(process.cwd(), outputPath), JSON.stringify(result.enriched, null, 2));
    console.log(`[enrich] Wrote ${result.enriched.length} items to ${outputPath}`);
  }

  process.stdout.write(JSON.stringify(result.stats, null, 2));
}

main().catch((error) => {
  process.stderr.write(`[enrich] FATAL: ${error.message}\n`);
  if (error.stack) process.stderr.write(`${error.stack}\n`);
  process.exit(1);
});
