import { mkdirSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

function defaultLedger() {
  return {
    version: 1,
    updated_at: null,
    channel_attempts: {},
    runs: {}
  };
}

function safeDestination(channel, destination) {
  if (!destination) {
    return destination;
  }
  if (channel === 'bark') {
    return 'bark-device';
  }
  if (channel === 'wecom') {
    return 'wecom-webhook';
  }
  return destination;
}

function sanitizeLedger(ledger) {
  for (const entry of Object.values(ledger.channel_attempts ?? {})) {
    entry.destination = safeDestination(entry.channel, entry.destination);
  }
  for (const run of Object.values(ledger.runs ?? {})) {
    for (const [channel, entry] of Object.entries(run.channels ?? {})) {
      entry.destination = safeDestination(channel, entry.destination);
    }
  }
  return ledger;
}

export function loadDeliveryLedger(ledgerPath) {
  const resolvedPath = resolve(process.cwd(), ledgerPath);
  if (!existsSync(resolvedPath)) {
    return defaultLedger();
  }

  return sanitizeLedger(JSON.parse(readFileSync(resolvedPath, 'utf8')));
}

export function saveDeliveryLedger(ledgerPath, ledger) {
  const resolvedPath = resolve(process.cwd(), ledgerPath);
  mkdirSync(dirname(resolvedPath), { recursive: true });
  writeFileSync(resolvedPath, JSON.stringify({
    ...ledger,
    updated_at: new Date().toISOString()
  }, null, 2));
  return resolvedPath;
}

export function getChannelLedgerEntry(ledger, idempotencyKey) {
  return ledger.channel_attempts[idempotencyKey] ?? null;
}

export function recordChannelAttempt(ledger, entry) {
  ledger.channel_attempts[entry.idempotency_key] = entry;
  const runEntry = ledger.runs[entry.run_id] ?? {
    run_id: entry.run_id,
    channels: {}
  };
  runEntry.channels[entry.channel] = {
    idempotency_key: entry.idempotency_key,
    status: entry.status,
    attempt_count: entry.attempt_count,
    last_attempt_timestamp: entry.last_attempt_timestamp,
    success: entry.success,
    dry_run: entry.dry_run,
    actual_send: entry.actual_send,
    skipped_duplicate: entry.skipped_duplicate,
    destination: entry.destination
  };
  ledger.runs[entry.run_id] = runEntry;
}
