import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { loadDeliveryRules } from '../config/load-config.js';
import { EmailDeliveryAdapter } from '../delivery/email-adapter.js';
import { TelegramDeliveryAdapter } from '../delivery/telegram-adapter.js';
import { getChannelLedgerEntry, loadDeliveryLedger, recordChannelAttempt, saveDeliveryLedger } from '../delivery/state-store.js';
import { createRunBundle, validateRunBundle } from '../models/run-bundle.js';
import { buildCandidatePools } from './build-candidate-pools.js';
import { buildEditorialSelection } from './build-editorial-selection.js';
import { buildSemanticCards } from './build-semantic-cards.js';
import { renderBriefing } from './render-briefing.js';
import { evaluateSchedule, writeSchedulerDebug } from '../scheduler/schedule.js';

const DEFAULT_DELIVERY_RULES_PATH = resolve(process.cwd(), 'config', 'delivery-rules.json');

function hashContent(value) {
  return createHash('sha256').update(value).digest('hex');
}

function makeRunId(runTimestamp) {
  return `dibs-${runTimestamp.replace(/[:.]/g, '-').replace(/Z$/, 'Z')}`;
}

function writeJson(path, value) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(value, null, 2));
  return path;
}

function resolveChannelDestination(channelConfig, environment) {
  return environment[channelConfig.destination_env] ?? channelConfig.destination_fallback;
}

function buildRunBundle({
  runId,
  runTimestamp,
  selectionResult,
  rendered,
  outputDir,
  deliveryRules,
  environment
}) {
  const diagnosticsReferences = {
    candidate_pool_report: resolve(outputDir, 'candidate_pool_report.json'),
    semantic_diagnostics: resolve(outputDir, 'semantic_diagnostics.json'),
    editorial_selection_debug: resolve(outputDir, 'editorial_selection_debug.json'),
    rendering_debug: resolve(outputDir, 'rendering_debug.json'),
    delivery_status: resolve(outputDir, 'delivery_status.json'),
    run_log: resolve(outputDir, 'run_log.json')
  };

  const runFingerprint = hashContent(JSON.stringify({
    run_id: runId,
    run_status: selectionResult.run_status,
    selected_article_ids: selectionResult.selected_article_ids,
    email: rendered.email.content,
    telegram: rendered.telegram.content
  }));

  const bundle = createRunBundle({
    run_id: runId,
    run_timestamp: runTimestamp,
    run_status: selectionResult.run_status,
    selected_count: selectionResult.selected_count,
    selected_article_ids: selectionResult.selected_article_ids,
    output_dir: outputDir,
    artifacts: {
      email: {
        content: rendered.email.content,
        path: resolve(outputDir, 'briefing_email.txt')
      },
      telegram: {
        content: rendered.telegram.content,
        path: resolve(outputDir, 'briefing_telegram.txt')
      },
      markdown: {
        content: rendered.markdown.content,
        path: resolve(outputDir, 'briefing_archive.md')
      }
    },
    delivery_targets: {
      email: {
        enabled: deliveryRules.delivery.email.enabled,
        mode: deliveryRules.delivery.email.mode,
        destination: resolveChannelDestination(deliveryRules.delivery.email, environment)
      },
      telegram: {
        enabled: deliveryRules.delivery.telegram.enabled,
        mode: deliveryRules.delivery.telegram.mode,
        destination: resolveChannelDestination(deliveryRules.delivery.telegram, environment)
      }
    },
    diagnostics_references: diagnosticsReferences,
    idempotency: {
      run_fingerprint: runFingerprint,
      per_channel: {
        email: hashContent(`${runId}:email:${rendered.email.content}`),
        telegram: hashContent(`${runId}:telegram:${rendered.telegram.content}`)
      }
    }
  }, selectionResult, rendered);

  return bundle;
}

function defaultAdapters(deliveryRules, adapterOverrides) {
  return {
    email: adapterOverrides.email ?? new EmailDeliveryAdapter({ mode: deliveryRules.delivery.email.mode }),
    telegram: adapterOverrides.telegram ?? new TelegramDeliveryAdapter({ mode: deliveryRules.delivery.telegram.mode })
  };
}

function buildSkippedDuplicateResult({ channel, target, priorEntry, now, dryRun, providerMode }) {
  return {
    channel,
    destination: target.destination,
    status: 'duplicate_blocked',
    success: true,
    dry_run: dryRun,
    actual_send: false,
    attempt_timestamp: now,
    artifact_path: priorEntry?.artifact_path ?? null,
    error: null,
    retryable: false,
    skipped_duplicate: true,
    provider_mode: providerMode
  };
}

function updateLedgerForResult({
  ledger,
  bundle,
  channel,
  target,
  result,
  attemptCount,
  idempotencyKey
}) {
  recordChannelAttempt(ledger, {
    run_id: bundle.run_id,
    channel,
    idempotency_key: idempotencyKey,
    attempt_count: attemptCount,
    last_attempt_timestamp: result.attempt_timestamp,
    status: result.status,
    success: result.success,
    dry_run: result.dry_run,
    actual_send: result.actual_send,
    skipped_duplicate: result.skipped_duplicate,
    destination: target.destination,
    artifact_path: result.artifact_path,
    error: result.error
  });
}

async function deliverChannel({
  bundle,
  channel,
  target,
  adapter,
  retryRules,
  ledger,
  replay,
  dryRun,
  now
}) {
  const idempotencyKey = bundle.idempotency.per_channel[channel];
  const priorEntry = getChannelLedgerEntry(ledger, idempotencyKey);
  const providerMode = target.mode;

  if (!target.enabled) {
    return {
      channel,
      destination: target.destination,
      status: 'disabled',
      success: false,
      dry_run: dryRun,
      actual_send: false,
      attempt_timestamp: now,
      artifact_path: null,
      error: null,
      retryable: false,
      skipped_duplicate: false,
      provider_mode: providerMode,
      attempt_count: priorEntry?.attempt_count ?? 0,
      retry_count: 0
    };
  }

  if (!replay && !dryRun && priorEntry?.success) {
    const duplicateResult = buildSkippedDuplicateResult({
      channel,
      target,
      priorEntry,
      now,
      dryRun,
      providerMode
    });
    updateLedgerForResult({
      ledger,
      bundle,
      channel,
      target,
      result: duplicateResult,
      attemptCount: priorEntry.attempt_count,
      idempotencyKey
    });
    return {
      ...duplicateResult,
      attempt_count: priorEntry.attempt_count,
      retry_count: 0
    };
  }

  let attemptCount = priorEntry?.attempt_count ?? 0;
  let retryCount = 0;
  let finalResult = null;

  while (attemptCount < retryRules.max_attempts) {
    attemptCount += 1;
    const attemptTimestamp = new Date().toISOString();
    const result = await adapter.deliver({
      bundle,
      destination: target.destination,
      dryRun,
      now: attemptTimestamp
    });

    finalResult = result;
    if (result.success || result.status === 'dry_run') {
      break;
    }

    const retryableByConfig = result.error?.code && retryRules.retryable_error_codes.includes(result.error.code);
    if (!(result.retryable || retryableByConfig) || attemptCount >= retryRules.max_attempts) {
      break;
    }

    retryCount += 1;
  }

  updateLedgerForResult({
    ledger,
    bundle,
    channel,
    target,
    result: finalResult,
    attemptCount,
    idempotencyKey
  });

  return {
    ...finalResult,
    attempt_count: attemptCount,
    retry_count: retryCount
  };
}

function finalRunOutcome(channelResults, dryRun) {
  const results = Object.values(channelResults);
  if (results.every((result) => result.status === 'dry_run')) {
    return dryRun ? 'dry_run' : 'no_send';
  }

  const succeeded = results.filter((result) => result.success || result.status === 'duplicate_blocked').length;
  const failed = results.filter((result) => result.status === 'failed').length;
  if (failed > 0 && succeeded > 0) {
    return 'partial_success';
  }
  if (failed > 0) {
    return 'failed';
  }
  return 'success';
}

function initializeRunLog(runId, runTimestamp, dryRun, replay) {
  return {
    run_id: runId,
    run_timestamp: runTimestamp,
    start_timestamp: new Date().toISOString(),
    end_timestamp: null,
    dry_run: dryRun,
    replay: replay,
    phase_statuses: {},
    delivery_statuses: {},
    final_outcome: null,
    failure_summary: []
  };
}

function markPhase(runLog, phase, status, details = {}) {
  runLog.phase_statuses[phase] = {
    status,
    timestamp: new Date().toISOString(),
    ...details
  };
}

function writeRunArtifacts(outputDir, bundle, deliveryStatus, runLog) {
  writeJson(resolve(outputDir, 'run_bundle.json'), bundle);
  writeJson(resolve(outputDir, 'delivery_status.json'), deliveryStatus);
  writeJson(resolve(outputDir, 'run_log.json'), runLog);
}

export async function deliverRunBundle({
  bundle,
  dryRun = false,
  replay = false,
  channels = ['email', 'telegram'],
  deliveryRulesPath = DEFAULT_DELIVERY_RULES_PATH,
  adapterOverrides = {},
  environment = process.env
}) {
  validateRunBundle(bundle);
  const deliveryRules = loadDeliveryRules(deliveryRulesPath);
  const adapters = defaultAdapters(deliveryRules, adapterOverrides);
  const ledger = loadDeliveryLedger(deliveryRules.artifacts.ledger_path);
  const channelResults = {};

  for (const channel of channels) {
    channelResults[channel] = await deliverChannel({
      bundle,
      channel,
      target: bundle.delivery_targets[channel],
      adapter: adapters[channel],
      retryRules: deliveryRules.retry,
      ledger,
      replay,
      dryRun,
      now: new Date().toISOString()
    });
  }

  const ledgerPath = saveDeliveryLedger(deliveryRules.artifacts.ledger_path, ledger);
  const deliveryStatus = {
    run_id: bundle.run_id,
    run_timestamp: bundle.run_timestamp,
    run_status: bundle.run_status,
    selected_count: bundle.selected_count,
    dry_run: dryRun,
    replay,
    channels: channelResults,
    ledger_path: ledgerPath,
    final_outcome: finalRunOutcome(channelResults, dryRun)
  };

  return {
    deliveryStatus,
    ledgerPath
  };
}

export async function executeDibsRun({
  rawItems,
  now = new Date(),
  runTimestamp = now.toISOString(),
  deliveryRulesPath = DEFAULT_DELIVERY_RULES_PATH,
  renderingRulesPath = resolve(process.cwd(), 'config', 'rendering-rules.json'),
  dryRun = false,
  replay = false,
  channels = ['email', 'telegram'],
  adapterOverrides = {},
  environment = process.env
}) {
  const deliveryRules = loadDeliveryRules(deliveryRulesPath);
  const runId = makeRunId(runTimestamp);
  const outputDir = resolve(process.cwd(), deliveryRules.artifacts.output_root, runId);
  mkdirSync(outputDir, { recursive: true });
  const runLog = initializeRunLog(runId, runTimestamp, dryRun, replay);

  try {
    markPhase(runLog, 'candidate_pool', 'started');
    const candidate = buildCandidatePools({
      rawItems,
      now,
      fetchedAt: runTimestamp,
      outputDir
    });
    markPhase(runLog, 'candidate_pool', 'succeeded', {
      main_candidate_pool_size: candidate.mainPool.length
    });

    markPhase(runLog, 'semantic', 'started');
    const semantic = buildSemanticCards({
      canonicalRecords: candidate.mainPool,
      outputDir,
      runTimestamp
    });
    markPhase(runLog, 'semantic', 'succeeded', {
      semantic_card_count: semantic.cards.length
    });

    markPhase(runLog, 'editorial', 'started');
    const editorial = buildEditorialSelection({
      semanticCards: semantic.cards,
      outputDir,
      runTimestamp
    });
    markPhase(runLog, 'editorial', 'succeeded', {
      selected_count: editorial.result.selected_count
    });

    markPhase(runLog, 'rendering', 'started');
    const rendered = renderBriefing({
      selectionResult: editorial.result,
      outputDir,
      runTimestamp,
      renderingRulesPath
    });
    markPhase(runLog, 'rendering', 'succeeded', {
      email_entries: rendered.email.entry_article_ids.length,
      telegram_entries: rendered.telegram.entry_article_ids.length
    });

    const bundle = buildRunBundle({
      runId,
      runTimestamp,
      selectionResult: editorial.result,
      rendered,
      outputDir,
      deliveryRules,
      environment
    });

    markPhase(runLog, 'delivery', 'started');
    const { deliveryStatus, ledgerPath } = await deliverRunBundle({
      bundle,
      dryRun,
      replay,
      channels,
      deliveryRulesPath,
      adapterOverrides,
      environment
    });
    markPhase(runLog, 'delivery', 'succeeded', {
      final_outcome: deliveryStatus.final_outcome,
      ledger_path: ledgerPath
    });

    runLog.delivery_statuses = deliveryStatus.channels;
    runLog.final_outcome = deliveryStatus.final_outcome;
    runLog.end_timestamp = new Date().toISOString();
    writeRunArtifacts(outputDir, bundle, deliveryStatus, runLog);

    return {
      runBundle: bundle,
      deliveryStatus,
      runLog,
      outputDir
    };
  } catch (error) {
    runLog.end_timestamp = new Date().toISOString();
    runLog.final_outcome = 'failed';
    runLog.failure_summary.push({
      message: error.message
    });
    writeJson(resolve(outputDir, 'run_log.json'), runLog);
    throw error;
  }
}

export async function retryRunBundleDelivery({
  runBundlePath,
  deliveryRulesPath = DEFAULT_DELIVERY_RULES_PATH,
  dryRun = false,
  replay = false,
  channels = ['email', 'telegram'],
  adapterOverrides = {},
  environment = process.env
}) {
  const bundle = JSON.parse(readFileSync(resolve(process.cwd(), runBundlePath), 'utf8'));
  const { deliveryStatus, ledgerPath } = await deliverRunBundle({
    bundle,
    dryRun,
    replay,
    channels,
    deliveryRulesPath,
    adapterOverrides,
    environment
  });
  writeJson(resolve(bundle.output_dir, 'delivery_status.json'), deliveryStatus);

  const runLogPath = resolve(bundle.output_dir, 'run_log.json');
  const existingRunLog = JSON.parse(readFileSync(runLogPath, 'utf8'));
  existingRunLog.end_timestamp = new Date().toISOString();
  existingRunLog.delivery_statuses = deliveryStatus.channels;
  existingRunLog.final_outcome = deliveryStatus.final_outcome;
  existingRunLog.phase_statuses.delivery = {
    status: 'retried',
    timestamp: new Date().toISOString(),
    ledger_path: ledgerPath
  };
  writeJson(runLogPath, existingRunLog);

  return {
    runBundle: bundle,
    deliveryStatus,
    runLog: existingRunLog
  };
}

export async function executeScheduledDibsRun({
  rawItems,
  now = new Date(),
  deliveryRulesPath = DEFAULT_DELIVERY_RULES_PATH,
  ...rest
}) {
  const deliveryRules = loadDeliveryRules(deliveryRulesPath);
  const schedulerDecision = evaluateSchedule(deliveryRules.schedule, now);
  const outputRoot = resolve(process.cwd(), deliveryRules.artifacts.output_root);

  if (!schedulerDecision.due) {
    const schedulerDebug = {
      checked_at: now.toISOString(),
      decision: schedulerDecision,
      scheduled_run_executed: false
    };
    const schedulerDebugPath = writeSchedulerDebug(outputRoot, schedulerDebug);
    return {
      scheduled: false,
      schedulerDebug,
      schedulerDebugPath
    };
  }

  const execution = await executeDibsRun({
    rawItems,
    now,
    runTimestamp: now.toISOString(),
    deliveryRulesPath,
    ...rest
  });

  const schedulerDebug = {
    checked_at: now.toISOString(),
    decision: schedulerDecision,
    scheduled_run_executed: true,
    run_id: execution.runBundle.run_id
  };
  const schedulerDebugPath = writeSchedulerDebug(outputRoot, schedulerDebug);

  return {
    scheduled: true,
    schedulerDebug,
    schedulerDebugPath,
    ...execution
  };
}
