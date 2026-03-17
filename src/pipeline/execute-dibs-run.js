import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { loadDeliveryRules, loadEditorialRules, loadSemanticRules, loadSourceCatalog } from '../config/load-config.js';
import { EmailDeliveryAdapter } from '../delivery/email-adapter.js';
import { TelegramDeliveryAdapter } from '../delivery/telegram-adapter.js';
import { getChannelLedgerEntry, loadDeliveryLedger, recordChannelAttempt, saveDeliveryLedger } from '../delivery/state-store.js';
import { createRunBundle, validateRunBundle } from '../models/run-bundle.js';
import { buildCandidatePools } from './build-candidate-pools.js';
import { buildEditorialSelection } from './build-editorial-selection.js';
import { buildSemanticCards } from './build-semantic-cards.js';
import { renderBriefing } from './render-briefing.js';
import { evaluateSchedule, writeSchedulerDebug } from '../scheduler/schedule.js';
import { normalizeWhitespace } from '../utils/text.js';

const DEFAULT_DELIVERY_RULES_PATH = resolve(process.cwd(), 'config', 'delivery-rules.json');
const DEFAULT_EDITORIAL_RULES_PATH = resolve(process.cwd(), 'config', 'editorial-rules.json');
const DEFAULT_SEMANTIC_RULES_PATH = resolve(process.cwd(), 'config', 'semantic-rules.json');
const DEFAULT_SOURCES_PATH = resolve(process.cwd(), 'config', 'sources.json');

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

function incrementCounter(target, key) {
  target[key] = (target[key] ?? 0) + 1;
}

function topEntries(map, limit = 5) {
  return Object.entries(map)
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, limit)
    .map(([key, count]) => ({ key, count }));
}

function countWords(text) {
  const normalized = normalizeWhitespace(text);
  return (normalized.match(/\b[\p{L}\p{N}'-]+\b/gu) ?? []).length;
}

function countChineseChars(text) {
  return (normalizeWhitespace(text).match(/[\u3400-\u9fff]/gu) ?? []).length;
}

function languageKey(value) {
  if (value === 'zh') {
    return 'zh';
  }
  if (value === 'en') {
    return 'en';
  }
  return 'unknown';
}

function endsWithTerminalPunctuation(text, language) {
  const normalized = normalizeWhitespace(text);
  if (!normalized) {
    return false;
  }
  if (language === 'zh') {
    return /[。！？]$/.test(normalized);
  }
  return /[.!?]$/.test(normalized);
}

function titleContainsSourceSuffix(title, sourceDisplayName) {
  const normalizedTitle = normalizeWhitespace(title).toLowerCase();
  const normalizedSource = normalizeWhitespace(sourceDisplayName).toLowerCase();
  if (!normalizedTitle || !normalizedSource) {
    return false;
  }
  return (
    normalizedTitle.endsWith(` - ${normalizedSource}`)
    || normalizedTitle.endsWith(` | ${normalizedSource}`)
    || normalizedTitle.includes(`${normalizedSource} -`)
    || normalizedTitle.includes(`${normalizedSource} |`)
  );
}

function shouldRenderBylineForReport(item) {
  return typeof item.author_byline === 'string'
    && item.author_byline.trim().length > 0
    && item.author_byline.trim().toLowerCase() !== item.source_display_name.trim().toLowerCase();
}

function buildContentQualityReport({ editorial, rendered, semanticRules, runTimestamp }) {
  const summaryLengths = { en: [], zh: [], unknown: [] };
  const whyLengths = { en: [], zh: [], unknown: [] };
  const sourceCounts = {};
  const counts = {
    summary_incomplete_count: 0,
    summary_short_count: 0,
    summary_long_count: 0,
    why_short_count: 0,
    why_long_count: 0,
    why_generic_count: 0,
    byline_shown_count: 0,
    byline_total_count: 0,
    summary_limited_context_count: 0
  };

  const genericWhyPatterns = {
    en: [
      /\bcould\b/i,
      /\bmay\b/i,
      /\bmight\b/i,
      /\breframes?\b/i,
      /\bshape(s|d)?\b/i,
      /\bimplications beyond\b/i
    ],
    zh: [/可能/, /或将/, /重新定义/]
  };

  for (const item of editorial.result.selected_items) {
    const language = languageKey(item.language);
    const summary = normalizeWhitespace(item.factual_summary);
    const why = normalizeWhitespace(item.why_it_matters);
    const isChinese = language === 'zh';
    const summaryLength = isChinese ? countChineseChars(summary) : countWords(summary);
    const whyLength = isChinese ? countChineseChars(why) : countWords(why);

    summaryLengths[language].push(summaryLength);
    whyLengths[language].push(whyLength);

    if (!endsWithTerminalPunctuation(summary, isChinese ? 'zh' : 'en')) {
      counts.summary_incomplete_count += 1;
    }

    const summaryFromLimitedContext = item.warnings?.some((warning) => warning.code === 'weak_canonical_text');
    if (summaryFromLimitedContext) {
      counts.summary_limited_context_count += 1;
    }

    const summaryMin = isChinese
      ? (summaryFromLimitedContext ? semanticRules.summary_rules.non_english_summary_only_min_chars : semanticRules.summary_rules.non_english_min_chars)
      : (summaryFromLimitedContext ? semanticRules.summary_rules.english_summary_only_min_words : semanticRules.summary_rules.english_min_words);
    const summaryMax = isChinese
      ? (summaryFromLimitedContext ? semanticRules.summary_rules.non_english_summary_only_max_chars : semanticRules.summary_rules.non_english_max_chars)
      : (summaryFromLimitedContext ? semanticRules.summary_rules.english_summary_only_max_words : semanticRules.summary_rules.english_max_words);

    if (summaryLength < summaryMin) {
      counts.summary_short_count += 1;
    }
    if (summaryLength > summaryMax) {
      counts.summary_long_count += 1;
    }

    const whyMin = isChinese ? semanticRules.summary_rules.why_it_matters_min_chars : semanticRules.summary_rules.why_it_matters_min_words;
    const whyMax = isChinese ? semanticRules.summary_rules.why_it_matters_max_chars : semanticRules.summary_rules.why_it_matters_max_words;
    if (whyLength < whyMin) {
      counts.why_short_count += 1;
    }
    if (whyLength > whyMax) {
      counts.why_long_count += 1;
    }

    const genericPatterns = genericWhyPatterns[language] ?? [];
    if (genericPatterns.some((pattern) => pattern.test(why))) {
      counts.why_generic_count += 1;
    }

    incrementCounter(sourceCounts, item.source_display_name);
    counts.byline_total_count += 1;
    if (shouldRenderBylineForReport(item)) {
      counts.byline_shown_count += 1;
    }
  }

  const average = (values) => values.length === 0 ? 0 : Number((values.reduce((total, entry) => total + entry, 0) / values.length).toFixed(2));

  return {
    run_timestamp: runTimestamp,
    selected_count: editorial.result.selected_count,
    selected_items_by_source: sourceCounts,
    selected_items_by_section: Object.fromEntries(rendered.blocks.map((block) => [block.label, block.items.length])),
    summary_length_avg: {
      en: average(summaryLengths.en),
      zh: average(summaryLengths.zh),
      unknown: average(summaryLengths.unknown)
    },
    why_it_matters_length_avg: {
      en: average(whyLengths.en),
      zh: average(whyLengths.zh),
      unknown: average(whyLengths.unknown)
    },
    summary_length_range: {
      en: { min: Math.min(...summaryLengths.en, Infinity) || 0, max: Math.max(...summaryLengths.en, 0) },
      zh: { min: Math.min(...summaryLengths.zh, Infinity) || 0, max: Math.max(...summaryLengths.zh, 0) }
    },
    why_it_matters_length_range: {
      en: { min: Math.min(...whyLengths.en, Infinity) || 0, max: Math.max(...whyLengths.en, 0) },
      zh: { min: Math.min(...whyLengths.zh, Infinity) || 0, max: Math.max(...whyLengths.zh, 0) }
    },
    ...counts
  };
}

function buildFinalBriefingLanguageReport({ editorial, rendered, runTimestamp }) {
  const languageCounts = {};
  for (const item of editorial.result.selected_items) {
    incrementCounter(languageCounts, languageKey(item.language));
  }

  const sections = rendered.blocks.map((block) => {
    const counts = {};
    for (const item of block.items) {
      incrementCounter(counts, languageKey(item.language));
    }
    return {
      block_id: block.block_id,
      label: block.label,
      item_count: block.items.length,
      language_counts: counts
    };
  });

  return {
    run_timestamp: runTimestamp,
    selected_items_by_language: languageCounts,
    sections
  };
}

function buildFinalBriefingStyleAudit({ editorial, rendered, runTimestamp }) {
  const titleIssues = [];
  let sourceSuffixCount = 0;

  for (const item of editorial.result.selected_items) {
    if (titleContainsSourceSuffix(item.title, item.source_display_name)) {
      sourceSuffixCount += 1;
      titleIssues.push({
        article_id: item.article_id,
        title: item.title,
        source_display_name: item.source_display_name,
        issue: 'title_contains_source_suffix'
      });
    }
  }

  return {
    run_timestamp: runTimestamp,
    selected_count: editorial.result.selected_count,
    title_source_suffix_count: sourceSuffixCount,
    byline_shown_count: editorial.result.selected_items.filter((item) => shouldRenderBylineForReport(item)).length,
    ordered_items: rendered.blocks.flatMap((block) => block.items.map((item) => ({
      article_id: item.article_id,
      title: item.title,
      source_id: item.source_id,
      source_display_name: item.source_display_name,
      primary_domain: item.primary_domain,
      language: languageKey(item.language),
      block_id: block.block_id
    }))),
    title_fidelity_issues: titleIssues
  };
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
    briefing_candidate_funnel: resolve(outputDir, 'briefing_candidate_funnel.json'),
    briefing_selection_report: resolve(outputDir, 'briefing_selection_report.json'),
    briefing_status_report: resolve(outputDir, 'briefing_status_report.json'),
    attribution_audit_report: resolve(outputDir, 'attribution_audit_report.json'),
    content_quality_report: resolve(outputDir, 'content_quality_report.json'),
    final_briefing_language_report: resolve(outputDir, 'final_briefing_language_report.json'),
    final_briefing_style_audit: resolve(outputDir, 'final_briefing_style_audit.json'),
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

function deriveDegradedReasons({ candidate, semantic, editorial, editorialRules }) {
  const reasons = [];

  if (candidate.mainPool.length === 0) {
    reasons.push('main_candidate_pool_empty');
  }
  if (semantic.cards.length === 0) {
    reasons.push('semantic_pool_empty');
  }
  if (editorial.result.selected_count < editorialRules.selection.minimum_target_count) {
    reasons.push('selected_below_minimum_target');
  }
  if ((candidate.candidatePoolReport.rejection_reasons_breakdown.stale_item ?? 0) > 0) {
    reasons.push('stale_items_reduced_candidate_pool');
  }
  if ((candidate.candidatePoolReport.rejection_reasons_breakdown.insufficient_content_signal ?? 0) > 0) {
    reasons.push('insufficient_content_signal_reduced_candidate_pool');
  }

  return [...new Set(reasons)];
}

function buildBriefingCandidateFunnel({ candidate, semantic, editorial, rendered, editorialRules, runTimestamp }) {
  const scoredAboveThreshold = editorial.scoredCandidates.filter(
    (entry) => entry.final_composite_score >= editorialRules.selection.minimum_candidate_score
  );
  const sectionAllocationCandidates = scoredAboveThreshold.filter((entry) => entry.is_cluster_representative);
  const degradedReasons = deriveDegradedReasons({ candidate, semantic, editorial, editorialRules });

  return {
    run_timestamp: runTimestamp,
    raw_items_in: candidate.candidatePoolReport.total_raw_items_fetched,
    normalized_items: candidate.candidatePoolReport.total_normalized_items,
    rejected_items: candidate.candidatePoolReport.rejected_item_count,
    rejection_reasons_breakdown: candidate.candidatePoolReport.rejection_reasons_breakdown,
    backup_pool_items: candidate.backupPool.length,
    post_filter_items: candidate.mainPool.length,
    semantic_cards: semantic.cards.length,
    semantic_failures: semantic.failures.length,
    clusters: editorial.clusters.length,
    post_ranking_items: editorial.scoredCandidates.length,
    above_threshold_items: scoredAboveThreshold.length,
    section_allocation_candidates: sectionAllocationCandidates.length,
    final_selected_items: editorial.result.selected_count,
    final_sections: rendered.blocks.length,
    run_status: editorial.result.run_status,
    degraded_mode: editorial.result.degraded_mode,
    under_default_target: editorial.result.under_default_target,
    degraded_reasons: degradedReasons
  };
}

function buildBriefingSelectionReport({ editorial, rendered, runTimestamp }) {
  const sourceDistribution = {};
  for (const item of editorial.result.selected_items) {
    incrementCounter(sourceDistribution, item.source_display_name);
  }

  return {
    run_timestamp: runTimestamp,
    run_status: editorial.result.run_status,
    selected_count: editorial.result.selected_count,
    selected_article_ids: editorial.result.selected_article_ids,
    selected_sections: rendered.blocks.map((block) => ({
      block_id: block.block_id,
      label: block.label,
      item_count: block.items.length,
      article_ids: block.entry_article_ids
    })),
    selected_domain_counts: editorial.diagnostics.per_domain_counts,
    selected_source_counts: sourceDistribution,
    selected_source_class_counts: editorial.diagnostics.per_source_class_counts,
    top_selected_governed_sources: topEntries(sourceDistribution),
    selected_items: editorial.result.selected_items.map((item) => ({
      article_id: item.article_id,
      source_id: item.source_id,
      source_display_name: item.source_display_name,
      author_byline: item.author_byline ?? null,
      primary_domain: item.primary_domain,
      final_composite_score: item.final_composite_score,
      selection_reason_codes: item.selection_reason_codes
    }))
  };
}

function buildBriefingStatusReport({ candidate, semantic, editorial, rendered, editorialRules, runTimestamp }) {
  const degradedReasons = editorial.result.run_status === 'degraded'
    ? deriveDegradedReasons({ candidate, semantic, editorial, editorialRules })
    : [];

  return {
    run_timestamp: runTimestamp,
    pipeline_status: 'completed',
    run_status: editorial.result.run_status,
    degraded_mode: editorial.result.degraded_mode,
    under_default_target: editorial.result.under_default_target,
    selected_count: editorial.result.selected_count,
    minimum_target_count: editorialRules.selection.minimum_target_count,
    default_target_count: editorialRules.selection.default_target_count,
    degraded_reasons: degradedReasons,
    warning_flags: [
      ...candidate.candidatePoolReport.warnings,
      ...rendered.diagnostics.rendering_warnings
    ]
  };
}

function buildAttributionAuditReport({ editorial, sourceCatalog, runTimestamp }) {
  return {
    run_timestamp: runTimestamp,
    audited_items: editorial.result.selected_items.map((item) => {
      const governedSourceDisplayName = sourceCatalog.sources.get(item.source_id)?.display_name ?? item.source_display_name;
      const byline = item.author_byline ?? null;
      return {
        article_id: item.article_id,
        source_id: item.source_id,
        governed_source_display_name: governedSourceDisplayName,
        rendered_source_display_name: item.source_display_name,
        author_byline: byline,
        source_display_matches_governed_identity: item.source_display_name === governedSourceDisplayName,
        byline_rendered_separately: Boolean(byline && byline.trim() && byline.trim().toLowerCase() !== item.source_display_name.trim().toLowerCase())
      };
    })
  };
}

function writeBriefingDiagnostics(outputDir, reports) {
  writeJson(resolve(outputDir, 'briefing_candidate_funnel.json'), reports.candidateFunnel);
  writeJson(resolve(outputDir, 'briefing_selection_report.json'), reports.selectionReport);
  writeJson(resolve(outputDir, 'briefing_status_report.json'), reports.statusReport);
  writeJson(resolve(outputDir, 'attribution_audit_report.json'), reports.attributionAudit);
  writeJson(resolve(outputDir, 'content_quality_report.json'), reports.contentQuality);
  writeJson(resolve(outputDir, 'final_briefing_language_report.json'), reports.languageReport);
  writeJson(resolve(outputDir, 'final_briefing_style_audit.json'), reports.styleAudit);
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
  editorialRulesPath = DEFAULT_EDITORIAL_RULES_PATH,
  sourcesPath = DEFAULT_SOURCES_PATH,
  renderingRulesPath = resolve(process.cwd(), 'config', 'rendering-rules.json'),
  dryRun = false,
  replay = false,
  channels = ['email', 'telegram'],
  adapterOverrides = {},
  environment = process.env
}) {
  const deliveryRules = loadDeliveryRules(deliveryRulesPath);
  const editorialRules = loadEditorialRules(editorialRulesPath);
  const sourceCatalog = loadSourceCatalog(sourcesPath);
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
      runTimestamp,
      editorialRulesPath,
      sourcesPath
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

    const semanticRules = loadSemanticRules(DEFAULT_SEMANTIC_RULES_PATH);
    const contentQuality = buildContentQualityReport({
      editorial,
      rendered,
      semanticRules,
      runTimestamp
    });
    const languageReport = buildFinalBriefingLanguageReport({
      editorial,
      rendered,
      runTimestamp
    });
    const styleAudit = buildFinalBriefingStyleAudit({
      editorial,
      rendered,
      runTimestamp
    });

    writeBriefingDiagnostics(outputDir, {
      candidateFunnel: buildBriefingCandidateFunnel({
        candidate,
        semantic,
        editorial,
        rendered,
        editorialRules,
        runTimestamp
      }),
      selectionReport: buildBriefingSelectionReport({
        editorial,
        rendered,
        runTimestamp
      }),
      statusReport: buildBriefingStatusReport({
        candidate,
        semantic,
        editorial,
        rendered,
        editorialRules,
        runTimestamp
      }),
      attributionAudit: buildAttributionAuditReport({
        editorial,
        sourceCatalog,
        runTimestamp
      }),
      contentQuality,
      languageReport,
      styleAudit
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
