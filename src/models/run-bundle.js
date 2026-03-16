import { validateRenderedBriefing } from './rendering.js';

const REQUIRED_RUN_BUNDLE_FIELDS = [
  'run_id',
  'run_timestamp',
  'run_status',
  'selected_count',
  'selected_article_ids',
  'artifacts',
  'delivery_targets',
  'diagnostics_references',
  'idempotency',
  'output_dir'
];

const RUN_STATUS_VALUES = new Set([
  'on_target',
  'under_default_target',
  'degraded'
]);

function assertNonEmptyString(value, fieldName, context) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new Error(`${context}: "${fieldName}" must be a non-empty string`);
  }
}

function assertStringArray(value, fieldName, context) {
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string' || item.trim().length === 0)) {
    throw new Error(`${context}: "${fieldName}" must be an array of non-empty strings`);
  }
}

export function validateRunBundle(bundle, selectionResult, renderedBriefing) {
  const context = 'Run bundle validation failed';

  if (!bundle || typeof bundle !== 'object' || Array.isArray(bundle)) {
    throw new Error(`${context}: bundle must be an object`);
  }

  for (const field of REQUIRED_RUN_BUNDLE_FIELDS) {
    if (!(field in bundle)) {
      throw new Error(`${context}: missing required field "${field}"`);
    }
  }

  assertNonEmptyString(bundle.run_id, 'run_id', context);
  assertNonEmptyString(bundle.run_timestamp, 'run_timestamp', context);
  assertNonEmptyString(bundle.output_dir, 'output_dir', context);

  if (!RUN_STATUS_VALUES.has(bundle.run_status)) {
    throw new Error(`${context}: invalid run_status "${bundle.run_status}"`);
  }

  assertStringArray(bundle.selected_article_ids, 'selected_article_ids', context);

  if (bundle.selected_count !== bundle.selected_article_ids.length) {
    throw new Error(`${context}: selected_count must match selected_article_ids length`);
  }

  if (!bundle.artifacts || typeof bundle.artifacts !== 'object' || Array.isArray(bundle.artifacts)) {
    throw new Error(`${context}: "artifacts" must be an object`);
  }

  for (const channel of ['email', 'telegram', 'markdown']) {
    const artifact = bundle.artifacts[channel];
    if (!artifact || typeof artifact !== 'object' || Array.isArray(artifact)) {
      throw new Error(`${context}: missing artifact for "${channel}"`);
    }
    assertNonEmptyString(artifact.content, `artifacts.${channel}.content`, context);
    assertNonEmptyString(artifact.path, `artifacts.${channel}.path`, context);
  }

  if (!bundle.delivery_targets || typeof bundle.delivery_targets !== 'object' || Array.isArray(bundle.delivery_targets)) {
    throw new Error(`${context}: "delivery_targets" must be an object`);
  }
  for (const channel of ['email', 'telegram']) {
    const target = bundle.delivery_targets[channel];
    if (!target || typeof target !== 'object' || Array.isArray(target)) {
      throw new Error(`${context}: missing delivery target for "${channel}"`);
    }
    if (typeof target.enabled !== 'boolean') {
      throw new Error(`${context}: delivery target "${channel}" requires boolean enabled`);
    }
    assertNonEmptyString(target.mode, `delivery_targets.${channel}.mode`, context);
    assertNonEmptyString(target.destination, `delivery_targets.${channel}.destination`, context);
  }

  if (!bundle.diagnostics_references || typeof bundle.diagnostics_references !== 'object' || Array.isArray(bundle.diagnostics_references)) {
    throw new Error(`${context}: "diagnostics_references" must be an object`);
  }
  for (const field of ['delivery_status', 'run_log']) {
    assertNonEmptyString(bundle.diagnostics_references[field], `diagnostics_references.${field}`, context);
  }

  if (!bundle.idempotency || typeof bundle.idempotency !== 'object' || Array.isArray(bundle.idempotency)) {
    throw new Error(`${context}: "idempotency" must be an object`);
  }
  assertNonEmptyString(bundle.idempotency.run_fingerprint, 'idempotency.run_fingerprint', context);
  if (!bundle.idempotency.per_channel || typeof bundle.idempotency.per_channel !== 'object' || Array.isArray(bundle.idempotency.per_channel)) {
    throw new Error(`${context}: "idempotency.per_channel" must be an object`);
  }
  for (const channel of ['email', 'telegram']) {
    assertNonEmptyString(bundle.idempotency.per_channel[channel], `idempotency.per_channel.${channel}`, context);
  }

  if (selectionResult) {
    if (bundle.run_timestamp !== selectionResult.run_timestamp) {
      throw new Error(`${context}: bundle run_timestamp must match the selection result run_timestamp`);
    }
    if (bundle.run_status !== selectionResult.run_status) {
      throw new Error(`${context}: bundle run_status must match the selection result run_status`);
    }
    if (JSON.stringify(bundle.selected_article_ids) !== JSON.stringify(selectionResult.selected_article_ids)) {
      throw new Error(`${context}: bundle selected_article_ids must match the selection result`);
    }
  }

  if (renderedBriefing) {
    validateRenderedBriefing(renderedBriefing, selectionResult);
  }

  return true;
}

export function createRunBundle(data, selectionResult, renderedBriefing) {
  const bundle = {
    run_id: data.run_id,
    run_timestamp: data.run_timestamp,
    run_status: data.run_status,
    selected_count: Number(data.selected_count ?? 0),
    selected_article_ids: data.selected_article_ids ?? [],
    artifacts: data.artifacts ?? {},
    delivery_targets: data.delivery_targets ?? {},
    diagnostics_references: data.diagnostics_references ?? {},
    idempotency: data.idempotency ?? {},
    output_dir: data.output_dir,
    ...data
  };

  validateRunBundle(bundle, selectionResult, renderedBriefing);
  return Object.freeze(bundle);
}
