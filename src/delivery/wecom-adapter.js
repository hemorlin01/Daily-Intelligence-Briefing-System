const RETRYABLE_HTTP_STATUS = new Set([408, 425, 429, 500, 502, 503, 504]);
const MAX_MARKDOWN_BYTES = 3800;

function byteLength(value) {
  return Buffer.byteLength(value, 'utf8');
}

export function splitWeComMarkdown(content, maxBytes = MAX_MARKDOWN_BYTES) {
  const chunks = [];
  let current = '';
  for (const line of String(content ?? '').split(/\r?\n/)) {
    const candidate = current ? `${current}\n${line}` : line;
    if (byteLength(candidate) <= maxBytes) {
      current = candidate;
      continue;
    }
    if (current) {
      chunks.push(current);
      current = '';
    }
    if (byteLength(line) <= maxBytes) {
      current = line;
      continue;
    }
    let segment = '';
    for (const char of line) {
      if (byteLength(segment + char) > maxBytes) {
        chunks.push(segment);
        segment = char;
      } else {
        segment += char;
      }
    }
    current = segment;
  }
  if (current) {
    chunks.push(current);
  }
  return chunks.filter((chunk) => chunk.trim().length > 0);
}

function buildResult({
  destination = 'wecom-webhook',
  status,
  success,
  dryRun,
  actualSend,
  attemptTimestamp,
  error = null,
  retryable = false,
  providerMetadata = null
}) {
  return {
    channel: 'wecom',
    destination,
    status,
    success,
    dry_run: dryRun,
    actual_send: actualSend,
    attempt_timestamp: attemptTimestamp,
    artifact_path: null,
    error,
    retryable,
    skipped_duplicate: false,
    provider_mode: 'wecom-webhook',
    provider_metadata: providerMetadata
  };
}

async function readJson(response) {
  const text = await response.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return { errcode: null, errmsg: text };
  }
}

export class WeComDeliveryAdapter {
  constructor({ request = globalThis.fetch } = {}) {
    this.request = request;
  }

  async deliver({ bundle, destination, dryRun = false, now = new Date().toISOString() }) {
    if (!bundle?.artifacts?.wecom?.content) {
      throw new Error('WeCom delivery requires a rendered wecom artifact');
    }

    const resolvedDestination = destination || process.env.WECOM_WEBHOOK_URL || '';
    const webhookUrl = resolvedDestination === 'local-wecom-outbox' ? '' : resolvedDestination;
    if (dryRun) {
      return buildResult({
        destination: webhookUrl ? 'wecom-webhook' : null,
        status: 'dry_run',
        success: false,
        dryRun: true,
        actualSend: false,
        attemptTimestamp: now
      });
    }
    if (!/^https:\/\/qyapi\.weixin\.qq\.com\/cgi-bin\/webhook\/send\?key=/.test(webhookUrl)) {
      return buildResult({
        destination: webhookUrl ? 'wecom-webhook' : null,
        status: 'failed',
        success: false,
        dryRun: false,
        actualSend: false,
        attemptTimestamp: now,
        error: { code: webhookUrl ? 'INVALID_WECOM_WEBHOOK_URL' : 'MISSING_WECOM_WEBHOOK_URL', message: 'A valid WeCom bot webhook URL is required.' }
      });
    }
    if (typeof this.request !== 'function') {
      return buildResult({
        destination: 'wecom-webhook',
        status: 'failed',
        success: false,
        dryRun: false,
        actualSend: false,
        attemptTimestamp: now,
        error: { code: 'MISSING_FETCH_IMPLEMENTATION', message: 'WeCom delivery requires a fetch-compatible request implementation.' }
      });
    }

    const chunks = splitWeComMarkdown(bundle.artifacts.wecom.content);
    const sentChunks = [];
    try {
      for (let index = 0; index < chunks.length; index += 1) {
        const response = await this.request(webhookUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            msgtype: 'markdown',
            markdown: { content: chunks[index] }
          })
        });
        const data = await readJson(response);
        if (!response.ok || data?.errcode !== 0) {
          return buildResult({
            destination: 'wecom-webhook',
            status: 'failed',
            success: false,
            dryRun: false,
            actualSend: sentChunks.length > 0,
            attemptTimestamp: now,
            error: {
              code: data?.errcode ? `WECOM_${data.errcode}` : `WECOM_HTTP_${response.status}`,
              message: data?.errmsg || `WeCom webhook request failed with status ${response.status}.`
            },
            retryable: RETRYABLE_HTTP_STATUS.has(response.status),
            providerMetadata: {
              http_status: response.status ?? null,
              sent_chunk_count: sentChunks.length,
              total_chunk_count: chunks.length,
              failed_chunk_index: index
            }
          });
        }
        sentChunks.push(index);
      }

      return buildResult({
        destination: 'wecom-webhook',
        status: 'success',
        success: true,
        dryRun: false,
        actualSend: true,
        attemptTimestamp: now,
        providerMetadata: { sent_chunk_count: sentChunks.length, total_chunk_count: chunks.length }
      });
    } catch (error) {
      return buildResult({
        destination: 'wecom-webhook',
        status: 'failed',
        success: false,
        dryRun: false,
        actualSend: sentChunks.length > 0,
        attemptTimestamp: now,
        error: { code: error?.code || 'WECOM_REQUEST_FAILED', message: error?.message || 'Unknown WeCom delivery failure.' },
        retryable: ['ECONNRESET', 'EAI_AGAIN', 'ETIMEDOUT'].includes(error?.code),
        providerMetadata: { sent_chunk_count: sentChunks.length, total_chunk_count: chunks.length }
      });
    }
  }
}
