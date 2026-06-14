const RETRYABLE_HTTP_STATUS = new Set([408, 425, 429, 500, 502, 503, 504]);

function buildResult({
  destination = 'bark-device',
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
    channel: 'bark',
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
    provider_mode: 'bark-api',
    provider_metadata: providerMetadata
  };
}

function compactBarkBody(content, maxChars = 1800) {
  const normalized = String(content ?? '').trim();
  if (normalized.length <= maxChars) {
    return normalized;
  }
  return `${normalized.slice(0, maxChars - 18).trimEnd()}\n\n[内容已截短]`;
}

async function readJson(response) {
  const text = await response.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return { code: null, message: text };
  }
}

export class BarkDeliveryAdapter {
  constructor({ request = globalThis.fetch, server = null } = {}) {
    this.request = request;
    this.server = server;
  }

  async deliver({ bundle, destination, dryRun = false, now = new Date().toISOString() }) {
    if (!bundle?.artifacts?.telegram?.content) {
      throw new Error('Bark delivery requires a rendered Telegram artifact');
    }

    const resolvedDestination = destination || process.env.BARK_DEVICE_KEY || '';
    const deviceKey = resolvedDestination === 'local-bark-outbox' ? '' : resolvedDestination;
    if (dryRun) {
      return buildResult({
        destination: deviceKey ? 'bark-device' : null,
        status: 'dry_run',
        success: false,
        dryRun: true,
        actualSend: false,
        attemptTimestamp: now
      });
    }
    if (!deviceKey) {
      return buildResult({
        destination: null,
        status: 'failed',
        success: false,
        dryRun: false,
        actualSend: false,
        attemptTimestamp: now,
        error: { code: 'MISSING_BARK_DEVICE_KEY', message: 'Bark device key is required for bark-api mode.' }
      });
    }
    if (typeof this.request !== 'function') {
      return buildResult({
        destination: 'bark-device',
        status: 'failed',
        success: false,
        dryRun: false,
        actualSend: false,
        attemptTimestamp: now,
        error: { code: 'MISSING_FETCH_IMPLEMENTATION', message: 'Bark delivery requires a fetch-compatible request implementation.' }
      });
    }

    const server = (this.server || process.env.BARK_SERVER || 'https://api.day.app').replace(/\/+$/, '');
    try {
      const response = await this.request(`${server}/push`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          device_key: deviceKey,
          title: `DIBS Briefing - ${bundle.run_timestamp.slice(0, 10)}`,
          body: compactBarkBody(bundle.artifacts.telegram.content),
          group: 'DIBS'
        })
      });
      const data = await readJson(response);
      if (!response.ok || data?.code !== 200) {
        return buildResult({
          destination: 'bark-device',
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: {
            code: response.status ? `BARK_HTTP_${response.status}` : 'BARK_SEND_FAILED',
            message: data?.message || `Bark API request failed with status ${response.status}.`
          },
          retryable: RETRYABLE_HTTP_STATUS.has(response.status),
          providerMetadata: { http_status: response.status ?? null, provider_code: data?.code ?? null }
        });
      }

      return buildResult({
        destination: 'bark-device',
        status: 'success',
        success: true,
        dryRun: false,
        actualSend: true,
        attemptTimestamp: now,
        providerMetadata: { http_status: response.status ?? null, provider_code: data.code }
      });
    } catch (error) {
      return buildResult({
        destination: 'bark-device',
        status: 'failed',
        success: false,
        dryRun: false,
        actualSend: false,
        attemptTimestamp: now,
        error: { code: error?.code || 'BARK_REQUEST_FAILED', message: error?.message || 'Unknown Bark delivery failure.' },
        retryable: ['ECONNRESET', 'EAI_AGAIN', 'ETIMEDOUT'].includes(error?.code)
      });
    }
  }
}
