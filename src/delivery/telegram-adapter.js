import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const RETRYABLE_TELEGRAM_ERROR_CODES = new Set([
  'ECONNRESET',
  'EAI_AGAIN',
  'ETIMEDOUT',
  'TIMEOUT',
  'TRANSIENT_ERROR'
]);
const RETRYABLE_TELEGRAM_HTTP_STATUS = new Set([408, 409, 425, 429, 500, 502, 503, 504]);

function buildResult({
  channel,
  destination,
  status,
  success,
  dryRun,
  actualSend,
  attemptTimestamp,
  artifactPath = null,
  error = null,
  retryable = false,
  skippedDuplicate = false,
  providerMode,
  providerMetadata = null
}) {
  return {
    channel,
    destination,
    status,
    success,
    dry_run: dryRun,
    actual_send: actualSend,
    attempt_timestamp: attemptTimestamp,
    artifact_path: artifactPath,
    error,
    retryable,
    skipped_duplicate: skippedDuplicate,
    provider_mode: providerMode,
    provider_metadata: providerMetadata
  };
}

function buildError(code, message) {
  return { code, message };
}

function isValidChatId(value) {
  return typeof value === 'string' && /^-?\d+$|^@\w+$/.test(value.trim());
}

function isRetryableError(error) {
  return RETRYABLE_TELEGRAM_ERROR_CODES.has(error?.code) || Boolean(error?.retryable);
}

function resolveTelegramSettings(destination) {
  return {
    botToken: process.env.TELEGRAM_BOT_TOKEN || process.env.DIBS_TELEGRAM_BOT_TOKEN || '',
    chatId: destination || process.env.DIBS_TELEGRAM_CHAT_ID || process.env.TELEGRAM_CHAT_ID || ''
  };
}

async function readTelegramResponse(response) {
  const text = await response.text();
  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch {
    return { ok: false, description: text };
  }
}

export class TelegramDeliveryAdapter {
  constructor({ mode = 'local-file', transport = null, request = globalThis.fetch } = {}) {
    this.mode = mode;
    this.transport = transport;
    this.request = request;
  }

  async deliver({ bundle, destination, dryRun = false, now = new Date().toISOString() }) {
    if (!bundle?.artifacts?.telegram?.content) {
      throw new Error('Telegram delivery requires a rendered Telegram artifact');
    }

    if (dryRun) {
      return buildResult({
        channel: 'telegram',
        destination,
        status: 'dry_run',
        success: false,
        dryRun: true,
        actualSend: false,
        attemptTimestamp: now,
        providerMode: this.mode
      });
    }

    if (this.mode === 'local-file') {
      const outputPath = resolve(bundle.output_dir, 'delivery', 'telegram-delivered.txt');
      mkdirSync(resolve(bundle.output_dir, 'delivery'), { recursive: true });
      writeFileSync(outputPath, bundle.artifacts.telegram.content);
      return buildResult({
        channel: 'telegram',
        destination,
        status: 'success',
        success: true,
        dryRun: false,
        actualSend: true,
        attemptTimestamp: now,
        artifactPath: outputPath,
        providerMode: this.mode
      });
    }

    if (this.mode === 'telegram-bot-api') {
      const telegram = resolveTelegramSettings(destination);
      if (!telegram.botToken) {
        return buildResult({
          channel: 'telegram',
          destination: telegram.chatId || null,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError('MISSING_TELEGRAM_BOT_TOKEN', 'Telegram bot token is required for telegram-bot-api mode.'),
          providerMode: this.mode
        });
      }
      if (!telegram.chatId) {
        return buildResult({
          channel: 'telegram',
          destination: null,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError('MISSING_TELEGRAM_CHAT_ID', 'Telegram chat id is required for telegram-bot-api mode.'),
          providerMode: this.mode
        });
      }
      if (!isValidChatId(telegram.chatId)) {
        return buildResult({
          channel: 'telegram',
          destination: telegram.chatId,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError('INVALID_TELEGRAM_CHAT_ID', `Telegram destination "${telegram.chatId}" is not a valid chat id or channel handle.`),
          providerMode: this.mode
        });
      }

      const request = typeof this.transport === 'function' ? this.transport : this.request;
      if (typeof request !== 'function') {
        return buildResult({
          channel: 'telegram',
          destination: telegram.chatId,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError('MISSING_FETCH_IMPLEMENTATION', 'Telegram delivery requires a fetch-compatible request implementation.'),
          providerMode: this.mode
        });
      }

      try {
        const response = await request(`https://api.telegram.org/bot${telegram.botToken}/sendMessage`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            chat_id: telegram.chatId,
            text: bundle.artifacts.telegram.content,
            disable_web_page_preview: true
          })
        });

        const data = await readTelegramResponse(response);
        if (!response.ok || !data?.ok) {
          const errorCode = response.status ? `TELEGRAM_HTTP_${response.status}` : 'TELEGRAM_SEND_FAILED';
          return buildResult({
            channel: 'telegram',
            destination: telegram.chatId,
            status: 'failed',
            success: false,
            dryRun: false,
            actualSend: false,
            attemptTimestamp: now,
            error: buildError(errorCode, data?.description || `Telegram API request failed with status ${response.status}.`),
            retryable: RETRYABLE_TELEGRAM_HTTP_STATUS.has(response.status),
            providerMode: this.mode,
            providerMetadata: {
              http_status: response.status ?? null
            }
          });
        }

        return buildResult({
          channel: 'telegram',
          destination: telegram.chatId,
          status: 'success',
          success: true,
          dryRun: false,
          actualSend: true,
          attemptTimestamp: now,
          providerMode: this.mode,
          providerMetadata: {
            http_status: response.status ?? null,
            message_id: data?.result?.message_id ?? null,
            chat_id: data?.result?.chat?.id ?? telegram.chatId
          }
        });
      } catch (error) {
        return buildResult({
          channel: 'telegram',
          destination: telegram.chatId,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError(error?.code || 'TELEGRAM_REQUEST_FAILED', error?.message || 'Unknown Telegram delivery failure.'),
          retryable: isRetryableError(error),
          providerMode: this.mode
        });
      }
    }

    if (this.mode === 'mock' && typeof this.transport === 'function') {
      try {
        const response = await this.transport({
          channel: 'telegram',
          destination,
          content: bundle.artifacts.telegram.content,
          bundle
        });
        return buildResult({
          channel: 'telegram',
          destination,
          status: response?.status ?? 'success',
          success: response?.success ?? true,
          dryRun: false,
          actualSend: response?.actual_send ?? true,
          attemptTimestamp: now,
          artifactPath: response?.artifact_path ?? null,
          providerMode: this.mode,
          providerMetadata: response?.provider_metadata ?? null
        });
      } catch (error) {
        return buildResult({
          channel: 'telegram',
          destination,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError(error?.code ?? 'UNKNOWN', error?.message ?? 'Unknown telegram mock transport failure.'),
          retryable: Boolean(error?.retryable),
          providerMode: this.mode
        });
      }
    }

    return buildResult({
      channel: 'telegram',
      destination,
      status: 'failed',
      success: false,
      dryRun: false,
      actualSend: false,
      attemptTimestamp: now,
      error: buildError('UNSUPPORTED_MODE', `Unsupported telegram delivery mode "${this.mode}"`),
      retryable: false,
      providerMode: this.mode
    });
  }
}
