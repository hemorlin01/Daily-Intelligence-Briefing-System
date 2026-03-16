import nodemailer from 'nodemailer';
import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const RETRYABLE_EMAIL_ERROR_CODES = new Set([
  'ECONNECTION',
  'ECONNREFUSED',
  'ECONNRESET',
  'EAI_AGAIN',
  'ETIMEDOUT',
  'TIMEOUT',
  'TRANSIENT_ERROR'
]);

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

function isRetryableError(error) {
  return RETRYABLE_EMAIL_ERROR_CODES.has(error?.code) || Boolean(error?.retryable);
}

function isValidEmailAddress(value) {
  return typeof value === 'string' && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim());
}

function resolveSmtpSettings(destination) {
  return {
    host: process.env.GMAIL_SMTP_HOST || 'smtp.gmail.com',
    port: Number(process.env.GMAIL_SMTP_PORT || 465),
    secure: String(process.env.GMAIL_SMTP_SECURE || 'true').toLowerCase() === 'true',
    user: process.env.GMAIL_SMTP_USER || process.env.SMTP_USER || '',
    pass: process.env.GMAIL_SMTP_APP_PASSWORD || process.env.GMAIL_SMTP_PASSWORD || process.env.SMTP_PASSWORD || '',
    from: process.env.GMAIL_FROM_ADDRESS || process.env.EMAIL_FROM_ADDRESS || process.env.GMAIL_SMTP_USER || process.env.SMTP_USER || '',
    to: destination || process.env.DIBS_EMAIL_TO || process.env.GMAIL_TO_ADDRESS || ''
  };
}

export class EmailDeliveryAdapter {
  constructor({ mode = 'local-file', transport = null, transportFactory = nodemailer.createTransport } = {}) {
    this.mode = mode;
    this.transport = transport;
    this.transportFactory = transportFactory;
  }

  async deliver({ bundle, destination, dryRun = false, now = new Date().toISOString() }) {
    if (!bundle?.artifacts?.email?.content) {
      throw new Error('Email delivery requires a rendered email artifact');
    }

    if (dryRun) {
      return buildResult({
        channel: 'email',
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
      const outputPath = resolve(bundle.output_dir, 'delivery', 'email-delivered.txt');
      mkdirSync(resolve(bundle.output_dir, 'delivery'), { recursive: true });
      writeFileSync(outputPath, bundle.artifacts.email.content);
      return buildResult({
        channel: 'email',
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

    if (this.mode === 'gmail-smtp') {
      const smtp = resolveSmtpSettings(destination);
      if (!smtp.user) {
        return buildResult({
          channel: 'email',
          destination: smtp.to || destination || null,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError('MISSING_SMTP_USER', 'Gmail SMTP user is required for gmail-smtp mode.'),
          providerMode: this.mode
        });
      }
      if (!smtp.pass) {
        return buildResult({
          channel: 'email',
          destination: smtp.to || destination || null,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError('MISSING_SMTP_PASSWORD', 'Gmail SMTP app password is required for gmail-smtp mode.'),
          providerMode: this.mode
        });
      }
      if (!smtp.from || !isValidEmailAddress(smtp.from)) {
        return buildResult({
          channel: 'email',
          destination: smtp.to || destination || null,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError('INVALID_SMTP_FROM_ADDRESS', 'A valid sender email address is required for gmail-smtp mode.'),
          providerMode: this.mode
        });
      }
      if (!smtp.to) {
        return buildResult({
          channel: 'email',
          destination: null,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError('MISSING_EMAIL_DESTINATION', 'No email destination was provided for gmail-smtp mode.'),
          providerMode: this.mode
        });
      }
      if (!isValidEmailAddress(smtp.to)) {
        return buildResult({
          channel: 'email',
          destination: smtp.to,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError('INVALID_EMAIL_DESTINATION', `Email destination "${smtp.to}" is not a valid email address.`),
          providerMode: this.mode
        });
      }

      try {
        const transporter = this.transport?.sendMail
          ? this.transport
          : this.transportFactory({
            host: smtp.host,
            port: smtp.port,
            secure: smtp.secure,
            auth: {
              user: smtp.user,
              pass: smtp.pass
            }
          });

        const subject = bundle.artifacts.email.subject
          ?? `DIBS Briefing ${bundle.run_timestamp?.slice(0, 10) ?? ''}`.trim();
        const info = await transporter.sendMail({
          from: smtp.from,
          to: smtp.to,
          subject,
          text: bundle.artifacts.email.content
        });

        return buildResult({
          channel: 'email',
          destination: smtp.to,
          status: 'success',
          success: true,
          dryRun: false,
          actualSend: true,
          attemptTimestamp: now,
          providerMode: this.mode,
          providerMetadata: {
            message_id: info?.messageId ?? null,
            response: info?.response ?? null,
            accepted: info?.accepted ?? [],
            rejected: info?.rejected ?? []
          }
        });
      } catch (error) {
        return buildResult({
          channel: 'email',
          destination: smtp.to,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError(error?.code || 'SMTP_SEND_FAILED', error?.message || 'Unknown Gmail SMTP send failure.'),
          retryable: isRetryableError(error),
          providerMode: this.mode
        });
      }
    }

    if (this.mode === 'mock' && typeof this.transport === 'function') {
      try {
        const response = await this.transport({
          channel: 'email',
          destination,
          content: bundle.artifacts.email.content,
          bundle
        });
        return buildResult({
          channel: 'email',
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
          channel: 'email',
          destination,
          status: 'failed',
          success: false,
          dryRun: false,
          actualSend: false,
          attemptTimestamp: now,
          error: buildError(error?.code ?? 'UNKNOWN', error?.message ?? 'Unknown email mock transport failure.'),
          retryable: Boolean(error?.retryable),
          providerMode: this.mode
        });
      }
    }

    return buildResult({
      channel: 'email',
      destination,
      status: 'failed',
      success: false,
      dryRun: false,
      actualSend: false,
      attemptTimestamp: now,
      error: buildError('UNSUPPORTED_MODE', `Unsupported email delivery mode "${this.mode}"`),
      retryable: false,
      providerMode: this.mode
    });
  }
}
