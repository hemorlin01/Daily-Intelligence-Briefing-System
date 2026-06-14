import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import {
  EmailDeliveryAdapter,
  TelegramDeliveryAdapter,
  BarkDeliveryAdapter,
  WeComDeliveryAdapter,
  splitWeComMarkdown,
  deliveryOutcomeExitCode,
  deliverRunBundle,
  executeDibsRun,
  executeScheduledDibsRun,
  retryRunBundleDelivery
} from '../src/index.js';
import { FIXED_NOW, makeRawEntry } from './fixtures/factories.js';

async function withTempDir(callback) {
  const directory = mkdtempSync(join(tmpdir(), 'dibs-delivery-'));
  try {
    return await callback(directory);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function withPatchedEnv(overrides, callback) {
  const originalValues = new Map();
  for (const [key, value] of Object.entries(overrides)) {
    originalValues.set(key, Object.prototype.hasOwnProperty.call(process.env, key) ? process.env[key] : undefined);
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }

  const restore = () => {
    for (const [key, value] of originalValues.entries()) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  };

  try {
    const result = callback();
    if (result && typeof result.then === 'function') {
      return result.finally(restore);
    }
    restore();
    return result;
  } catch (error) {
    restore();
    throw error;
  }
}

function buildRawItems() {
  return [
    makeRawEntry('reuters', {
      title: 'Federal Reserve signals caution on rates as inflation cools',
      url: 'https://example.com/raw-macro',
      canonical_text: 'The Federal Reserve signaled caution on future rate moves as inflation continued to cool across the quarter. '.repeat(8),
      snippet: 'Markets are adjusting to a more cautious rates path.',
      summary: 'Markets are adjusting to a more cautious rates path.'
    }),
    makeRawEntry('bloomberg', {
      title: 'Nvidia supplier expands AI packaging capacity after export-rule changes',
      url: 'https://example.com/raw-tech',
      canonical_text: 'Nvidia suppliers expanded AI packaging capacity after export-rule changes altered production planning and deployment timing. '.repeat(8),
      snippet: 'The move affects AI deployment and infrastructure capacity.',
      summary: 'The move affects AI deployment and infrastructure capacity.'
    }),
    makeRawEntry('caixin', {
      title: 'Provincial factories raise investment plans as Beijing targets manufacturing upgrades',
      url: 'https://example.com/raw-china',
      canonical_text: 'Factories raised investment plans as Beijing pushed another round of manufacturing upgrades through regional industrial plans. '.repeat(8),
      snippet: 'The plans alter China capacity expectations.',
      summary: 'The plans alter China capacity expectations.'
    }),
    makeRawEntry('bloomberg-green', {
      title: 'European utilities accelerate battery and grid plans after new clean-power rules',
      url: 'https://example.com/raw-climate',
      canonical_text: 'European utilities accelerated battery and grid plans after new clean-power rules were announced across multiple markets. '.repeat(8),
      snippet: 'Utilities are changing project timing after new rules.',
      summary: 'Utilities are changing project timing after new rules.'
    }),
    makeRawEntry('brookings', {
      title: 'Brookings argues industrial subsidies need tighter accountability rules',
      url: 'https://example.com/raw-policy',
      canonical_text: 'Brookings argued that industrial subsidies need tighter accountability rules and clearer performance benchmarks for agencies. '.repeat(8),
      snippet: 'The paper argues for tighter subsidy accountability.',
      summary: 'The paper argues for tighter subsidy accountability.'
    })
  ];
}

function writeTempDeliveryRules(directory, overrides = {}) {
  const rules = JSON.parse(readFileSync(resolve(process.cwd(), 'config', 'delivery-rules.json'), 'utf8'));
  const merged = {
    ...rules,
    artifacts: {
      ...rules.artifacts,
      output_root: join(directory, 'runs'),
      ledger_path: join(directory, 'state', 'delivery-ledger.json')
    },
    delivery: {
      email: {
        ...rules.delivery.email,
        mode: 'local-file',
        ...(overrides.delivery?.email ?? {})
      },
      telegram: {
        ...rules.delivery.telegram,
        mode: 'local-file',
        ...(overrides.delivery?.telegram ?? {})
      },
      bark: {
        ...rules.delivery.bark,
        ...(overrides.delivery?.bark ?? {})
      },
      wecom: {
        ...rules.delivery.wecom,
        ...(overrides.delivery?.wecom ?? {})
      }
    },
    retry: {
      ...rules.retry,
      ...(overrides.retry ?? {})
    },
    schedule: {
      ...rules.schedule,
      ...(overrides.schedule ?? {})
    }
  };

  const path = join(directory, 'delivery-rules.json');
  writeFileSync(path, JSON.stringify(merged, null, 2));
  return path;
}

test('one-shot execution produces a full run bundle and delivery status artifacts', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = writeTempDeliveryRules(directory);
    const result = await executeDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath
    });

    assert.equal(result.runBundle.run_id.length > 0, true);
    assert.equal(existsSync(join(result.outputDir, 'run_bundle.json')), true);
    assert.equal(existsSync(join(result.outputDir, 'delivery_status.json')), true);
    assert.equal(existsSync(join(result.outputDir, 'run_log.json')), true);
    assert.equal(result.deliveryStatus.final_outcome, 'success');
  });
});

test('dry-run mode does not perform actual send but records correct status', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = writeTempDeliveryRules(directory);
    const result = await executeDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath,
      dryRun: true
    });

    assert.equal(result.deliveryStatus.channels.email.status, 'dry_run');
    assert.equal(result.deliveryStatus.channels.telegram.status, 'dry_run');
    assert.equal(result.deliveryStatus.channels.email.actual_send, false);
    assert.equal(existsSync(join(result.outputDir, 'delivery', 'email-delivered.txt')), false);
  });
});

test('bark adapter validates provider response and sends rendered compact content', async () => {
  const requests = [];
  const bundle = {
    run_timestamp: FIXED_NOW.toISOString(),
    artifacts: {
      telegram: { content: 'Headline\nSummary: Decision-useful context.\nWhy it matters: Concrete impact.' }
    }
  };
  const adapter = new BarkDeliveryAdapter({
    request: async (url, options) => {
      requests.push({ url, options });
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({ code: 200, message: 'success' })
      };
    }
  });

  const success = await adapter.deliver({ bundle, destination: 'device-key' });
  assert.equal(success.status, 'success');
  assert.equal(success.actual_send, true);
  assert.match(JSON.parse(requests[0].options.body).body, /Decision-useful context/);

  const failed = await new BarkDeliveryAdapter({
    request: async () => ({
      ok: true,
      status: 200,
      text: async () => JSON.stringify({ code: 400, message: 'invalid device key' })
    })
  }).deliver({ bundle, destination: 'bad-key' });
  assert.equal(failed.status, 'failed');
  assert.match(failed.error.message, /invalid device key/);
});

test('wecom adapter splits by UTF-8 bytes and fails honestly on a rejected chunk', async () => {
  const content = `# Briefing\n\n${'中文摘要。'.repeat(900)}`;
  const chunks = splitWeComMarkdown(content);
  assert.ok(chunks.length > 1);
  assert.equal(chunks.every((chunk) => Buffer.byteLength(chunk, 'utf8') <= 3800), true);

  let attempts = 0;
  const adapter = new WeComDeliveryAdapter({
    request: async () => {
      attempts += 1;
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify(
          attempts === 2
            ? { errcode: 40058, errmsg: 'markdown.content exceed max length 4096' }
            : { errcode: 0, errmsg: 'ok' }
        )
      };
    }
  });
  const result = await adapter.deliver({
    bundle: { artifacts: { wecom: { content } } },
    destination: 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test-key'
  });

  assert.equal(result.status, 'failed');
  assert.equal(result.actual_send, true);
  assert.equal(result.error.code, 'WECOM_40058');
  assert.equal(result.provider_metadata.sent_chunk_count, 1);

  const invalidDestination = await adapter.deliver({
    bundle: { artifacts: { wecom: { content: 'Briefing' } } },
    destination: 'https://example.com/private-webhook'
  });
  assert.equal(invalidDestination.status, 'failed');
  assert.equal(invalidDestination.destination, 'wecom-webhook');
});

test('sensitive Bark and WeCom destinations are redacted from run artifacts and ledger', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = writeTempDeliveryRules(directory);
    const barkKey = 'private-bark-device-key';
    const wecomWebhook = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=private-wecom-key';
    const ledgerPath = join(directory, 'state', 'delivery-ledger.json');
    mkdirSync(join(directory, 'state'), { recursive: true });
    writeFileSync(ledgerPath, JSON.stringify({
      version: 1,
      updated_at: FIXED_NOW.toISOString(),
      channel_attempts: {
        old_bark: {
          run_id: 'old-run',
          channel: 'bark',
          idempotency_key: 'old_bark',
          destination: barkKey
        },
        old_wecom: {
          run_id: 'old-run',
          channel: 'wecom',
          idempotency_key: 'old_wecom',
          destination: wecomWebhook
        }
      },
      runs: {
        'old-run': {
          run_id: 'old-run',
          channels: {
            bark: { destination: barkKey },
            wecom: { destination: wecomWebhook }
          }
        }
      }
    }, null, 2));
    const result = await executeDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath,
      dryRun: true,
      channels: ['bark', 'wecom'],
      environment: {
        BARK_DEVICE_KEY: barkKey,
        WECOM_WEBHOOK_URL: wecomWebhook
      }
    });

    const artifactText = [
      readFileSync(join(result.outputDir, 'run_bundle.json'), 'utf8'),
      readFileSync(join(result.outputDir, 'delivery_status.json'), 'utf8'),
      readFileSync(ledgerPath, 'utf8')
    ].join('\n');

    assert.equal(artifactText.includes(barkKey), false);
    assert.equal(artifactText.includes(wecomWebhook), false);
    assert.equal(result.deliveryStatus.channels.bark.destination, 'bark-device');
    assert.equal(result.deliveryStatus.channels.wecom.destination, 'wecom-webhook');
  });
});

test('delivery CLI exit code surfaces failed and partial channel outcomes', () => {
  assert.equal(deliveryOutcomeExitCode('success'), 0);
  assert.equal(deliveryOutcomeExitCode('dry_run'), 0);
  assert.equal(deliveryOutcomeExitCode('failed'), 1);
  assert.equal(deliveryOutcomeExitCode('partial_success'), 1);
});

test('delivery rejects unregistered channels with an explicit error', async () => {
  await assert.rejects(
    () => deliverRunBundle({
      bundle: {
        run_id: 'invalid-channel-run',
        run_timestamp: FIXED_NOW.toISOString(),
        run_status: 'degraded',
        selected_count: 0,
        selected_article_ids: [],
        artifacts: {
          email: { content: 'Email', path: 'email.txt' },
          telegram: { content: 'Telegram', path: 'telegram.txt' },
          markdown: { content: 'Markdown', path: 'archive.md' }
        },
        delivery_targets: {
          email: { enabled: true, mode: 'local-file', destination: 'email' },
          telegram: { enabled: true, mode: 'local-file', destination: 'telegram' },
          bark: { enabled: true, mode: 'bark-api', destination: 'bark' },
          wecom: { enabled: true, mode: 'wecom-webhook', destination: 'wecom' }
        },
        diagnostics_references: {
          delivery_status: 'delivery_status.json',
          run_log: 'run_log.json'
        },
        idempotency: {
          run_fingerprint: 'run-key',
          per_channel: {
            email: 'email-key',
            telegram: 'telegram-key',
            bark: 'bark-key',
            wecom: 'wecom-key'
          }
        },
        output_dir: 'artifacts/runs/invalid-channel-run'
      },
      channels: ['wechat']
    }),
    /Unsupported delivery channel "wechat"/
  );
});

test('email adapter returns structured success and failure shapes', async () => {
  await withTempDir(async (directory) => {
    const adapter = new EmailDeliveryAdapter({ mode: 'local-file' });
    const bundle = {
      output_dir: directory,
      artifacts: {
        email: {
          content: 'Email body'
        }
      }
    };

    const success = await adapter.deliver({
      bundle,
      destination: 'local-email-outbox'
    });
    assert.equal(success.channel, 'email');
    assert.equal(success.status, 'success');
    assert.equal(success.success, true);

    const failingAdapter = new EmailDeliveryAdapter({
      mode: 'mock',
      transport: async () => {
        const error = new Error('SMTP timeout');
        error.code = 'TIMEOUT';
        error.retryable = true;
        throw error;
      }
    });
    const failure = await failingAdapter.deliver({
      bundle,
      destination: 'local-email-outbox'
    });
    assert.equal(failure.channel, 'email');
    assert.equal(failure.status, 'failed');
    assert.equal(failure.error.code, 'TIMEOUT');
  });
});

test('telegram adapter returns structured success and failure shapes', async () => {
  await withTempDir(async (directory) => {
    const adapter = new TelegramDeliveryAdapter({ mode: 'local-file' });
    const bundle = {
      output_dir: directory,
      artifacts: {
        telegram: {
          content: 'Telegram body'
        }
      }
    };

    const success = await adapter.deliver({
      bundle,
      destination: 'local-telegram-outbox'
    });
    assert.equal(success.channel, 'telegram');
    assert.equal(success.status, 'success');

    const failingAdapter = new TelegramDeliveryAdapter({
      mode: 'mock',
      transport: async () => {
        const error = new Error('Telegram transient failure');
        error.code = 'TRANSIENT_ERROR';
        error.retryable = true;
        throw error;
      }
    });
    const failure = await failingAdapter.deliver({
      bundle,
      destination: 'local-telegram-outbox'
    });
    assert.equal(failure.channel, 'telegram');
    assert.equal(failure.status, 'failed');
    assert.equal(failure.error.code, 'TRANSIENT_ERROR');
  });
});

test('gmail-smtp mode validates required config and env correctly', async () => {
  await withTempDir(async (directory) => {
    const bundle = {
      output_dir: directory,
      run_timestamp: FIXED_NOW.toISOString(),
      artifacts: {
        email: {
          content: 'Email body'
        }
      }
    };

    const adapter = new EmailDeliveryAdapter({ mode: 'gmail-smtp' });
    const dryRunResult = await adapter.deliver({
      bundle,
      destination: 'recipient@example.com',
      dryRun: true
    });
    assert.equal(dryRunResult.status, 'dry_run');

    await withPatchedEnv({
      GMAIL_SMTP_USER: undefined,
      GMAIL_SMTP_APP_PASSWORD: undefined,
      GMAIL_FROM_ADDRESS: undefined
    }, async () => {
      const failure = await adapter.deliver({
        bundle,
        destination: 'recipient@example.com'
      });
      assert.equal(failure.status, 'failed');
      assert.equal(failure.error.code, 'MISSING_SMTP_USER');
    });

    await withPatchedEnv({
      GMAIL_SMTP_USER: 'sender@example.com',
      GMAIL_SMTP_APP_PASSWORD: 'app-password',
      GMAIL_FROM_ADDRESS: 'sender@example.com'
    }, async () => {
      const successAdapter = new EmailDeliveryAdapter({
        mode: 'gmail-smtp',
        transport: {
          sendMail: async () => ({
            messageId: '<message-id@example.com>',
            accepted: ['recipient@example.com']
          })
        }
      });
      const success = await successAdapter.deliver({
        bundle,
        destination: 'recipient@example.com'
      });
      assert.equal(success.status, 'success');
      assert.equal(success.provider_metadata.message_id, '<message-id@example.com>');
    });
  });
});

test('gmail-smtp accepts multiple recipients and trims whitespace', async () => {
  await withTempDir(async (directory) => {
    const bundle = {
      output_dir: directory,
      run_timestamp: FIXED_NOW.toISOString(),
      artifacts: {
        email: {
          content: 'Email body'
        }
      }
    };

    await withPatchedEnv({
      GMAIL_SMTP_USER: 'sender@example.com',
      GMAIL_SMTP_APP_PASSWORD: 'app-password',
      GMAIL_FROM_ADDRESS: 'sender@example.com'
    }, async () => {
      let capturedTo = null;
      const adapter = new EmailDeliveryAdapter({
        mode: 'gmail-smtp',
        transport: {
          sendMail: async (message) => {
            capturedTo = message.to;
            return {
              messageId: '<message-id@example.com>',
              accepted: message.to
            };
          }
        }
      });

      const result = await adapter.deliver({
        bundle,
        destination: ' person1@gmail.com, person2@qq.com , , person3@163.com '
      });

      assert.equal(result.status, 'success');
      assert.deepEqual(capturedTo, ['person1@gmail.com', 'person2@qq.com', 'person3@163.com']);
      assert.equal(result.destination, 'person1@gmail.com, person2@qq.com, person3@163.com');
    });
  });
});

test('gmail-smtp rejects invalid recipient list', async () => {
  await withTempDir(async (directory) => {
    const bundle = {
      output_dir: directory,
      run_timestamp: FIXED_NOW.toISOString(),
      artifacts: {
        email: {
          content: 'Email body'
        }
      }
    };

    await withPatchedEnv({
      GMAIL_SMTP_USER: 'sender@example.com',
      GMAIL_SMTP_APP_PASSWORD: 'app-password',
      GMAIL_FROM_ADDRESS: 'sender@example.com'
    }, async () => {
      const adapter = new EmailDeliveryAdapter({ mode: 'gmail-smtp' });
      const result = await adapter.deliver({
        bundle,
        destination: 'good@example.com, bad@@example.com'
      });
      assert.equal(result.status, 'failed');
      assert.equal(result.error.code, 'INVALID_EMAIL_DESTINATION');
      assert.match(result.error.message, /bad@@example\.com/);
    });
  });
});

test('gmail-smtp rejects empty recipient list', async () => {
  await withTempDir(async (directory) => {
    const bundle = {
      output_dir: directory,
      run_timestamp: FIXED_NOW.toISOString(),
      artifacts: {
        email: {
          content: 'Email body'
        }
      }
    };

    await withPatchedEnv({
      GMAIL_SMTP_USER: 'sender@example.com',
      GMAIL_SMTP_APP_PASSWORD: 'app-password',
      GMAIL_FROM_ADDRESS: 'sender@example.com'
    }, async () => {
      const adapter = new EmailDeliveryAdapter({ mode: 'gmail-smtp' });
      const result = await adapter.deliver({
        bundle,
        destination: ' , , '
      });
      assert.equal(result.status, 'failed');
      assert.equal(result.error.code, 'MISSING_EMAIL_DESTINATION');
    });
  });
});

test('telegram-bot-api mode validates required config and env correctly', async () => {
  await withTempDir(async (directory) => {
    const bundle = {
      output_dir: directory,
      artifacts: {
        telegram: {
          content: 'Telegram body'
        }
      }
    };

    const adapter = new TelegramDeliveryAdapter({ mode: 'telegram-bot-api' });
    const dryRunResult = await adapter.deliver({
      bundle,
      destination: '@dibs_channel',
      dryRun: true
    });
    assert.equal(dryRunResult.status, 'dry_run');

    await withPatchedEnv({
      TELEGRAM_BOT_TOKEN: undefined,
      DIBS_TELEGRAM_BOT_TOKEN: undefined
    }, async () => {
      const failure = await adapter.deliver({
        bundle,
        destination: '@dibs_channel'
      });
      assert.equal(failure.status, 'failed');
      assert.equal(failure.error.code, 'MISSING_TELEGRAM_BOT_TOKEN');
    });

    await withPatchedEnv({
      TELEGRAM_BOT_TOKEN: 'bot-token'
    }, async () => {
      const successAdapter = new TelegramDeliveryAdapter({
        mode: 'telegram-bot-api',
        transport: async () => ({
          ok: true,
          status: 200,
          text: async () => JSON.stringify({
            ok: true,
            result: {
              message_id: 42,
              chat: { id: '@dibs_channel' }
            }
          })
        })
      });
      const success = await successAdapter.deliver({
        bundle,
        destination: '@dibs_channel'
      });
      assert.equal(success.status, 'success');
      assert.equal(success.provider_metadata.message_id, 42);
    });
  });
});

test('duplicate-send protection blocks a second send of the same successful run', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = writeTempDeliveryRules(directory);
    const first = await executeDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath
    });

    const second = await retryRunBundleDelivery({
      runBundlePath: join(first.outputDir, 'run_bundle.json'),
      deliveryRulesPath
    });

    assert.equal(second.deliveryStatus.channels.email.status, 'duplicate_blocked');
    assert.equal(second.deliveryStatus.channels.telegram.status, 'duplicate_blocked');
  });
});

test('explicit replay can resend when allowed', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = writeTempDeliveryRules(directory);
    const first = await executeDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath
    });

    const replay = await retryRunBundleDelivery({
      runBundlePath: join(first.outputDir, 'run_bundle.json'),
      deliveryRulesPath,
      replay: true
    });

    assert.equal(replay.deliveryStatus.channels.email.status, 'success');
    assert.equal(replay.deliveryStatus.channels.telegram.status, 'success');
    assert.equal(replay.deliveryStatus.channels.email.actual_send, true);
  });
});

test('partial success is represented correctly when one channel succeeds and another fails', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = writeTempDeliveryRules(directory);
    const result = await executeDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath,
      adapterOverrides: {
        telegram: new TelegramDeliveryAdapter({
          mode: 'mock',
          transport: async () => {
            const error = new Error('Telegram failed');
            error.code = 'TIMEOUT';
            error.retryable = false;
            throw error;
          }
        })
      }
    });

    assert.equal(result.deliveryStatus.final_outcome, 'partial_success');
    assert.equal(result.deliveryStatus.channels.email.status, 'success');
    assert.equal(result.deliveryStatus.channels.telegram.status, 'failed');
  });
});

test('retries do not violate idempotency', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = writeTempDeliveryRules(directory, {
      retry: {
        max_attempts: 3
      }
    });
    let attempts = 0;
    const result = await executeDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath,
      channels: ['email'],
      adapterOverrides: {
        email: new EmailDeliveryAdapter({
          mode: 'mock',
          transport: async () => {
            attempts += 1;
            if (attempts === 1) {
              const error = new Error('Transient mail error');
              error.code = 'TRANSIENT_ERROR';
              error.retryable = true;
              throw error;
            }
            return {
              status: 'success',
              success: true,
              actual_send: true
            };
          }
        })
      }
    });

    assert.equal(result.deliveryStatus.channels.email.attempt_count, 2);
    assert.equal(result.deliveryStatus.channels.email.retry_count, 1);

    const second = await retryRunBundleDelivery({
      runBundlePath: join(result.outputDir, 'run_bundle.json'),
      deliveryRulesPath,
      channels: ['email']
    });
    assert.equal(second.deliveryStatus.channels.email.status, 'duplicate_blocked');
  });
});

test('failed sends record error details', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = writeTempDeliveryRules(directory);
    const result = await executeDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath,
      channels: ['telegram'],
      adapterOverrides: {
        telegram: new TelegramDeliveryAdapter({
          mode: 'mock',
          transport: async () => {
            const error = new Error('Chat not found');
            error.code = 'CHAT_NOT_FOUND';
            error.retryable = false;
            throw error;
          }
        })
      }
    });

    assert.equal(result.deliveryStatus.channels.telegram.status, 'failed');
    assert.equal(result.deliveryStatus.channels.telegram.error.code, 'CHAT_NOT_FOUND');
  });
});

test('scheduled execution uses the same orchestration path as one-shot execution', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = writeTempDeliveryRules(directory, {
      schedule: {
        enabled: true,
        timezone: 'UTC',
        days: ['Mon'],
        hour: 12,
        minute: 0
      }
    });

    const oneShot = await executeDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath,
      dryRun: true
    });
    const scheduled = await executeScheduledDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      deliveryRulesPath,
      dryRun: true
    });

    assert.equal(scheduled.scheduled, true);
    assert.equal(oneShot.runBundle.selected_count, scheduled.runBundle.selected_count);
    assert.equal(oneShot.runBundle.run_status, scheduled.runBundle.run_status);
    assert.equal(existsSync(scheduled.schedulerDebugPath), true);
  });
});

test('missing required rendered artifacts fails clearly', async () => {
  const badBundle = {
    run_id: 'bad-run',
    run_timestamp: FIXED_NOW.toISOString(),
    run_status: 'degraded',
    selected_count: 1,
    selected_article_ids: ['a1'],
    artifacts: {
      email: { content: '', path: 'x' },
      telegram: { content: 'telegram', path: 'y' },
      markdown: { content: 'md', path: 'z' }
    },
    delivery_targets: {
      email: { enabled: true, mode: 'local-file', destination: 'local-email-outbox' },
      telegram: { enabled: true, mode: 'local-file', destination: 'local-telegram-outbox' }
    },
    diagnostics_references: {
      delivery_status: 'delivery_status.json',
      run_log: 'run_log.json'
    },
    idempotency: {
      run_fingerprint: 'abc',
      per_channel: {
        email: 'email-key',
        telegram: 'telegram-key'
      }
    },
    output_dir: 'artifacts/runs/bad-run'
  };

  await assert.rejects(
    () => deliverRunBundle({
      bundle: badBundle
    }),
    /artifacts.email.content/
  );
});

test('operational logs and status artifacts are produced', async () => {
  await withTempDir(async (directory) => {
    const deliveryRulesPath = writeTempDeliveryRules(directory);
    const result = await executeDibsRun({
      rawItems: buildRawItems(),
      now: FIXED_NOW,
      runTimestamp: FIXED_NOW.toISOString(),
      deliveryRulesPath,
      dryRun: true
    });

    const deliveryStatus = JSON.parse(readFileSync(join(result.outputDir, 'delivery_status.json'), 'utf8'));
    const runLog = JSON.parse(readFileSync(join(result.outputDir, 'run_log.json'), 'utf8'));
    assert.equal(deliveryStatus.run_id, result.runBundle.run_id);
    assert.equal(runLog.final_outcome, 'dry_run');
    assert.equal('delivery' in runLog.phase_statuses, true);
  });
});
