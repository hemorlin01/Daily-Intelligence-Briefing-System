import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { buildCandidatePools, buildSemanticCards } from '../src/index.js';
import { enrichRawItems } from '../src/llm/enrich-raw-items.js';
import { FIXED_NOW, makeRawEntry } from './fixtures/factories.js';

test('LLM enrichment fields survive the Phase 1 canonical boundary', () => {
  const rawEntry = makeRawEntry('reuters', {
    llm_factual_summary: 'Regulators issued a detailed update affecting advanced-chip suppliers and compliance planning across several markets.',
    llm_why_it_matters: 'The update changes near-term compliance assumptions for semiconductor suppliers operating across regulated markets.'
  });

  const result = buildCandidatePools({
    rawItems: [rawEntry],
    now: FIXED_NOW,
    fetchedAt: FIXED_NOW.toISOString()
  });

  assert.equal(result.mainPool.length, 1);
  assert.equal(result.mainPool[0].llm_factual_summary, rawEntry.item.llm_factual_summary);
  assert.equal(result.mainPool[0].llm_why_it_matters, rawEntry.item.llm_why_it_matters);
});

test('LLM enrichment separates English and Chinese items into language-specific requests', async () => {
  const requests = [];
  const rawItems = [
    makeRawEntry('reuters', {
      title: 'Regulators update semiconductor export controls',
      url: 'https://example.com/en-item',
      language: 'en'
    }),
    makeRawEntry('wsj-cn-daily', {
      title: '中国制造业投资计划调整',
      url: 'https://example.com/zh-item',
      language: 'zh'
    })
  ];

  const request = async (_url, options) => {
    const body = JSON.parse(options.body);
    const userPrompt = body.messages.find((message) => message.role === 'user').content;
    requests.push(userPrompt);
    const articleIds = [...userPrompt.matchAll(/\[id:([^\]]+)\]/g)].map((match) => match[1]);
    const isChinese = userPrompt.includes('中国制造业投资计划调整');
    const content = articleIds.map((articleId) => ({
      article_id: articleId,
      factual_summary: isChinese
        ? '中国制造业投资计划出现调整，企业正在重新评估产能与供应链安排。'
        : 'Regulators updated semiconductor export controls, prompting suppliers to review compliance plans and regional capacity decisions.',
      why_it_matters: isChinese
        ? '这项调整影响制造业产能配置与供应链决策。'
        : 'The update changes compliance and capacity assumptions for advanced-chip suppliers.'
    }));
    return {
      ok: true,
      status: 200,
      json: async () => ({
        choices: [{ message: { content: JSON.stringify(content) } }]
      })
    };
  };

  const result = await enrichRawItems(rawItems, {
    apiKey: 'test-key',
    request,
    maxRetries: 0
  });

  assert.equal(requests.length, 2);
  assert.equal(requests.some((prompt) => prompt.includes('Regulators update') && prompt.includes('中国制造业')), false);
  assert.equal(result.stats.enriched_items, 2);
  assert.match(result.enriched[0].item.llm_factual_summary, /^Regulators/);
  assert.match(result.enriched[1].item.llm_factual_summary, /^中国/);
});

test('LLM enrichment rejects the run when any batch exhausts retries', async () => {
  const request = async () => ({
    ok: false,
    status: 500,
    statusText: 'Server Error',
    text: async () => 'provider unavailable'
  });

  await assert.rejects(
    () => enrichRawItems([makeRawEntry('reuters')], {
      apiKey: 'test-key',
      request,
      maxRetries: 0
    }),
    /LLM enrichment failed/
  );
});

test('LLM enrichment rejects incomplete provider responses', async () => {
  const request = async () => ({
    ok: true,
    status: 200,
    json: async () => ({
      choices: [{ message: { content: '[]' } }]
    })
  });

  await assert.rejects(
    () => enrichRawItems([makeRawEntry('reuters')], {
      apiKey: 'test-key',
      request,
      maxRetries: 0
    }),
    /missing article_id/
  );
});

test('validated LLM content reaches the final semantic card', () => {
  const rawEntry = makeRawEntry('reuters', {
    title: 'Regulators publish a new advanced-chip export framework',
    llm_factual_summary: 'Regulators published a new export framework covering advanced chips, manufacturing tools, reseller screening, and compliance reporting. Suppliers are reviewing contracts, inventory buffers, customer checks, and regional capacity plans as implementation details move toward formal adoption.',
    llm_why_it_matters: 'The framework changes compliance costs and capacity planning for advanced-chip suppliers serving customers across regulated markets.'
  });
  const candidate = buildCandidatePools({
    rawItems: [rawEntry],
    now: FIXED_NOW,
    fetchedAt: FIXED_NOW.toISOString()
  });
  const semantic = buildSemanticCards({
    canonicalRecords: candidate.mainPool,
    runTimestamp: FIXED_NOW.toISOString()
  });

  assert.equal(semantic.cards.length, 1);
  assert.equal(semantic.cards[0].metadata.llm_enriched, true);
  assert.match(semantic.cards[0].factual_summary, /^Regulators published/);
  assert.match(semantic.cards[0].why_it_matters, /^The framework changes/);
});

test('GitHub Actions uses only registered delivery channels', () => {
  const workflow = readFileSync('.github/workflows/dibs.yml', 'utf8');
  assert.match(workflow, /node-version: '24'/);
  assert.match(workflow, /CHANNELS="wecom"/);
  assert.doesNotMatch(workflow, /CHANNELS="\$CHANNELS,wechat"/);
  assert.doesNotMatch(workflow, /WECHAT_PUSH_TOKEN/);
  assert.match(workflow, /DEEPSEEK_API_KEY is not configured/);
  assert.match(workflow, /WECOM_WEBHOOK_URL is not configured/);
});
