import { createHash } from 'node:crypto';

const BATCH_SIZE = 15;
const MAX_CONCURRENT = 5;
const MAX_RETRIES = 2;
const RETRY_DELAY_MS = 2000;

function buildPrompt(batch, locale) {
  const isZh = locale === 'zh';
  
  const system = isZh
    ? `你是一位资深情报分析师，为政府关系专业人士生成新闻摘要。对每篇文章：
1. factual_summary: 60-100字中文摘要，只陈述事实，不含观点
2. why_it_matters: 20-50字，格式为"主体在地区 — 主题 / 战略维度 — 一句话影响判断"`

    : `You are a senior intelligence analyst generating news summaries for a government relations professional. For each article:
1. factual_summary: 50-80 word English summary, factual only, no opinion
2. why_it_matters: 20-35 words, format "Entity in Region — Topic / Strategic Dimension — one-sentence impact assessment"`;

  const items = batch.map((item, i) => 
    `${i + 1}. [id:${item.article_id}] ${item.title} (${item.source_display_name})\n   Snippet: ${item.snippet || '(none)'}`
  ).join('\n\n');

  const user = isZh
    ? `为以下${batch.length}篇文章生成摘要和重要性分析。仅返回JSON数组：\n\n${items}\n\nRespond with ONLY this JSON array:\n[{"article_id":"id", "factual_summary":"...", "why_it_matters":"..."}, ...]`
    
    : `Generate summaries and why-it-matters for these ${batch.length} articles. Return ONLY a JSON array:\n\n${items}\n\nRespond with ONLY this JSON array:\n[{"article_id":"id", "factual_summary":"...", "why_it_matters":"..."}, ...]`;

  return { system, user };
}

function parseResponse(text) {
  const trimmed = text.trim();
  
  // Try to extract JSON from markdown code blocks
  const codeBlockMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/);
  const jsonText = codeBlockMatch ? codeBlockMatch[1].trim() : trimmed;
  
  // Try to find a JSON array
  const arrayMatch = jsonText.match(/\[\s*\{[\s\S]*\}\s*\]/);
  const toParse = arrayMatch ? arrayMatch[0] : jsonText;
  
  try {
    const parsed = JSON.parse(toParse);
    if (!Array.isArray(parsed)) {
      throw new Error('Response is not an array');
    }
    return parsed;
  } catch (e) {
    throw new Error(`Failed to parse LLM response as JSON array: ${e.message}\nRaw: ${trimmed.slice(0, 200)}`);
  }
}

function validateBatchResults(batch, results) {
  const expectedIds = new Set(batch.map((item) => item.article_id));
  const returnedIds = new Set();

  for (const entry of results) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
      throw new Error('LLM response contains a non-object entry');
    }
    if (!expectedIds.has(entry.article_id)) {
      throw new Error(`LLM response contains unknown article_id "${entry.article_id ?? 'missing'}"`);
    }
    if (returnedIds.has(entry.article_id)) {
      throw new Error(`LLM response contains duplicate article_id "${entry.article_id}"`);
    }
    if (typeof entry.factual_summary !== 'string' || entry.factual_summary.trim().length === 0) {
      throw new Error(`LLM response is missing factual_summary for article_id "${entry.article_id}"`);
    }
    if (typeof entry.why_it_matters !== 'string' || entry.why_it_matters.trim().length === 0) {
      throw new Error(`LLM response is missing why_it_matters for article_id "${entry.article_id}"`);
    }
    returnedIds.add(entry.article_id);
  }

  for (const expectedId of expectedIds) {
    if (!returnedIds.has(expectedId)) {
      throw new Error(`LLM response is missing article_id "${expectedId}"`);
    }
  }

  return results;
}

async function callDeepSeek({ system, user, apiKey, apiUrl, model, request }) {
  const res = await request(apiUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model,
      messages: [
        { role: 'system', content: system },
        { role: 'user', content: user }
      ],
      temperature: 0.3,
      max_tokens: 4096
    })
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`DeepSeek API error ${res.status}: ${res.statusText} — ${body.slice(0, 200)}`);
  }

  const data = await res.json();
  const content = data.choices?.[0]?.message?.content;
  
  if (!content) {
    throw new Error(`DeepSeek returned empty response: ${JSON.stringify(data).slice(0, 200)}`);
  }

  return content;
}

async function enrichBatch(batch, config, retries = 0) {
  const locale = batch[0]?.language === 'zh' ? 'zh' : 'en';
  const { system, user } = buildPrompt(batch, locale);

  try {
    const content = await callDeepSeek({ system, user, ...config });
    return validateBatchResults(batch, parseResponse(content));
  } catch (error) {
    if (retries < config.maxRetries) {
      await new Promise((resolve) => setTimeout(resolve, config.retryDelayMs));
      // Retry with smaller batch
      if (batch.length > 5 && retries >= 1) {
        const mid = Math.ceil(batch.length / 2);
        const [results1, results2] = await Promise.all([
          enrichBatch(batch.slice(0, mid), config, retries + 1),
          enrichBatch(batch.slice(mid), config, retries + 1)
        ]);
        return [...results1, ...results2];
      }
      return enrichBatch(batch, config, retries + 1);
    }
    throw error;
  }
}

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

async function processQueue(batches, config, onProgress) {
  const results = [];
  const failures = [];
  let completed = 0;
  const total = batches.length;

  // Process in concurrent groups
  for (let i = 0; i < batches.length; i += config.maxConcurrent) {
    const group = batches.slice(i, i + config.maxConcurrent);
    const groupResults = await Promise.allSettled(
      group.map(batch => enrichBatch(batch, config))
    );

    for (const result of groupResults) {
      completed += 1;
      if (result.status === 'fulfilled') {
        results.push(...result.value);
      } else {
        failures.push({
          batch: completed,
          message: result.reason?.message ?? 'Unknown LLM batch failure'
        });
      }
      if (onProgress) {
        onProgress(completed, total);
      }
    }
  }

  if (failures.length > 0) {
    const details = failures.map((failure) => `batch ${failure.batch}: ${failure.message}`).join('; ');
    throw new Error(`LLM enrichment failed for ${failures.length}/${total} batch(es): ${details}`);
  }

  return results;
}

function normalizeItem(item) {
  const inner = item.item || item;
  const snippet = 
    inner.snippet || 
    inner.summary || 
    inner.canonical_text || 
    inner.title || 
    '';

  return {
    article_id: inner.article_id || item.article_id || 
      createHash('md5').update(inner.url || inner.title || '').digest('hex').slice(0, 12),
    title: inner.title || '',
    snippet: snippet.slice(0, 600),
    source_display_name: inner.source_display_name || item.source_id || 'unknown',
    language: inner.language || 'en'
  };
}

export async function enrichRawItems(rawItems, config) {
  const {
    apiKey = process.env.DEEPSEEK_API_KEY || process.env.LLM_API_KEY,
    apiUrl = process.env.LLM_API_URL || 'https://api.deepseek.com/v1/chat/completions',
    model = process.env.LLM_MODEL || 'deepseek-chat',
    onProgress = null,
    dryRun = false,
    request = globalThis.fetch,
    maxRetries = MAX_RETRIES,
    retryDelayMs = RETRY_DELAY_MS,
    batchSize = BATCH_SIZE,
    maxConcurrent = MAX_CONCURRENT
  } = config;

  if (!apiKey) {
    throw new Error('DEEPSEEK_API_KEY or LLM_API_KEY is required for LLM enrichment');
  }
  if (typeof request !== 'function') {
    throw new Error('LLM enrichment requires a fetch-compatible request implementation');
  }

  if (dryRun) {
    return {
      enriched: rawItems,
      stats: {
        total_items: rawItems.length,
        enriched_items: 0,
        batches: 0,
        dry_run: true
      }
    };
  }

  const normalized = rawItems.map(normalizeItem);
  const languageGroups = new Map();
  for (const item of normalized) {
    const language = item.language === 'zh' ? 'zh' : 'en';
    const group = languageGroups.get(language) ?? [];
    group.push(item);
    languageGroups.set(language, group);
  }
  const batches = Array.from(languageGroups.values())
    .flatMap((group) => chunkArray(group, batchSize));
  
  console.log(`[LLM] Starting enrichment: ${normalized.length} items in ${batches.length} language-specific batches (${batchSize}/batch, ${maxConcurrent}x concurrent)`);
  
  const startTime = Date.now();
  const enrichedResults = await processQueue(
    batches,
    { apiKey, apiUrl, model, request, maxRetries, retryDelayMs, maxConcurrent },
    onProgress
  );
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  // Build lookup map
  const enrichmentMap = new Map();
  for (const entry of enrichedResults) {
    if (entry.article_id) {
      enrichmentMap.set(entry.article_id, {
        llm_factual_summary: entry.factual_summary || '',
        llm_why_it_matters: entry.why_it_matters || ''
      });
    }
  }

  // Merge enrichment back into raw items
  let enriched = 0;
  const enrichedItems = rawItems.map(item => {
    const normalized = normalizeItem(item);
    const enrichment = enrichmentMap.get(normalized.article_id);
    if (enrichment && enrichment.llm_factual_summary) {
      enriched += 1;
      const inner = item.item || item;
      return {
        ...item,
        item: {
          ...inner,
          llm_factual_summary: enrichment.llm_factual_summary,
          llm_why_it_matters: enrichment.llm_why_it_matters
        }
      };
    }
    return item;
  });

  console.log(`[LLM] Enrichment complete: ${enriched}/${normalized.length} items enriched in ${elapsed}s`);

  return {
    enriched: enrichedItems,
    stats: {
      total_items: normalized.length,
      enriched_items: enriched,
      batches: batches.length,
      elapsed_seconds: parseFloat(elapsed),
      dry_run: false
    }
  };
}
