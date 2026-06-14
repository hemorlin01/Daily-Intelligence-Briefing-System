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

async function callDeepSeek({ system, user, apiKey, apiUrl, model }) {
  const res = await fetch(apiUrl, {
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

async function enrichBatch(batch, { apiKey, apiUrl, model }, retries = 0) {
  const hasZh = batch.some(item => item.language === 'zh');
  const locale = hasZh ? 'zh' : 'en';
  const { system, user } = buildPrompt(batch, locale);

  try {
    const content = await callDeepSeek({ system, user, apiKey, apiUrl, model });
    return parseResponse(content);
  } catch (error) {
    if (retries < MAX_RETRIES) {
      await new Promise(r => setTimeout(r, RETRY_DELAY_MS));
      // Retry with smaller batch
      if (batch.length > 5 && retries >= 1) {
        const mid = Math.ceil(batch.length / 2);
        const [results1, results2] = await Promise.all([
          enrichBatch(batch.slice(0, mid), { apiKey, apiUrl, model }, retries + 1),
          enrichBatch(batch.slice(mid), { apiKey, apiUrl, model }, retries + 1)
        ]);
        return [...results1, ...results2];
      }
      return enrichBatch(batch, { apiKey, apiUrl, model }, retries + 1);
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
  let completed = 0;
  const total = batches.length;

  // Process in concurrent groups
  for (let i = 0; i < batches.length; i += MAX_CONCURRENT) {
    const group = batches.slice(i, i + MAX_CONCURRENT);
    const groupResults = await Promise.allSettled(
      group.map(batch => enrichBatch(batch, config))
    );

    for (const result of groupResults) {
      completed += 1;
      if (result.status === 'fulfilled') {
        results.push(...result.value);
      } else {
        console.error(`[LLM] Batch ${completed} failed: ${result.reason.message}`);
      }
      if (onProgress) {
        onProgress(completed, total);
      }
    }
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
    dryRun = false
  } = config;

  if (!apiKey) {
    throw new Error('DEEPSEEK_API_KEY or LLM_API_KEY is required for LLM enrichment');
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
  const batches = chunkArray(normalized, BATCH_SIZE);
  
  console.log(`[LLM] Starting enrichment: ${normalized.length} items in ${batches.length} batches (${BATCH_SIZE}/batch, ${MAX_CONCURRENT}x concurrent)`);
  
  const startTime = Date.now();
  const enrichedResults = await processQueue(batches, { apiKey, apiUrl, model }, onProgress);
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
