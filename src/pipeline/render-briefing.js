import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { loadRenderingRules } from '../config/load-config.js';
import { validateRenderedBriefing, validateRenderingInput } from '../models/rendering.js';
import { normalizeWhitespace } from '../utils/text.js';

const DEFAULT_RENDERING_RULES_PATH = resolve(process.cwd(), 'config', 'rendering-rules.json');

function formatDate(runTimestamp, rules) {
  return new Intl.DateTimeFormat(rules.briefing.date_format, {
    dateStyle: 'long',
    timeZone: rules.briefing.timezone
  }).format(new Date(runTimestamp));
}

function statusLabel(selectionResult, rules) {
  return rules.status_display.labels[selectionResult.run_status];
}

function formatKeywords(keywords) {
  return keywords.join(', ');
}

function truncateText(text, maxChars) {
  const normalized = normalizeWhitespace(text);
  if (normalized.length <= maxChars) {
    return { text: normalized, truncated: false };
  }

  return {
    text: `${normalized.slice(0, maxChars - 1).trimEnd()}…`,
    truncated: true
  };
}

function buildRenderingBlocks(selectionResult, rules) {
  const blockBuckets = new Map(
    rules.blocks.order.map((blockId) => [blockId, {
      block_id: blockId,
      label: rules.blocks.definitions[blockId].label,
      domains: [...rules.blocks.definitions[blockId].domains],
      entry_article_ids: [],
      items: []
    }])
  );

  for (const item of selectionResult.selected_items) {
    const blockId = rules.blocks.domain_to_block[item.primary_domain];
    const block = blockBuckets.get(blockId);
    block.entry_article_ids.push(item.article_id);
    block.items.push(item);
  }

  return rules.blocks.order
    .map((blockId) => blockBuckets.get(blockId))
    .filter((block) => block.items.length > 0);
}

function buildHeaderLines(selectionResult, rules, runTimestamp) {
  const lines = [
    rules.briefing.title,
    formatDate(runTimestamp, rules)
  ];

  if (rules.status_display.show_run_status) {
    lines.push(`Status: ${statusLabel(selectionResult, rules)}`);
  }

  lines.push(`Selected items: ${selectionResult.selected_count}`);
  return lines;
}

function renderEmail(selectionResult, blocks, rules, runTimestamp) {
  const lines = buildHeaderLines(selectionResult, rules, runTimestamp);
  lines.push('');

  const entryArticleIds = [];
  for (const block of blocks) {
    lines.push(block.label);
    for (const item of block.items) {
      entryArticleIds.push(item.article_id);
      lines.push(`- ${item.title}`);
      lines.push(`  Source: ${item.source_display_name}`);
      lines.push(`  URL: ${item.url}`);
      if (rules.email.show_keywords) {
        lines.push(`  Keywords: ${formatKeywords(item.candidate_keywords)}`);
      }
      lines.push(`  Summary: ${normalizeWhitespace(item.factual_summary)}`);
      lines.push(`  Why it matters: ${normalizeWhitespace(item.why_it_matters)}`);
      lines.push('');
    }
  }

  if (rules.email.include_footer) {
    lines.push(`Footer: ${selectionResult.selected_count} items | ${statusLabel(selectionResult, rules)}`);
  }

  return {
    content: lines.join('\n'),
    entry_article_ids: entryArticleIds,
    omitted_article_ids: []
  };
}

function renderMarkdown(selectionResult, blocks, rules, runTimestamp) {
  const lines = [
    `# ${rules.briefing.title}`,
    '',
    `- Date: ${formatDate(runTimestamp, rules)}`,
    `- Run status: ${statusLabel(selectionResult, rules)}`,
    `- Selected count: ${selectionResult.selected_count}`,
    ''
  ];

  const entryArticleIds = [];
  for (const block of blocks) {
    lines.push(`## ${block.label}`);
    lines.push('');
    for (const item of block.items) {
      entryArticleIds.push(item.article_id);
      lines.push(`### ${item.title}`);
      lines.push('');
      lines.push(`- Article ID: ${item.article_id}`);
      lines.push(`- Source: ${item.source_display_name}`);
      lines.push(`- URL: ${item.url}`);
      lines.push(`- Primary domain: ${item.primary_domain}`);
      lines.push(`- Keywords: ${formatKeywords(item.candidate_keywords)}`);
      lines.push(`- Selection reasons: ${item.selection_reason_codes.join(', ')}`);
      lines.push('');
      lines.push(`**Summary**: ${normalizeWhitespace(item.factual_summary)}`);
      lines.push('');
      lines.push(`**Why it matters**: ${normalizeWhitespace(item.why_it_matters)}`);
      lines.push('');
    }
  }

  if (rules.markdown.include_footer) {
    lines.push(`_Run status: ${statusLabel(selectionResult, rules)} | Selected items: ${selectionResult.selected_count}_`);
  }

  return {
    content: lines.join('\n'),
    entry_article_ids: entryArticleIds,
    omitted_article_ids: []
  };
}

function renderTelegramForStage(selectionResult, blocks, rules, runTimestamp, stage) {
  const lines = [
    `${rules.briefing.title} | ${formatDate(runTimestamp, rules)}`,
    `Status: ${statusLabel(selectionResult, rules)}`
  ];

  const entryArticleIds = [];
  const truncation = {
    summary_count: 0,
    why_count: 0,
    total_count: 0
  };

  for (const block of blocks) {
    lines.push('');
    lines.push(`${rules.telegram.block_heading_prefix} ${block.label}`);

    for (const item of block.items) {
      entryArticleIds.push(item.article_id);
      const summary = truncateText(item.factual_summary, stage.summary_max_chars);
      const why = truncateText(item.why_it_matters, stage.why_max_chars);

      if (summary.truncated) {
        truncation.summary_count += 1;
      }
      if (why.truncated) {
        truncation.why_count += 1;
      }

      lines.push(`${item.title} — ${item.source_display_name}`);
      lines.push(`Summary: ${summary.text}`);
      lines.push(`Why: ${why.text}`);
      lines.push(item.url);
    }
  }

  truncation.total_count = truncation.summary_count + truncation.why_count;

  return {
    content: lines.join('\n'),
    entry_article_ids: entryArticleIds,
    truncation
  };
}

function renderTelegram(selectionResult, blocks, rules, runTimestamp) {
  const stages = [
    {
      name: 'standard',
      summary_max_chars: rules.telegram.summary_max_chars,
      why_max_chars: rules.telegram.why_max_chars
    },
    {
      name: 'compact',
      summary_max_chars: rules.telegram.compact_summary_max_chars,
      why_max_chars: rules.telegram.compact_why_max_chars
    },
    {
      name: 'minimal',
      summary_max_chars: rules.telegram.minimal_summary_max_chars,
      why_max_chars: rules.telegram.minimal_why_max_chars
    }
  ];

  for (const stage of stages) {
    const rendered = renderTelegramForStage(selectionResult, blocks, rules, runTimestamp, stage);
    if (rendered.content.length <= rules.telegram.length_budget_chars) {
      return {
        ...rendered,
        omitted_article_ids: [],
        length_budget_status: stage.name === 'standard' ? 'within_budget' : 'compacted_to_fit',
        compaction_stage: stage.name,
        final_length_chars: rendered.content.length
      };
    }
  }

  const minimalStage = stages[stages.length - 1];
  let includedIds = [...selectionResult.selected_article_ids];
  while (includedIds.length > 0) {
    const includedSet = new Set(includedIds);
    const reducedBlocks = blocks
      .map((block) => ({
        ...block,
        entry_article_ids: block.entry_article_ids.filter((id) => includedSet.has(id)),
        items: block.items.filter((item) => includedSet.has(item.article_id))
      }))
      .filter((block) => block.items.length > 0);

    const rendered = renderTelegramForStage(selectionResult, reducedBlocks, rules, runTimestamp, minimalStage);
    if (rendered.content.length <= rules.telegram.length_budget_chars) {
      return {
        ...rendered,
        omitted_article_ids: selectionResult.selected_article_ids.filter((id) => !includedSet.has(id)),
        length_budget_status: 'omitted_entries_to_fit',
        compaction_stage: minimalStage.name,
        final_length_chars: rendered.content.length
      };
    }

    includedIds = includedIds.slice(0, -1);
  }

  throw new Error('Telegram rendering failed to fit within the configured length budget');
}

function buildRenderingDiagnostics(selectionResult, blocks, rendered, rules) {
  const omittedBlockIds = rules.blocks.order.filter((blockId) => !blocks.some((block) => block.block_id === blockId));
  const warnings = [];

  if (selectionResult.degraded_mode) {
    warnings.push('degraded_run_rendered');
  } else if (selectionResult.under_default_target) {
    warnings.push('under_default_target_rendered');
  }

  if (rendered.telegram.omitted_article_ids.length > 0) {
    warnings.push('telegram_entries_omitted');
  } else if (rendered.telegram.length_budget_status === 'compacted_to_fit') {
    warnings.push('telegram_compacted_to_fit');
  }

  return {
    run_status: selectionResult.run_status,
    degraded_mode: selectionResult.degraded_mode,
    under_default_target: selectionResult.under_default_target,
    rendered_block_counts: Object.fromEntries(blocks.map((block) => [block.block_id, block.items.length])),
    rendered_block_order: blocks.map((block) => block.block_id),
    omitted_block_count: omittedBlockIds.length,
    omitted_block_ids: omittedBlockIds,
    per_format_item_counts: {
      email: rendered.email.entry_article_ids.length,
      telegram: rendered.telegram.entry_article_ids.length,
      markdown: rendered.markdown.entry_article_ids.length
    },
    truncation_counts: {
      telegram_summary_count: rendered.telegram.truncation.summary_count,
      telegram_why_count: rendered.telegram.truncation.why_count,
      telegram_total_count: rendered.telegram.truncation.total_count
    },
    telegram: {
      length_budget_chars: rules.telegram.length_budget_chars,
      final_length_chars: rendered.telegram.final_length_chars,
      length_budget_status: rendered.telegram.length_budget_status,
      compaction_stage: rendered.telegram.compaction_stage,
      omitted_article_ids: rendered.telegram.omitted_article_ids
    },
    omitted_entry_detail: {
      email: rendered.email.omitted_article_ids,
      telegram: rendered.telegram.omitted_article_ids,
      markdown: rendered.markdown.omitted_article_ids
    },
    rendering_warnings: warnings
  };
}

function writeRenderingArtifacts(outputDir, rendered) {
  mkdirSync(outputDir, { recursive: true });
  writeFileSync(resolve(outputDir, 'briefing_email.txt'), rendered.email.content);
  writeFileSync(resolve(outputDir, 'briefing_telegram.txt'), rendered.telegram.content);
  writeFileSync(resolve(outputDir, 'briefing_archive.md'), rendered.markdown.content);
  writeFileSync(resolve(outputDir, 'rendering_debug.json'), JSON.stringify(rendered.diagnostics, null, 2));
}

export function renderBriefing({
  selectionResult,
  outputDir = null,
  runTimestamp = null,
  renderingRulesPath = DEFAULT_RENDERING_RULES_PATH
}) {
  const rules = loadRenderingRules(renderingRulesPath);
  validateRenderingInput(selectionResult, rules);

  const effectiveRunTimestamp = runTimestamp ?? selectionResult.run_timestamp;
  if (!effectiveRunTimestamp) {
    throw new Error('Rendering requires a run_timestamp on the selected-set contract or an explicit runTimestamp argument');
  }

  const blocks = buildRenderingBlocks(selectionResult, rules);
  const email = renderEmail(selectionResult, blocks, rules, effectiveRunTimestamp);
  const telegram = renderTelegram(selectionResult, blocks, rules, effectiveRunTimestamp);
  const markdown = renderMarkdown(selectionResult, blocks, rules, effectiveRunTimestamp);
  const diagnostics = buildRenderingDiagnostics(selectionResult, blocks, {
    email,
    telegram,
    markdown
  }, rules);

  const rendered = {
    runTimestamp: effectiveRunTimestamp,
    blocks,
    email,
    telegram,
    markdown,
    diagnostics
  };

  validateRenderedBriefing(rendered, selectionResult);

  if (outputDir) {
    writeRenderingArtifacts(outputDir, rendered);
  }

  return rendered;
}
