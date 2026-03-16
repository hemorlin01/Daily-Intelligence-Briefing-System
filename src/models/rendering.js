const REQUIRED_RENDERING_RESULT_FIELDS = [
  'blocks',
  'email',
  'telegram',
  'markdown',
  'diagnostics'
];

const RUN_STATUS_VALUES = new Set([
  'on_target',
  'under_default_target',
  'degraded'
]);

const REQUIRED_RENDERING_ITEM_FIELDS = [
  'article_id',
  'source_id',
  'source_display_name',
  'title',
  'url',
  'candidate_keywords',
  'factual_summary',
  'why_it_matters',
  'primary_domain'
];

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

function validateSelectedRenderingItem(item, context) {
  for (const field of REQUIRED_RENDERING_ITEM_FIELDS) {
    if (!(field in item)) {
      throw new Error(`${context}: missing required selected item field "${field}"`);
    }
  }

  assertNonEmptyString(item.article_id, 'article_id', context);
  assertNonEmptyString(item.source_id, 'source_id', context);
  assertNonEmptyString(item.source_display_name, 'source_display_name', context);
  assertNonEmptyString(item.title, 'title', context);
  assertNonEmptyString(item.url, 'url', context);
  assertStringArray(item.candidate_keywords, 'candidate_keywords', context);
  assertNonEmptyString(item.factual_summary, 'factual_summary', context);
  assertNonEmptyString(item.why_it_matters, 'why_it_matters', context);
  assertNonEmptyString(item.primary_domain, 'primary_domain', context);
}

export function validateRenderingInput(selectionResult, renderingRules) {
  const context = 'Rendering input validation failed';

  if (!selectionResult || typeof selectionResult !== 'object' || Array.isArray(selectionResult)) {
    throw new Error(`${context}: selection result must be an object`);
  }

  for (const field of ['selected_items', 'selected_article_ids', 'selected_count', 'run_status', 'degraded_mode', 'under_default_target']) {
    if (!(field in selectionResult)) {
      throw new Error(`${context}: missing required field "${field}"`);
    }
  }

  if (!Array.isArray(selectionResult.selected_items)) {
    throw new Error(`${context}: "selected_items" must be an array`);
  }

  if (!Array.isArray(selectionResult.selected_article_ids)) {
    throw new Error(`${context}: "selected_article_ids" must be an array`);
  }

  if (selectionResult.selected_items.length !== selectionResult.selected_article_ids.length || selectionResult.selected_count !== selectionResult.selected_items.length) {
    throw new Error(`${context}: selected item counts are inconsistent`);
  }

  if (!RUN_STATUS_VALUES.has(selectionResult.run_status)) {
    throw new Error(`${context}: invalid run_status "${selectionResult.run_status}"`);
  }

  if (typeof selectionResult.degraded_mode !== 'boolean' || typeof selectionResult.under_default_target !== 'boolean') {
    throw new Error(`${context}: run-status flags must be booleans`);
  }

  for (const [index, item] of selectionResult.selected_items.entries()) {
    validateSelectedRenderingItem(item, `${context} for article ${item?.article_id ?? 'unknown'}`);
    if (selectionResult.selected_article_ids[index] !== item.article_id) {
      throw new Error(`${context}: "selected_article_ids" must match ordered "selected_items"`);
    }
    if (!(item.primary_domain in renderingRules.blocks.domain_to_block)) {
      throw new Error(`${context}: no rendering block mapping exists for primary_domain "${item.primary_domain}"`);
    }
  }

  return true;
}

function validateRenderedEntryIds(entryIds, selectedArticleIds, context, allowSubset = false) {
  if (!Array.isArray(entryIds)) {
    throw new Error(`${context}: entry ids must be an array`);
  }

  const selectedSet = new Set(selectedArticleIds);
  for (const id of entryIds) {
    if (!selectedSet.has(id)) {
      throw new Error(`${context}: rendered entry "${id}" is not in the selected set`);
    }
  }

  if (!allowSubset && entryIds.length !== selectedArticleIds.length) {
    throw new Error(`${context}: rendered entry count does not match selected item count`);
  }

  const expectedOrderedIds = allowSubset
    ? selectedArticleIds.filter((id) => entryIds.includes(id))
    : selectedArticleIds;

  if (JSON.stringify(entryIds) !== JSON.stringify(expectedOrderedIds)) {
    throw new Error(`${context}: rendered entry order does not match the selected-set order`);
  }
}

function validateBlockOrdering(blocks, selectedArticleIds, context) {
  const selectedIndex = new Map(selectedArticleIds.map((id, index) => [id, index]));
  const flattenedIds = [];

  for (const block of blocks) {
    for (const articleId of block.entry_article_ids) {
      flattenedIds.push(articleId);
    }
    for (let index = 1; index < block.entry_article_ids.length; index += 1) {
      const previousIndex = selectedIndex.get(block.entry_article_ids[index - 1]);
      const currentIndex = selectedIndex.get(block.entry_article_ids[index]);
      if (previousIndex > currentIndex) {
        throw new Error(`${context}: block "${block.block_id}" does not preserve selected-item order`);
      }
    }
  }

  if (flattenedIds.length !== selectedArticleIds.length) {
    throw new Error(`${context}: block membership does not cover the full selected set`);
  }

  if (new Set(flattenedIds).size !== flattenedIds.length) {
    throw new Error(`${context}: duplicate article ids appear across rendering blocks`);
  }

  const sortedFlattenedIds = [...flattenedIds].sort();
  const sortedSelectedIds = [...selectedArticleIds].sort();
  if (JSON.stringify(sortedFlattenedIds) !== JSON.stringify(sortedSelectedIds)) {
    throw new Error(`${context}: rendering blocks do not map back to the selected article ids`);
  }

  return flattenedIds;
}

export function validateRenderedBriefing(rendered, selectionResult) {
  const context = 'Rendered briefing validation failed';
  if (!rendered || typeof rendered !== 'object' || Array.isArray(rendered)) {
    throw new Error(`${context}: rendered result must be an object`);
  }

  for (const field of REQUIRED_RENDERING_RESULT_FIELDS) {
    if (!(field in rendered)) {
      throw new Error(`${context}: missing required field "${field}"`);
    }
  }

  if (!Array.isArray(rendered.blocks)) {
    throw new Error(`${context}: "blocks" must be an array`);
  }

  const flattenedBlockEntryIds = validateBlockOrdering(rendered.blocks, selectionResult.selected_article_ids, context);
  validateRenderedEntryIds(rendered.email.entry_article_ids, flattenedBlockEntryIds, `${context} for email`);
  validateRenderedEntryIds(rendered.markdown.entry_article_ids, flattenedBlockEntryIds, `${context} for markdown`);
  validateRenderedEntryIds(rendered.telegram.entry_article_ids, flattenedBlockEntryIds, `${context} for telegram`, true);

  if (rendered.email.omitted_article_ids.length > 0) {
    throw new Error(`${context}: email must not omit selected items`);
  }

  if (rendered.markdown.omitted_article_ids.length > 0) {
    throw new Error(`${context}: markdown must not omit selected items`);
  }

  if (!rendered.diagnostics || typeof rendered.diagnostics !== 'object' || Array.isArray(rendered.diagnostics)) {
    throw new Error(`${context}: diagnostics must be an object`);
  }

  if (rendered.telegram.omitted_article_ids.length > 0) {
    const omittedIds = [...rendered.telegram.omitted_article_ids].sort();
    const diagnosticOmittedIds = [...(rendered.diagnostics.telegram.omitted_article_ids ?? [])].sort();
    if (JSON.stringify(omittedIds) !== JSON.stringify(diagnosticOmittedIds)) {
      throw new Error(`${context}: telegram omission diagnostics do not match the rendered output`);
    }
  }

  return true;
}
