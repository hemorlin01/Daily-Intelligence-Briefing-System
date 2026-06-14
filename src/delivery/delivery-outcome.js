const FAILED_OUTCOMES = new Set(['failed', 'partial_success']);

export function deliveryOutcomeExitCode(outcome) {
  return FAILED_OUTCOMES.has(outcome) ? 1 : 0;
}
