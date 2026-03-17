# Briefing Run Status

This document defines the operational run-status semantics used by the DIBS briefing pipeline.

## Status Fields

The editorial selection layer emits a stable status contract used by rendering, delivery, and diagnostics.

- `run_status`
  - `on_target`: selected count is at or above the configured default target.
  - `under_default_target`: selected count is below default target but at or above the configured minimum target.
  - `degraded`: selected count is below the configured minimum target.

- `degraded_mode`
  - `true` only when `run_status = degraded`.
  - `false` otherwise.

- `under_default_target`
  - `true` only when `run_status = under_default_target`.
  - `false` otherwise.

## How Status Is Computed

Status is computed from editorial selection counts:

1. If `selected_count < minimum_target_count` → `run_status = degraded`.
2. Else if `selected_count < default_target_count` → `run_status = under_default_target`.
3. Else → `run_status = on_target`.

This means a run is not degraded merely for being under the default target; it is only degraded when it falls below the minimum target.

## Degraded Reasons

`briefing_status_report.json` includes `degraded_reasons` only when a run is truly degraded. The reasons are derived from the pipeline funnel and help diagnose whether the issue was:

- empty main candidate pool
- empty semantic pool
- selected count below minimum target
- excessive stale-item rejections
- insufficient content-signal rejections

These diagnostics are intended for operational auditing and should be surfaced in workflow logs for daily review.
