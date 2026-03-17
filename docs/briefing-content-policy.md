# Briefing Content Policy

This document defines the user-facing content rules for the Daily Intelligence Briefing System (DIBS) output.
The goal is a concise, editorialized, single-user briefing with clear structure, consistent language handling,
and clean formatting.

## Title Fidelity
- Render the source article headline unchanged.
- Do not append the governed source name or domain tags to the headline.
- Render governed source and byline as separate metadata lines.
- The governed source always wins over byline or wire credit for the source label.

## Summary Policy
- Summaries must be complete, closed paragraphs.
- No ellipsis-style truncation or clipped fragments.
- Use full text when available; fall back to summary/snippet only when necessary.
- Full-text targets (English): 70–100 words.
- Summary-only targets (English): 32–60 words.
- Full-text targets (Chinese): 80–140 characters.
- Summary-only targets (Chinese): 40–90 characters.

## Why-It-Matters Policy
- English target length: 30–50 words.
- Chinese target length: 30–70 characters.
- Use concrete signals: primary entity, geography, topic label, strategic dimension, and event type.
- Avoid generic filler such as “could shape,” “reframes,” or empty template phrases.
- Why-it-matters must not repeat the factual summary.

## Language Strategy
- Headlines remain in the source article language.
- Summary and why-it-matters follow the source language when the article is Chinese.
- Summary and why-it-matters are English for English-language sources.
- When language is unclear, the system falls back to English and logs a warning.

## Sectioning Philosophy
- Prefer fewer, coherent sections when the briefing is small.
- The current rendering blocks map domains into five stable sections:
  - China & Geopolitics
  - Technology & Digital Economy
  - Climate, Energy & Urban Systems
  - Global Economy & Policy
  - Culture & Lifestyle

## Formatting Rules
- Email: headline first, source/byline second, summary and why-it-matters as readable paragraphs, and a clean “Read:” line.
- Telegram: compact, scannable blocks with headline, source/byline, short summary, short why-it-matters, and a read link.
- Markdown archive: preserve structure and audit metadata without keywords in the user-facing text.

## Quality Diagnostics
The rendering pipeline writes quality-oriented diagnostics:
- `content_quality_report.json`
- `final_briefing_language_report.json`
- `final_briefing_style_audit.json`

These reports track summary/why lengths, language distribution, title fidelity checks, and byline visibility.
