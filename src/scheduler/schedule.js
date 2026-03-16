import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

function scheduleDayName(now, timezone) {
  return new Intl.DateTimeFormat('en-US', {
    weekday: 'short',
    timeZone: timezone
  }).format(now);
}

function scheduleHourMinute(now, timezone) {
  const parts = new Intl.DateTimeFormat('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: timezone
  }).formatToParts(now);

  return {
    hour: Number(parts.find((part) => part.type === 'hour')?.value ?? 0),
    minute: Number(parts.find((part) => part.type === 'minute')?.value ?? 0)
  };
}

export function evaluateSchedule(scheduleConfig, now = new Date()) {
  if (!scheduleConfig.enabled) {
    return {
      due: false,
      reason: 'schedule_disabled'
    };
  }

  const day = scheduleDayName(now, scheduleConfig.timezone);
  const { hour, minute } = scheduleHourMinute(now, scheduleConfig.timezone);
  if (!scheduleConfig.days.includes(day)) {
    return {
      due: false,
      reason: 'day_not_allowed',
      observed_day: day
    };
  }

  if (hour !== scheduleConfig.hour || minute !== scheduleConfig.minute) {
    return {
      due: false,
      reason: 'time_not_due',
      observed_day: day,
      observed_hour: hour,
      observed_minute: minute
    };
  }

  return {
    due: true,
    reason: 'schedule_due',
    observed_day: day,
    observed_hour: hour,
    observed_minute: minute
  };
}

export function writeSchedulerDebug(outputRoot, schedulerDebug) {
  const path = resolve(outputRoot, 'scheduler_debug.json');
  mkdirSync(resolve(outputRoot), { recursive: true });
  writeFileSync(path, JSON.stringify(schedulerDebug, null, 2));
  return path;
}
