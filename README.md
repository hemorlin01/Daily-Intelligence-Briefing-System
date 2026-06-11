# DIBS — Daily Intelligence Briefing System

> 从 80+ 源、JS pipeline 的 DIY 新闻推送系统，演进为基于 WorkBuddy Skill 的 27 源、6 领域、5 通道智能早报引擎。

## 项目演进

| 阶段 | 形态 | 说明 |
|------|------|------|
| Phase 1 | npm/JS 项目 | `src/` 下的多阶段 pipeline：标准化→去重→聚类→编辑选稿→渲染→交付 |
| Phase 2 | WorkBuddy Skill v1 | 15 源、5 领域、AI 替代代码 pipeline、4 路交付 |
| Phase 3 | Skill v2 + 独立脚本 | 27 源、6 领域、L0-L3 信源分级、GR_Brain 协同、5 路交付 |

## 当前架构

```
dibs-briefing Skill (~/.workbuddy/skills/dibs-briefing/)
├── SKILL.md                    ← 核心指令书 (27源, 6领域, 编辑规则)
├── scripts/
│   ├── standalone-briefing.mjs ← 独立脚本 (无需 WorkBuddy)
│   ├── send_email.mjs          ← SMTP 邮件
│   ├── send_telegram.mjs       ← Telegram Bot
│   ├── send_bark.mjs           ← Bark iOS 推送
│   ├── send_wechat.mjs         ← Server酱/微信
│   ├── send_wecom.mjs          ← 企业微信 Webhook
│   ├── run-briefing.ps1        ← Windows 调度包装器
│   └── register-task.ps1       ← 一键注册任务计划
├── references/
│   └── source_catalog.md       ← 完整源目录 + GR_Brain 对齐表
└── assets/
```

## 6 领域 × 27 信源

| 领域 | 源 | 配额 |
|------|-----|------|
| China & Geopolitics | BBC, CNA, SCMP, Caixin, 新华网, 澎湃, AP | 5 |
| Digital Regulation | EU DSA, SAMR, CAC, Tech Policy Press, Wire China, China Law Translate | 5 |
| Technology & Digital | Ars, Register, TC, Verge, Wired, 36氪, 晚点 | 5 |
| Climate & Urban | Inside Climate News, Carbon Brief | 4 |
| Global Economy | CNBC, Guardian, 财经 | 4 |
| Culture | Aeon | 2 |

## 交付通道

企业微信 Webhook → 个人微信 (Server酱) → Email (SMTP) → Telegram Bot → Bark (iOS)

## 运行方式

**方式 A: 独立脚本** (推荐日常使用)
```powershell
# 一次性: 注册 Windows 任务计划 (每日 08:00)
powershell -ExecutionPolicy Bypass -File "scripts/register-task.ps1"

# 手动运行
node scripts/standalone-briefing.mjs
```

**方式 B: WorkBuddy Skill** (AI 完整编辑)
```
在 WorkBuddy 对话中输入: 运行今早日报
```

## 双系统协同

```
DIBS (晨间哨兵)           GR_Brain (深度证据库)
      │                        │
  每日 08:00 扫描            按需启动深度研究
  27 源 → 识别信号           PUB → SRC → FACT → wiki
      │                        │
      └──────── 协同 ──────────┘
   DIBS 发现 → GR_Brain 深挖
```

## 原始代码

`src/` 和 `test/` 目录保留了原始 DIBS 的 npm 项目代码，作为历史参考。当前活跃的 Skill 代码位于 `~/.workbuddy/skills/dibs-briefing/`。
