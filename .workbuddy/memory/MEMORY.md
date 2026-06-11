# DIBS Briefing 项目记忆

## 项目概述
将 DIBS (Daily Intelligence Briefing System) 从 npm/JS 项目重构为 WorkBuddy Skill，并基于 GR_Brain 系统设计进行了 v2 升级。

## 已创建资产

### Skill: dibs-briefing (v2.0.0)
- 路径: `~/.workbuddy/skills/dibs-briefing/`
- SKILL.md: 27个精选源、6领域编辑规则（含Digital Regulation）、L0-L3信源分级、业务影响标签
- 交付脚本: send_email.mjs, send_telegram.mjs, send_bark.mjs, send_wechat.mjs
- 独立运行: standalone-briefing.mjs（无需WorkBuddy）
- Windows调度: run-briefing.ps1, register-task.ps1
- 参考文档: references/source_catalog.md（v2，含GR_Brain对齐表）
- Automation: 每日08:00

### v2 升级内容（基于 GR_Brain 分析）

1. **新增第6领域**: Digital Regulation & Platform Governance（6源，配额5篇）
   - L0官方源: EU DSA, SAMR, CAC, China Law Translate
   - L1分析源: Tech Policy Press, The Wire China

2. **中文源从0到6**: 新华网、澎湃、36氪、晚点、财经 + 官方监管源SAMR/CAC

3. **L0-L3信源分级**: 每篇Why-it-matters标注证据强度 + 业务影响标签[Temu]/[PDD主站]/[供应链]/[通用]

4. **GR_Brain协同**: DIBS做为晨间哨兵识别监管信号，GR_Brain负责深度证据链

### 关键设计决策
1. **AI模型替代JS pipeline**: 去重、摘要、编辑选稿全部由AI prompt约束
2. **源精选**: 从80+源缩减到27个，按6领域组织
3. **双语策略**: 中文源输出中文摘要，英文源输出英文摘要
4. **Why-it-matters**: 含主体实体、地域、主题、战略维度、证据强度、业务线
5. **5路交付通道**: Email + Telegram + Bark + WeChat(Server酱) + WeCom(企业微信Webhook) + 本地Markdown
6. **降级模式**: 少于10篇标记[LIGHT DAY]
7. **双模式**: WorkBuddy Skill（AI完整处理）+ standalone脚本（可选LLM API）

## 当前信源矩阵 (27源/6领域)

| 领域 | 源数 | 典型源 |
|------|------|--------|
| China & Geopolitics | 7 | BBC/CNA/SCMP/Caixin/新华网/澎湃/AP |
| Digital Regulation | 6 | EU DSA/SAMR/CAC/Tech Policy Press/Wire China/China Law Translate |
| Technology & Digital | 7 | Ars/Register/TC/Verge/Wired/36氪/晚点 |
| Climate & Urban | 2 | ICN/Carbon Brief |
| Global Economy | 4 | CNBC/Guardian/财经 |
| Culture | 1 | Aeon |
| **总计** | **27** | |
