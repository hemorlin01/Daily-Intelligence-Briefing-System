// Script to add/upgrade Chinese RSS sources in sources.json and feed-overrides.json
import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const SOURCES_PATH = resolve(process.cwd(), 'config', 'sources.json');
const FEED_OVERRIDES_PATH = resolve(process.cwd(), 'config', 'feed-overrides.json');

const sources = JSON.parse(readFileSync(SOURCES_PATH, 'utf8'));
const feeds = JSON.parse(readFileSync(FEED_OVERRIDES_PATH, 'utf8'));

// Get max numeric key
const maxKey = Math.max(...Object.keys(sources.sources).map(Number).filter(k => !isNaN(k)));
console.log(`Max source key: ${maxKey}`);

// Define Chinese RSS sources to add/upgrade
const zhSources = [
  // === UPGRADE existing: change fetch_method and add RSS ===
  { upgradeKey: '22', sourceId: 'thirtysixkr-daily', name: '36氪', 
    url: 'https://36kr.com/feed', format: 'rss', type: 'news', 
    topics: ['technology', 'digital_economy', 'china_economy'], tier: 2 },

  { upgradeKey: '118', sourceId: 'latepost', name: '晚点 LatePost',
    url: 'https://rss.latepost.com/', format: 'rss', type: 'analysis',
    topics: ['technology', 'china_economy', 'digital_economy'], tier: 2 },

  { upgradeKey: '119', sourceId: 'huxiu', name: '虎嗅',
    url: 'https://www.huxiu.com/rss/0.xml', format: 'rss', type: 'news',
    topics: ['technology', 'digital_economy', 'china_economy'], tier: 3 },

  { upgradeKey: '120', sourceId: 'the-paper', name: '澎湃新闻',
    url: 'https://www.thepaper.cn/rss_news.jsp', format: 'rss', type: 'news',
    topics: ['policy_analysis', 'china_economy', 'geopolitics'], tier: 2 },

  { upgradeKey: '121', sourceId: 'cls', name: '财联社',
    url: 'https://www.cls.cn/telegraph', format: 'rss', type: 'news',
    topics: ['china_economy', 'global_macro', 'technology'], tier: 2 },

  { upgradeKey: '116', sourceId: 'eeo', name: '经济观察报',
    url: 'https://www.eeo.com.cn/rss/feed.xml', format: 'rss', type: 'analysis',
    topics: ['china_economy', 'policy_analysis', 'global_macro'], tier: 2 },

  { upgradeKey: '117', sourceId: 'yicai', name: '第一财经',
    url: 'https://www.yicai.com/rss', format: 'rss', type: 'news',
    topics: ['china_economy', 'global_macro', 'technology'], tier: 2 },

  { upgradeKey: '122', sourceId: 'china-newsweek', name: '中国新闻周刊',
    url: 'https://www.inewsweek.cn/rss', format: 'rss', type: 'news',
    topics: ['policy_analysis', 'china_economy', 'culture_design'], tier: 3 },

  { upgradeKey: '60', sourceId: 'tmtpost-weekly', name: '钛媒体',
    url: 'https://www.tmtpost.com/rss.xml', format: 'rss', type: 'analysis',
    topics: ['technology', 'digital_economy', 'china_economy'], tier: 3 },

  // === NEW sources ===
  { new: true, sourceId: 'xinhua-politics', name: '新华网 · 时政',
    url: 'http://www.xinhuanet.com/politics/xhll.xml', format: 'rss', type: 'news',
    topics: ['policy_analysis', 'geopolitics', 'china_economy'], tier: 1 },

  { new: true, sourceId: 'people-daily', name: '人民网 · 时政',
    url: 'http://www.people.com.cn/rss/politics.xml', format: 'rss', type: 'news',
    topics: ['policy_analysis', 'china_economy', 'geopolitics'], tier: 1 },

  { new: true, sourceId: 'huanqiu', name: '环球网',
    url: 'https://www.huanqiu.com/rss', format: 'rss', type: 'news',
    topics: ['geopolitics', 'policy_analysis', 'global_macro'], tier: 2 },

  { new: true, sourceId: 'cankaoxiaoxi', name: '参考消息',
    url: 'https://www.cankaoxiaoxi.com/rss', format: 'rss', type: 'news',
    topics: ['geopolitics', 'policy_analysis', 'global_macro'], tier: 2 },

  { new: true, sourceId: 'ithome', name: 'IT之家',
    url: 'https://www.ithome.com/rss/', format: 'rss', type: 'news',
    topics: ['technology', 'digital_economy'], tier: 3 },

  { new: true, sourceId: 'geekpark', name: '极客公园',
    url: 'https://www.geekpark.net/rss', format: 'rss', type: 'analysis',
    topics: ['technology', 'digital_economy'], tier: 3 },

  { new: true, sourceId: 'pingwest', name: '品玩',
    url: 'https://www.pingwest.com/feed', format: 'rss', type: 'news',
    topics: ['technology', 'digital_economy', 'china_economy'], tier: 3 },

  { new: true, sourceId: 'jiemian', name: '界面新闻',
    url: 'https://a.jiemian.com/index.php?m=article&a=rss', format: 'rss', type: 'news',
    topics: ['china_economy', 'global_macro', 'technology'], tier: 2 },

  { new: true, sourceId: 'caijing-mag', name: '财经杂志',
    url: 'https://www.caijing.com.cn/rss', format: 'rss', type: 'analysis',
    topics: ['china_economy', 'policy_analysis', 'global_macro'], tier: 2 },

  { new: true, sourceId: 'china-energy', name: '中国能源报',
    url: 'https://paper.people.com.cn/zgnyb/rss', format: 'rss', type: 'news',
    topics: ['climate_transition', 'china_economy', 'policy_analysis'], tier: 2 },

  { new: true, sourceId: 'sci-tech-daily', name: '科技日报',
    url: 'http://www.stdaily.com/rss', format: 'rss', type: 'news',
    topics: ['technology', 'policy_analysis', 'china_economy'], tier: 2 },

  { new: true, sourceId: 'finance-sina', name: '新浪财经',
    url: 'https://finance.sina.com.cn/rss/finance.xml', format: 'rss', type: 'news',
    topics: ['china_economy', 'global_macro', 'technology'], tier: 3 },

  { new: true, sourceId: 'tencent-tech', name: '腾讯科技',
    url: 'https://new.qq.com/omn/rss/tech', format: 'rss', type: 'news',
    topics: ['technology', 'digital_economy'], tier: 3 },
];

// Process each source
let nextKey = maxKey + 1;
let added = 0;
let upgraded = 0;

for (const entry of zhSources) {
  if (entry.upgradeKey) {
    // Upgrade existing source
    const src = sources.sources[entry.upgradeKey];
    if (src) {
      src.fetch_method = 'rss';
      console.log(`  UPGRADE: ${entry.name} (key ${entry.upgradeKey})`);
      upgraded++;
    } else {
      console.log(`  WARN: upgrade key ${entry.upgradeKey} not found for ${entry.name}`);
    }
  } else if (entry.new) {
    // Add new source
    const key = String(nextKey);
    sources.sources[key] = {
      source_id: entry.sourceId,
      display_name: entry.name,
      source_class: 'china_policy_economy',
      language: 'zh',
      primary_region: 'china',
      default_topic_affinities: entry.topics,
      priority_tier: entry.tier,
      fetch_method: 'rss',
      parser_type: 'article',
      canonicalization_policy: 'standard',
      paywall_policy: 'free',
      expected_article_type: entry.type,
      reliability_status: 'trusted',
      active_status: 'active'
    };
    console.log(`  NEW: ${entry.name} (key ${key})`);
    nextKey++;
    added++;
  }
}

// Add feed overrides for all zh sources
for (const entry of zhSources) {
  const sourceId = entry.sourceId;
  if (!feeds.sources[sourceId]) {
    feeds.sources[sourceId] = {};
  }
  
  const feed = feeds.sources[sourceId];
  feed.feed_support_status = 'supported';
  feed.support_level = 'validated_public_feed';
  feed.ingestion_method = 'rss';
  feed.adapter_type = 'xml_feed';
  feed.notes = `${entry.name} Chinese RSS feed.`;
  feed.feed_definitions = [{
    feed_id: 'home',
    label: `${entry.name} Home`,
    url: entry.url,
    format: entry.format,
    adapter_type: 'xml_feed',
    content_mode: 'summary_only',
    expected_entry_type: entry.type,
    active_status: 'active'
  }];
}

// Save
writeFileSync(SOURCES_PATH, JSON.stringify(sources, null, 2));
writeFileSync(FEED_OVERRIDES_PATH, JSON.stringify(feeds, null, 2));

console.log(`\nDone: ${upgraded} upgraded, ${added} added. Total sources: ${Object.keys(sources.sources).length}`);
