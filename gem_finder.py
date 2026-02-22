"""
Gem Finder - Market Intelligence Radar
Scans Reddit, Chinese social media (Weibo), forums, and other platforms
for early-stage, potentially market-moving information about public companies.

Targets: product leaks, unreleased features, takeover chatter, insider hints,
supply chain signals, regulatory whispers, and other "gem" intelligence.

Runs once daily. Sends findings to Discord webhook.
"""

import os
import re
import json
import time
import logging
import hashlib
import sqlite3
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote, unquote

# ─── Configuration ───────────────────────────────────────────────────────────

DISCORD_WEBHOOK = os.getenv(
    "DISCORD_WEBHOOK",
    "https://discordapp.com/api/webhooks/1474952931051831428/U3NpVQpkD1CsyjUztm30apV7MCgWP3Z9n1dWJKxdlFCBeyih8DG0XeSqZRZPb_3L25Yl"
)

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-1cf5b2ab46a14eb6978ff7ba7ce3f3e3")
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"

DB_PATH = os.getenv("DB_PATH", "gem_finder.db")
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))
MIN_GEM_SCORE = int(os.getenv("MIN_GEM_SCORE", "7"))  # 1-10 scale, only send 7+

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gem_finder")

# ─── Major Public Company Tickers & Names ────────────────────────────────────
# Focused list of high-interest companies where "gems" would be most impactful.
# Expand as needed.

COMPANY_MAP = {
    # Mega-cap Tech
    "AAPL": ["apple", "iphone", "ipad", "macbook", "apple vision", "apple car"],
    "MSFT": ["microsoft", "windows", "azure", "xbox", "copilot", "bing"],
    "GOOGL": ["google", "alphabet", "youtube", "waymo", "deepmind", "gemini ai", "pixel"],
    "AMZN": ["amazon", "aws", "alexa", "prime video", "whole foods", "amazon go"],
    "META": ["meta", "facebook", "instagram", "whatsapp", "threads", "quest", "metaverse"],
    "NVDA": ["nvidia", "geforce", "rtx", "cuda", "jensen huang", "blackwell", "dgx"],
    "TSLA": ["tesla", "elon musk", "cybertruck", "model s", "model 3", "model y", "autopilot", "fsd"],
    "TSM": ["tsmc", "taiwan semiconductor"],
    "AVGO": ["broadcom", "vmware"],
    "ORCL": ["oracle", "larry ellison"],
    # AI / Semiconductor
    "AMD": ["amd", "radeon", "ryzen", "epyc", "lisa su", "xilinx"],
    "INTC": ["intel", "pat gelsinger", "intel foundry"],
    "ARM": ["arm holdings", "arm chips"],
    "SMCI": ["supermicro", "super micro"],
    "MRVL": ["marvell technology"],
    "MU": ["micron", "micron technology"],
    "QCOM": ["qualcomm", "snapdragon"],
    # Software / Cloud
    "CRM": ["salesforce", "marc benioff", "slack"],
    "SNOW": ["snowflake computing", "snowflake data"],
    "PLTR": ["palantir"],
    "AI": ["c3.ai", "c3 ai"],
    "PATH": ["uipath"],
    "NET": ["cloudflare"],
    "DDOG": ["datadog"],
    "ZS": ["zscaler"],
    "CRWD": ["crowdstrike"],
    "PANW": ["palo alto networks"],
    # Consumer / Retail
    "WMT": ["walmart"],
    "COST": ["costco"],
    "NKE": ["nike"],
    "SBUX": ["starbucks"],
    "MCD": ["mcdonald"],
    "DIS": ["disney", "disney+", "espn"],
    "NFLX": ["netflix"],
    "SPOT": ["spotify"],
    # Pharma / Biotech
    "LLY": ["eli lilly", "lilly", "mounjaro", "zepbound"],
    "NVO": ["novo nordisk", "ozempic", "wegovy"],
    "PFE": ["pfizer"],
    "MRNA": ["moderna"],
    "ABBV": ["abbvie"],
    "JNJ": ["johnson & johnson", "j&j"],
    "BMY": ["bristol-myers", "bristol myers"],
    "GILD": ["gilead"],
    "AMGN": ["amgen"],
    "REGN": ["regeneron"],
    # Financial
    "JPM": ["jpmorgan", "jp morgan", "jamie dimon"],
    "GS": ["goldman sachs"],
    "MS": ["morgan stanley"],
    "BAC": ["bank of america"],
    "V": ["visa"],
    "MA": ["mastercard"],
    # EV / Auto
    "RIVN": ["rivian"],
    "LCID": ["lucid motors", "lucid group"],
    "F": ["ford motor"],
    "GM": ["general motors"],
    "TM": ["toyota"],
    # Energy / Industrial
    "XOM": ["exxon", "exxonmobil"],
    "CVX": ["chevron"],
    "LMT": ["lockheed martin"],
    "BA": ["boeing"],
    "RTX": ["raytheon"],
    "GE": ["general electric"],
    # Chinese ADRs (high interest for Weibo cross-referencing)
    "BABA": ["alibaba", "阿里巴巴", "阿里", "淘宝", "天猫"],
    "PDD": ["pinduoduo", "拼多多", "temu"],
    "JD": ["jd.com", "京东"],
    "BIDU": ["baidu", "百度"],
    "NIO": ["nio", "蔚来"],
    "XPEV": ["xpeng", "小鹏"],
    "LI": ["li auto", "理想汽车", "理想"],
    "BILI": ["bilibili", "b站", "哔哩哔哩"],
    "TME": ["tencent music", "腾讯音乐"],
    "NTES": ["netease", "网易"],
    "TCOM": ["trip.com", "携程"],
    # Crypto-adjacent
    "MSTR": ["microstrategy", "michael saylor"],
    "COIN": ["coinbase"],
    # Meme / High-Retail-Interest
    "GME": ["gamestop"],
    "AMC": ["amc entertainment", "amc theatres"],
    "BBBY": ["bed bath"],
    "PLTR": ["palantir"],
    "SOFI": ["sofi"],
    "HOOD": ["robinhood"],
}

# Flatten for fast lookup
KEYWORD_TO_TICKER = {}
for ticker, keywords in COMPANY_MAP.items():
    for kw in keywords:
        KEYWORD_TO_TICKER[kw.lower()] = ticker
    KEYWORD_TO_TICKER[ticker.lower()] = ticker
    KEYWORD_TO_TICKER[f"${ticker.lower()}"] = ticker

# ─── Chinese Language Utilities ───────────────────────────────────────────────

# CJK Unicode ranges for detecting Chinese text
CJK_RANGES = [
    (0x4E00, 0x9FFF),    # CJK Unified Ideographs
    (0x3400, 0x4DBF),    # CJK Extension A
    (0xF900, 0xFAFF),    # CJK Compatibility Ideographs
    (0x2F800, 0x2FA1F),  # CJK Compatibility Supplement
]


def is_chinese_char(ch: str) -> bool:
    cp = ord(ch)
    return any(start <= cp <= end for start, end in CJK_RANGES)


def chinese_char_ratio(text: str) -> float:
    """Return the ratio of Chinese characters in the text."""
    if not text:
        return 0.0
    chinese_count = sum(1 for ch in text if is_chinese_char(ch))
    return chinese_count / len(text)


def detect_language(text: str) -> str:
    """Detect if text is primarily Chinese or English."""
    ratio = chinese_char_ratio(text)
    if ratio > 0.1:  # More than 10% CJK characters → treat as Chinese
        return "zh"
    return "en"


def translate_chinese_batch(texts: list[dict]) -> list[dict]:
    """
    Translate a batch of Chinese posts to English using DeepSeek.
    Each item should have 'title' and 'body' keys.
    Returns the same list with 'title_en', 'body_en', and 'original_language' added.
    """
    if not texts:
        return texts

    # Build batch translation request
    to_translate = []
    for i, item in enumerate(texts):
        combined = f"TITLE: {item['title'][:200]}\nBODY: {item['body'][:800]}"
        to_translate.append(f"--- TEXT {i + 1} ---\n{combined}")

    prompt = f"""Translate the following {len(to_translate)} Chinese social media posts into natural English.
For each post, provide an accurate English translation of both the title and body.
Preserve financial terminology, company names, ticker symbols, and any numbers exactly.
If a post is already in English or mixed, still provide a clean English version.

Respond ONLY in this JSON format:
{{
  "translations": [
    {{
      "post_number": 1,
      "title_en": "English translation of title",
      "body_en": "English translation of body"
    }}
  ]
}}

POSTS TO TRANSLATE:
{chr(10).join(to_translate)}"""

    try:
        resp = requests.post(
            DEEPSEEK_API_URL,
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "You are a professional Chinese-to-English translator specializing in financial and technology content. Respond only in valid JSON."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
                "max_tokens": 4000,
            },
            timeout=60,
        )

        if resp.status_code != 200:
            log.error(f"DeepSeek translation API error: {resp.status_code}")
            # Fallback: mark as untranslated
            for item in texts:
                item["title_en"] = item["title"]
                item["body_en"] = item["body"]
                item["original_language"] = "zh"
                item["translation_failed"] = True
            return texts

        result = resp.json()
        ai_text = result["choices"][0]["message"]["content"]
        json_match = re.search(r"\{[\s\S]*\}", ai_text)
        if json_match:
            translations = json.loads(json_match.group())
            for t in translations.get("translations", []):
                idx = t.get("post_number", 1) - 1
                if 0 <= idx < len(texts):
                    texts[idx]["title_en"] = t.get("title_en", texts[idx]["title"])
                    texts[idx]["body_en"] = t.get("body_en", texts[idx]["body"])
                    texts[idx]["original_language"] = "zh"
                    texts[idx]["original_title"] = texts[idx]["title"]
                    texts[idx]["original_body"] = texts[idx]["body"]
        else:
            log.warning("Could not parse translation response")
            for item in texts:
                item["title_en"] = item["title"]
                item["body_en"] = item["body"]
                item["original_language"] = "zh"

    except Exception as e:
        log.error(f"Translation error: {e}")
        for item in texts:
            item["title_en"] = item["title"]
            item["body_en"] = item["body"]
            item["original_language"] = "zh"

    return texts


# ─── Gem-worthy topic signals ───────────────────────────────────────────────

GEM_SIGNALS = [
    # Product leaks & launches
    "leak", "leaked", "prototype", "unreleased", "upcoming", "sneak peek",
    "hands on", "first look", "early access", "beta test", "teardown",
    "fcc filing", "certification", "benchmarks leaked",
    # M&A / Takeover
    "acquisition", "acquire", "takeover", "merger", "buyout", "bid for",
    "in talks to buy", "deal", "private equity", "going private",
    # Insider / Executive signals
    "insider", "executive", "ceo", "cfo", "stepping down", "fired",
    "hired from", "poached", "restructuring", "layoffs", "mass layoff",
    # Supply chain / Production
    "supply chain", "factory", "production", "yield", "shortage",
    "ramping up", "mass production", "supplier", "foxconn", "assembly line",
    # Regulatory / Legal
    "fda approval", "fda reject", "patent", "lawsuit", "antitrust",
    "investigation", "sec probe", "subpoena", "settlement", "banned",
    # Financial signals
    "revenue", "earnings", "guidance", "beat estimates", "miss estimates",
    "downgrade", "upgrade", "price target", "short squeeze", "short interest",
    "insider buying", "insider selling", "13f", "activist investor",
    # China-specific signals
    "中国市场", "收购", "合并", "泄露", "新品", "发布会",
    "监管", "罚款", "调查", "裁员", "量产",
]


# ─── Database for deduplication ──────────────────────────────────────────────

def init_db():
    """Initialize SQLite database for tracking seen posts."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_posts (
            content_hash TEXT PRIMARY KEY,
            source TEXT,
            ticker TEXT,
            title TEXT,
            score REAL,
            found_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS run_log (
            run_id TEXT PRIMARY KEY,
            started_at TEXT,
            finished_at TEXT,
            gems_found INTEGER,
            posts_scanned INTEGER
        )
    """)
    conn.commit()
    return conn


def is_seen(conn, content_hash: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM seen_posts WHERE content_hash = ?", (content_hash,)
    ).fetchone()
    return row is not None


def mark_seen(conn, content_hash: str, source: str, ticker: str, title: str, score: float):
    conn.execute(
        "INSERT OR IGNORE INTO seen_posts VALUES (?, ?, ?, ?, ?, ?)",
        (content_hash, source, ticker, title, score, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


# ─── Source: Reddit ──────────────────────────────────────────────────────────

REDDIT_SUBREDDITS = [
    # Financial / Stock-focused
    "wallstreetbets", "stocks", "investing", "stockmarket", "options",
    "SecurityAnalysis", "valueinvesting", "smallstreetbets",
    "pennystocks", "spacs", "Biotechplays",
    # Tech leaks & news
    "technology", "Apple", "android", "hardware", "gadgets",
    "GalaxyS", "iphone", "nvidia", "Amd", "intel",
    # Industry-specific
    "biotech", "pharma", "electricvehicles", "teslamotors",
    "semiconductor", "cybersecurity",
    # China / Asia markets
    "China", "ChinaTech", "asianmarkets",
    # General news that catches early
    "news", "worldnews", "business",
]

REDDIT_HEADERS = {
    "User-Agent": "GemFinder/1.0 (Market Intelligence Research Bot)"
}


def fetch_reddit_posts(subreddit: str, sort: str = "new", limit: int = 50) -> list[dict]:
    """Fetch recent posts from a subreddit via Reddit's public JSON API."""
    url = f"https://old.reddit.com/r/{subreddit}/{sort}.json?limit={limit}"
    try:
        resp = requests.get(url, headers=REDDIT_HEADERS, timeout=15)
        if resp.status_code == 429:
            log.warning(f"Reddit rate limited on r/{subreddit}, sleeping 60s...")
            time.sleep(60)
            resp = requests.get(url, headers=REDDIT_HEADERS, timeout=15)
        if resp.status_code != 200:
            log.warning(f"Reddit r/{subreddit} returned {resp.status_code}")
            return []
        data = resp.json()
        posts = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
        for child in data.get("data", {}).get("children", []):
            post = child.get("data", {})
            created = datetime.fromtimestamp(post.get("created_utc", 0), tz=timezone.utc)
            if created < cutoff:
                continue
            posts.append({
                "source": f"reddit/r/{subreddit}",
                "title": post.get("title", ""),
                "body": (post.get("selftext", "") or "")[:2000],
                "url": f"https://reddit.com{post.get('permalink', '')}",
                "score": post.get("score", 0),
                "num_comments": post.get("num_comments", 0),
                "created": created.isoformat(),
                "author": post.get("author", ""),
                "language": "en",
            })
        return posts
    except Exception as e:
        log.error(f"Error fetching r/{subreddit}: {e}")
        return []


def scan_reddit() -> list[dict]:
    """Scan all target subreddits for potential gems."""
    all_posts = []
    for sub in REDDIT_SUBREDDITS:
        log.info(f"  Scanning r/{sub}...")
        posts = fetch_reddit_posts(sub, sort="new", limit=50)
        # Also grab "hot" for trending posts
        posts += fetch_reddit_posts(sub, sort="hot", limit=25)
        all_posts.extend(posts)
        time.sleep(2)  # Be polite to Reddit
    log.info(f"  Reddit: collected {len(all_posts)} posts from {len(REDDIT_SUBREDDITS)} subreddits")
    return all_posts


# ─── Source: Weibo (Chinese Social Media) ────────────────────────────────────

WEIBO_SEARCH_TERMS = [
    # Major US tech companies (Chinese names)
    "苹果 新品", "苹果 泄露", "苹果 发布会", "苹果 供应链",
    "特斯拉", "特斯拉 工厂", "特斯拉 降价", "特斯拉 召回",
    "英伟达", "英伟达 芯片", "英伟达 供货",
    "微软 裁员", "微软 收购", "谷歌 裁员", "谷歌 新品",
    "亚马逊 裁员", "亚马逊 AWS",
    # Chinese ADRs (very high value for cross-referencing)
    "阿里巴巴 收购", "阿里巴巴 裁员", "阿里巴巴 拆分", "阿里 组织调整",
    "拼多多 业绩", "拼多多 商家", "Temu 封号",
    "京东 裁员", "京东 收购", "京东 物流",
    "百度 自动驾驶", "百度 AI", "百度 裁员",
    "蔚来汽车 交付", "蔚来 裁员", "蔚来 新车",
    "小鹏汽车 交付", "小鹏 新车", "小鹏 自动驾驶",
    "理想汽车 交付", "理想 新车", "理想 销量",
    "比亚迪 出口", "比亚迪 新车", "比亚迪 电池",
    "腾讯 游戏", "腾讯 投资", "腾讯 裁员",
    "字节跳动 上市", "字节跳动 TikTok", "抖音 电商",
    "网易 游戏", "网易 裁员",
    "哔哩哔哩 财报", "B站 裁员",
    # Semiconductor / Supply chain (huge for NVDA, AMD, INTC, TSM)
    "芯片 量产", "台积电 产能", "台积电 良率", "中芯国际",
    "芯片 禁令", "芯片 出口管制", "半导体 突破",
    "光刻机", "ASML", "华为 芯片", "华为 手机",
    # Industry signals
    "新能源 补贴", "新能源 政策", "电动车 补贴",
    "FDA 批准", "药品 获批", "临床试验",
    "收购 传闻", "并购", "合并 传闻",
    "IPO 消息", "退市", "私有化",
    # Broad market-moving signals
    "内幕消息", "重大消息", "利好", "利空",
    "工厂 爆炸", "工厂 停产", "供应链 中断",
    "监管 处罚", "罚款", "反垄断 调查",
]

# Shared headers for Chinese platform requests
CN_MOBILE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
}


def fetch_weibo_search(query: str) -> list[dict]:
    """
    Search Weibo for posts matching query.
    Uses Weibo's mobile search endpoint (no auth required for basic search).
    """
    url = f"https://m.weibo.cn/api/container/getIndex"
    params = {
        "containerid": f"100103type=1&q={quote(query)}",
        "page_type": "searchall",
        "page": 1,
    }
    try:
        resp = requests.get(url, params=params, timeout=15, headers=CN_MOBILE_HEADERS)
        if resp.status_code != 200:
            log.warning(f"Weibo search for '{query}' returned {resp.status_code}")
            return []
        data = resp.json()
        posts = []
        cards = data.get("data", {}).get("cards", [])
        cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
        for card in cards:
            if card.get("card_type") != 9:
                continue
            mblog = card.get("mblog", {})
            # Parse created time
            created_str = mblog.get("created_at", "")
            text = re.sub(r"<[^>]+>", "", mblog.get("text", ""))  # Strip HTML
            post_url = f"https://m.weibo.cn/detail/{mblog.get('id', '')}"

            lang = detect_language(text)
            posts.append({
                "source": "weibo",
                "title": text[:100],
                "body": text[:2000],
                "url": post_url,
                "score": mblog.get("attitudes_count", 0),
                "num_comments": mblog.get("comments_count", 0),
                "reposts": mblog.get("reposts_count", 0),
                "created": created_str,
                "author": mblog.get("user", {}).get("screen_name", "unknown"),
                "language": lang,
            })
        return posts
    except Exception as e:
        log.error(f"Error fetching Weibo for '{query}': {e}")
        return []


def scan_weibo() -> list[dict]:
    """Scan Weibo for potential gems about public companies."""
    all_posts = []
    for term in WEIBO_SEARCH_TERMS:
        log.info(f"  Scanning Weibo: '{term}'...")
        posts = fetch_weibo_search(term)
        all_posts.extend(posts)
        time.sleep(3)
    log.info(f"  Weibo: collected {len(all_posts)} posts")
    return all_posts


# ─── Source: Baidu Tieba (Chinese Forums) ────────────────────────────────────

TIEBA_FORUMS = [
    # Stock/Finance forums
    "股票", "炒股", "基金",
    # Tech forums
    "苹果", "特斯拉", "英伟达", "华为", "半导体",
    # EV forums
    "新能源汽车", "蔚来", "小鹏", "理想汽车", "比亚迪",
]


def fetch_baidu_tieba(forum_name: str) -> list[dict]:
    """Fetch recent posts from a Baidu Tieba forum."""
    url = f"https://tieba.baidu.com/mo/q/m?kw={quote(forum_name)}&pn=0&is_good=0"
    try:
        resp = requests.get(url, timeout=15, headers=CN_MOBILE_HEADERS)
        if resp.status_code != 200:
            return []
        data = resp.json()
        posts = []
        thread_list = data.get("data", {}).get("thread_list", [])
        for thread in thread_list[:30]:
            title = thread.get("title", "")
            abstract = ""
            for ab in thread.get("abstract", []):
                if ab.get("type") == 0:
                    abstract += ab.get("text", "")
            text = f"{title} {abstract}"
            lang = detect_language(text)
            posts.append({
                "source": f"tieba/{forum_name}",
                "title": title[:200],
                "body": abstract[:2000],
                "url": f"https://tieba.baidu.com/p/{thread.get('tid', '')}",
                "score": int(thread.get("reply_num", 0)),
                "num_comments": int(thread.get("reply_num", 0)),
                "created": datetime.now(timezone.utc).isoformat(),
                "author": thread.get("author", {}).get("name_show", "unknown"),
                "language": lang,
            })
        return posts
    except Exception as e:
        log.error(f"Error fetching Tieba '{forum_name}': {e}")
        return []


def scan_baidu_tieba() -> list[dict]:
    """Scan Baidu Tieba forums for potential gems."""
    all_posts = []
    for forum in TIEBA_FORUMS:
        log.info(f"  Scanning Tieba: '{forum}'...")
        posts = fetch_baidu_tieba(forum)
        all_posts.extend(posts)
        time.sleep(2)
    log.info(f"  Baidu Tieba: collected {len(all_posts)} posts")
    return all_posts


# ─── Source: Zhihu (Chinese Q&A / Quora equivalent) ─────────────────────────

ZHIHU_SEARCH_TERMS = [
    "收购 上市公司", "芯片 突破", "新品 泄露",
    "裁员 大厂", "自动驾驶 进展", "FDA 审批",
    "苹果 新品", "特斯拉 中国", "英伟达 AI",
    "阿里 改革", "拼多多 海外", "比亚迪 出海",
]


def fetch_zhihu_search(query: str) -> list[dict]:
    """Search Zhihu for relevant discussions."""
    url = "https://www.zhihu.com/api/v4/search_v3"
    params = {
        "q": query,
        "t": "general",
        "offset": 0,
        "limit": 20,
    }
    try:
        resp = requests.get(url, params=params, timeout=15, headers={
            **CN_MOBILE_HEADERS,
            "Referer": "https://www.zhihu.com/",
        })
        if resp.status_code != 200:
            return []
        data = resp.json()
        posts = []
        for item in data.get("data", []):
            obj = item.get("object", {})
            if not obj:
                continue
            title = obj.get("title", "") or obj.get("question", {}).get("title", "")
            excerpt = obj.get("excerpt", "") or obj.get("content", "")
            # Strip HTML from content
            excerpt = re.sub(r"<[^>]+>", "", excerpt)
            text = f"{title} {excerpt}"
            lang = detect_language(text)
            obj_type = item.get("type", "")
            obj_id = obj.get("id", "")
            if obj_type == "answer":
                url_link = f"https://www.zhihu.com/answer/{obj_id}"
            elif obj_type == "article":
                url_link = f"https://zhuanlan.zhihu.com/p/{obj_id}"
            else:
                url_link = f"https://www.zhihu.com/question/{obj_id}"
            posts.append({
                "source": "zhihu",
                "title": title[:200],
                "body": excerpt[:2000],
                "url": url_link,
                "score": obj.get("voteup_count", 0),
                "num_comments": obj.get("comment_count", 0),
                "created": datetime.now(timezone.utc).isoformat(),
                "author": obj.get("author", {}).get("name", "unknown"),
                "language": lang,
            })
        return posts
    except Exception as e:
        log.error(f"Error fetching Zhihu '{query}': {e}")
        return []


def scan_zhihu() -> list[dict]:
    """Scan Zhihu for potential gems."""
    all_posts = []
    for term in ZHIHU_SEARCH_TERMS:
        log.info(f"  Scanning Zhihu: '{term}'...")
        posts = fetch_zhihu_search(term)
        all_posts.extend(posts)
        time.sleep(3)
    log.info(f"  Zhihu: collected {len(all_posts)} posts")
    return all_posts


# ─── Source: Xiaohongshu / RedNote (Chinese lifestyle + increasingly biz) ────

XHS_SEARCH_TERMS = [
    "苹果新品泄露", "iPhone泄露", "特斯拉降价",
    "工厂内部", "新品提前看", "供应链消息",
    "华为新机", "芯片突破",
]


def fetch_xiaohongshu_search(query: str) -> list[dict]:
    """
    Search Xiaohongshu (RedNote) for posts.
    Uses the web search endpoint - limited but useful for catching viral leaks.
    """
    url = "https://edith.xiaohongshu.com/api/sns/web/v1/search/notes"
    try:
        resp = requests.post(url, timeout=15, headers={
            **CN_MOBILE_HEADERS,
            "Referer": "https://www.xiaohongshu.com/",
            "Origin": "https://www.xiaohongshu.com",
        }, json={
            "keyword": query,
            "page": 1,
            "page_size": 20,
            "search_id": hashlib.md5(query.encode()).hexdigest(),
            "sort": "time_descending",
            "note_type": 0,
        })
        if resp.status_code != 200:
            return []
        data = resp.json()
        posts = []
        for item in data.get("data", {}).get("items", []):
            note = item.get("note_card", {})
            title = note.get("display_title", "")
            desc = note.get("desc", "")
            text = f"{title} {desc}"
            lang = detect_language(text)
            note_id = item.get("id", "")
            posts.append({
                "source": "xiaohongshu",
                "title": title[:200],
                "body": desc[:2000],
                "url": f"https://www.xiaohongshu.com/explore/{note_id}",
                "score": note.get("liked_count", 0) or 0,
                "num_comments": note.get("comment_count", 0) or 0,
                "created": datetime.now(timezone.utc).isoformat(),
                "author": note.get("user", {}).get("nickname", "unknown"),
                "language": lang,
            })
        return posts
    except Exception as e:
        log.error(f"Error fetching Xiaohongshu '{query}': {e}")
        return []


def scan_xiaohongshu() -> list[dict]:
    """Scan Xiaohongshu for potential gems (product leaks, factory posts)."""
    all_posts = []
    for term in XHS_SEARCH_TERMS:
        log.info(f"  Scanning Xiaohongshu: '{term}'...")
        posts = fetch_xiaohongshu_search(term)
        all_posts.extend(posts)
        time.sleep(3)
    log.info(f"  Xiaohongshu: collected {len(all_posts)} posts")
    return all_posts


# ─── Source: Hacker News ─────────────────────────────────────────────────────

def scan_hackernews() -> list[dict]:
    """Scan Hacker News top and new stories for company-related gems."""
    posts = []
    try:
        # Get top stories and new stories
        for endpoint in ["topstories", "newstories"]:
            resp = requests.get(
                f"https://hacker-news.firebaseio.com/v0/{endpoint}.json",
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            story_ids = resp.json()[:100]  # Top 100
            cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

            for sid in story_ids:
                try:
                    sr = requests.get(
                        f"https://hacker-news.firebaseio.com/v0/item/{sid}.json",
                        timeout=10,
                    )
                    if sr.status_code != 200:
                        continue
                    story = sr.json()
                    if not story or story.get("type") != "story":
                        continue
                    created = datetime.fromtimestamp(
                        story.get("time", 0), tz=timezone.utc
                    )
                    if created < cutoff:
                        continue
                    posts.append({
                        "source": "hackernews",
                        "title": story.get("title", ""),
                        "body": story.get("text", "") or "",
                        "url": story.get("url", f"https://news.ycombinator.com/item?id={sid}"),
                        "score": story.get("score", 0),
                        "num_comments": story.get("descendants", 0),
                        "created": created.isoformat(),
                        "author": story.get("by", ""),
                        "language": "en",
                    })
                except Exception:
                    continue
                time.sleep(0.1)  # Rate limit
        log.info(f"  HackerNews: collected {len(posts)} stories")
    except Exception as e:
        log.error(f"Error scanning HackerNews: {e}")
    return posts


# ─── Source: StockTwits ──────────────────────────────────────────────────────

STOCKTWITS_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AMD",
    "BABA", "PDD", "NIO", "XPEV", "PLTR", "SMCI", "ARM", "COIN",
    "MSTR", "CRWD", "GME", "RIVN", "LLY", "MRNA", "SOFI", "HOOD",
]


def fetch_stocktwits(ticker: str) -> list[dict]:
    """Fetch recent StockTwits messages for a ticker."""
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json()
        posts = []
        for msg in data.get("messages", []):
            body = msg.get("body", "")
            posts.append({
                "source": f"stocktwits/${ticker}",
                "title": body[:100],
                "body": body[:2000],
                "url": f"https://stocktwits.com/message/{msg.get('id', '')}",
                "score": msg.get("likes", {}).get("total", 0) if isinstance(msg.get("likes"), dict) else 0,
                "num_comments": 0,
                "created": msg.get("created_at", ""),
                "author": msg.get("user", {}).get("username", ""),
                "language": "en",
            })
        return posts
    except Exception as e:
        log.error(f"Error fetching StockTwits ${ticker}: {e}")
        return []


def scan_stocktwits() -> list[dict]:
    """Scan StockTwits for high-interest tickers."""
    all_posts = []
    for ticker in STOCKTWITS_TICKERS:
        log.info(f"  Scanning StockTwits: ${ticker}...")
        posts = fetch_stocktwits(ticker)
        all_posts.extend(posts)
        time.sleep(2)
    log.info(f"  StockTwits: collected {len(all_posts)} messages")
    return all_posts


# ─── Source: RSS/Blog Feeds ──────────────────────────────────────────────────

RSS_FEEDS = [
    # Tech leak sites
    "https://9to5mac.com/feed/",
    "https://9to5google.com/feed/",
    "https://www.theverge.com/rss/index.xml",
    "https://www.macrumors.com/macrumors.xml",
    "https://wccftech.com/feed/",
    # Regulatory
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=40&search_text=&start=0&output=atom",
]


def fetch_rss_feed(url: str) -> list[dict]:
    """Fetch and parse an RSS/Atom feed for relevant posts."""
    try:
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "GemFinder/1.0"
        })
        if resp.status_code != 200:
            return []

        posts = []
        # Simple XML parsing (avoid heavy dependencies)
        text = resp.text

        # Try to find items/entries
        items = re.findall(r"<item>(.*?)</item>", text, re.DOTALL)
        if not items:
            items = re.findall(r"<entry>(.*?)</entry>", text, re.DOTALL)

        for item in items[:30]:
            title = ""
            link = ""
            desc = ""

            t_match = re.search(r"<title[^>]*>(.*?)</title>", item, re.DOTALL)
            if t_match:
                title = re.sub(r"<!\[CDATA\[|\]\]>|<[^>]+>", "", t_match.group(1)).strip()

            l_match = re.search(r"<link[^>]*>(.*?)</link>", item, re.DOTALL)
            if not l_match:
                l_match = re.search(r'<link[^>]+href="([^"]+)"', item)
            if l_match:
                link = l_match.group(1).strip()

            d_match = re.search(
                r"<(?:description|summary|content)[^>]*>(.*?)</(?:description|summary|content)>",
                item, re.DOTALL,
            )
            if d_match:
                desc = re.sub(r"<!\[CDATA\[|\]\]>|<[^>]+>", "", d_match.group(1)).strip()

            posts.append({
                "source": f"rss/{url.split('/')[2]}",
                "title": title,
                "body": desc[:2000],
                "url": link,
                "score": 0,
                "num_comments": 0,
                "created": datetime.now(timezone.utc).isoformat(),
                "author": "",
                "language": detect_language(f"{title} {desc}"),
            })
        return posts
    except Exception as e:
        log.error(f"Error fetching RSS {url}: {e}")
        return []


def scan_rss_feeds() -> list[dict]:
    """Scan RSS feeds for relevant posts."""
    all_posts = []
    for feed_url in RSS_FEEDS:
        log.info(f"  Scanning RSS: {feed_url.split('/')[2]}...")
        posts = fetch_rss_feed(feed_url)
        all_posts.extend(posts)
        time.sleep(1)
    log.info(f"  RSS: collected {len(all_posts)} items from {len(RSS_FEEDS)} feeds")
    return all_posts


# ─── Company & Signal Matching ───────────────────────────────────────────────

def extract_tickers(text: str) -> list[str]:
    """Extract company tickers mentioned in text."""
    text_lower = text.lower()
    found = set()

    # Check keyword matches
    for keyword, ticker in KEYWORD_TO_TICKER.items():
        if keyword in text_lower:
            found.add(ticker)

    # Check $TICKER cashtag pattern
    cashtags = re.findall(r"\$([A-Z]{1,5})\b", text)
    for tag in cashtags:
        if tag in COMPANY_MAP:
            found.add(tag)

    return list(found)


def has_gem_signals(text: str) -> tuple[bool, list[str]]:
    """Check if text contains gem-worthy signal keywords."""
    text_lower = text.lower()
    found_signals = []
    for signal in GEM_SIGNALS:
        if signal in text_lower:
            found_signals.append(signal)
    return len(found_signals) > 0, found_signals


def pre_filter_posts(posts: list[dict]) -> list[dict]:
    """
    First-pass filter: keep only posts that mention a tracked company
    AND contain at least one gem signal keyword.
    Also detects language and batches Chinese posts for translation.
    """
    candidates = []
    seen_hashes = set()

    for post in posts:
        combined_text = f"{post['title']} {post['body']}"
        h = content_hash(combined_text)
        if h in seen_hashes:
            continue
        seen_hashes.add(h)

        # Detect language if not already set
        if "language" not in post:
            post["language"] = detect_language(combined_text)

        tickers = extract_tickers(combined_text)
        if not tickers:
            continue

        has_signal, signals = has_gem_signals(combined_text)
        if not has_signal:
            continue

        post["matched_tickers"] = tickers
        post["matched_signals"] = signals
        post["_hash"] = h
        candidates.append(post)

    # ── Translate Chinese candidates before AI scoring ──
    chinese_candidates = [c for c in candidates if c.get("language") == "zh"]
    if chinese_candidates:
        log.info(f"  Translating {len(chinese_candidates)} Chinese posts via DeepSeek...")
        # Process in batches of 8 (to stay within token limits)
        for i in range(0, len(chinese_candidates), 8):
            batch = chinese_candidates[i : i + 8]
            translate_chinese_batch(batch)
            time.sleep(1)

        # Replace title/body with translated versions for downstream processing
        for c in chinese_candidates:
            if c.get("title_en"):
                c["original_title"] = c.get("original_title", c["title"])
                c["original_body"] = c.get("original_body", c["body"])
                c["title"] = c["title_en"]
                c["body"] = c["body_en"]
        log.info(f"  Translation complete for {len(chinese_candidates)} posts")

    return candidates


# ─── DeepSeek AI Analysis ───────────────────────────────────────────────────

def analyze_gems_batch(candidates: list[dict]) -> list[dict]:
    """
    Use DeepSeek API to analyze and score candidate posts for gem quality.
    Processes in batches to be efficient with API calls.
    """
    if not candidates:
        return []

    scored_gems = []
    batch_size = 10

    for i in range(0, len(candidates), batch_size):
        batch = candidates[i : i + batch_size]
        posts_text = ""
        for idx, c in enumerate(batch):
            lang = c.get("language", "en")
            original_note = ""
            if lang == "zh" and c.get("original_title"):
                original_note = f"\nOriginal Chinese: {c.get('original_title', '')[:200]}"
            posts_text += f"""
--- POST {idx + 1} ---
Source: {c['source']}
Language: {lang.upper()}
Tickers: {', '.join(c['matched_tickers'])}
Signals: {', '.join(c['matched_signals'][:5])}
Title: {c['title'][:200]}
Body: {c['body'][:500]}{original_note}
Engagement: score={c['score']}, comments={c['num_comments']}
URL: {c['url']}
"""

        prompt = f"""You are a financial intelligence analyst specializing in identifying early-stage, 
potentially market-moving information about publicly traded companies BEFORE it becomes widely known.

Analyze the following {len(batch)} social media/forum posts and score each one as a potential "gem" - 
information that could meaningfully move a stock price.

CRITICAL - MULTILINGUAL CONTENT:
- Posts may be in Chinese (from Weibo, Baidu Tieba, Zhihu, Xiaohongshu) or English (Reddit, HN, etc).
- You MUST fully understand Chinese-language posts and extract their financial significance.
- ALL of your output (headline, why_gem, risk_note) MUST be written in ENGLISH only.
- For Chinese posts, translate the key finding into a clear English headline and explanation.
- Pay special attention to Chinese supply chain leaks, factory worker posts, and regulatory signals 
  — these often surface 24-48hrs before English-language media picks them up.
- Chinese social media posts about 裁员 (layoffs), 收购 (acquisition), 泄露 (leaks), 量产 (mass production),
  停产 (production halt), 罚款 (fines), 审批 (approvals) are high-value signals.

GEM CRITERIA (score 1-10):
- 10: Confirmed leak/insider info with specific actionable details (e.g., unreleased product specs from factory worker, M&A deal terms from source close to deal)
- 8-9: Highly credible early signal (e.g., supply chain confirmation of new product, credible takeover rumor with specific details, regulatory decision leak)
- 6-7: Interesting lead worth monitoring (e.g., unusual hiring patterns, early product sighting, unconfirmed but plausible rumor from credible source)
- 4-5: Mildly interesting but likely noise (e.g., speculation without evidence, rehash of known info)
- 1-3: Noise, obvious opinion, already widely known, or irrelevant

For EACH post, respond in this exact JSON format:
{{
  "analyses": [
    {{
      "post_number": 1,
      "gem_score": 8,
      "ticker": "AAPL",
      "category": "product_leak",
      "headline": "Factory worker posts photos of unreleased iPhone 17 Ultra with new camera module",
      "why_gem": "First-hand evidence from supply chain suggests new premium tier product, could signal ASP increase",
      "risk_note": "Could be fabricated; verify with additional sources",
      "urgency": "high",
      "original_language": "zh"
    }}
  ]
}}

Categories: product_leak, takeover_rumor, insider_signal, supply_chain, regulatory, 
executive_change, financial_signal, competitive_intel, china_signal, other

IMPORTANT: Be very selective. Most posts are NOT gems. Only score 7+ if the information 
is genuinely novel, specific, and could plausibly move a stock. Generic opinions, 
widely-known news, and vague speculation should score low.

POSTS TO ANALYZE:
{posts_text}"""

        try:
            resp = requests.post(
                DEEPSEEK_API_URL,
                headers={
                    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "You are a bilingual (English/Chinese) financial intelligence analyst. You can read and analyze content in both English and Chinese with native-level fluency. ALL output must be in English. Respond only in valid JSON."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.3,
                    "max_tokens": 4000,
                },
                timeout=60,
            )

            if resp.status_code != 200:
                log.error(f"DeepSeek API error: {resp.status_code} - {resp.text[:200]}")
                continue

            result = resp.json()
            ai_text = result["choices"][0]["message"]["content"]

            # Extract JSON from response (handle markdown code blocks)
            json_match = re.search(r"\{[\s\S]*\}", ai_text)
            if not json_match:
                log.warning("Could not parse DeepSeek response as JSON")
                continue

            analyses = json.loads(json_match.group())
            for analysis in analyses.get("analyses", []):
                post_idx = analysis.get("post_number", 1) - 1
                if 0 <= post_idx < len(batch):
                    candidate = batch[post_idx]
                    candidate["gem_score"] = analysis.get("gem_score", 0)
                    candidate["gem_category"] = analysis.get("category", "other")
                    candidate["gem_headline"] = analysis.get("headline", candidate["title"][:100])
                    candidate["gem_why"] = analysis.get("why_gem", "")
                    candidate["gem_risk"] = analysis.get("risk_note", "")
                    candidate["gem_urgency"] = analysis.get("urgency", "medium")
                    candidate["gem_ticker"] = analysis.get("ticker", candidate["matched_tickers"][0] if candidate["matched_tickers"] else "???")
                    scored_gems.append(candidate)

        except json.JSONDecodeError as e:
            log.error(f"JSON parse error from DeepSeek: {e}")
        except Exception as e:
            log.error(f"Error in DeepSeek analysis: {e}")

        time.sleep(1)  # Rate limit

    return scored_gems


# ─── Discord Webhook Output ─────────────────────────────────────────────────

# Major news outlets - if a story is covered by multiple of these, it's NOT a gem
MAJOR_OUTLETS = [
    "reuters.com", "bloomberg.com", "cnbc.com", "wsj.com", "ft.com",
    "nytimes.com", "washingtonpost.com", "bbc.com", "apnews.com",
    "marketwatch.com", "finance.yahoo.com", "seekingalpha.com",
    "barrons.com", "businessinsider.com", "thestreet.com",
    "fool.com", "investopedia.com", "benzinga.com", "zacks.com",
    "cnn.com/business", "foxbusiness.com",
    # Tech mainstream
    "techcrunch.com", "theverge.com", "engadget.com", "arstechnica.com",
    "wired.com", "zdnet.com", "cnet.com",
]


def search_web_duckduckgo(query: str, max_results: int = 10) -> list[dict]:
    """
    Search DuckDuckGo HTML for news results. No API key required.
    Returns list of {title, url, snippet} dicts.
    """
    url = "https://html.duckduckgo.com/html/"
    try:
        resp = requests.post(url, data={"q": query, "df": "d"}, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        if resp.status_code != 200:
            return []

        results = []
        # Parse result blocks from DDG HTML
        result_blocks = re.findall(
            r'class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>.*?'
            r'class="result__snippet"[^>]*>(.*?)</(?:td|div)',
            resp.text, re.DOTALL
        )
        for link, title, snippet in result_blocks[:max_results]:
            # DDG wraps URLs in a redirect - extract the actual URL
            actual_url = link
            url_match = re.search(r'uddg=([^&]+)', link)
            if url_match:
                actual_url = unquote(url_match.group(1))
            title_clean = re.sub(r'<[^>]+>', '', title).strip()
            snippet_clean = re.sub(r'<[^>]+>', '', snippet).strip()
            results.append({
                "title": title_clean,
                "url": actual_url,
                "snippet": snippet_clean,
            })
        return results
    except Exception as e:
        log.error(f"DuckDuckGo search error: {e}")
        return []


def search_google_news_rss(query: str) -> list[dict]:
    """
    Search Google News via RSS feed. No API key required.
    Returns list of {title, url, source, published} dicts.
    """
    url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"
    try:
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "GemFinder/1.0",
        })
        if resp.status_code != 200:
            return []

        results = []
        items = re.findall(r"<item>(.*?)</item>", resp.text, re.DOTALL)
        for item in items[:10]:
            title = ""
            link = ""
            source = ""
            pub_date = ""

            t = re.search(r"<title>(.*?)</title>", item, re.DOTALL)
            if t:
                title = re.sub(r"<!\[CDATA\[|\]\]>", "", t.group(1)).strip()
            l = re.search(r"<link>(.*?)</link>", item, re.DOTALL)
            if l:
                link = l.group(1).strip()
            s = re.search(r"<source[^>]*>(.*?)</source>", item, re.DOTALL)
            if s:
                source = re.sub(r"<!\[CDATA\[|\]\]>", "", s.group(1)).strip()
            p = re.search(r"<pubDate>(.*?)</pubDate>", item, re.DOTALL)
            if p:
                pub_date = p.group(1).strip()

            results.append({
                "title": title,
                "url": link,
                "source": source,
                "published": pub_date,
            })
        return results
    except Exception as e:
        log.error(f"Google News RSS error: {e}")
        return []


def check_novelty_single(gem: dict) -> dict:
    """
    Check if a single gem's information is already widely known.
    Searches the web, then uses DeepSeek to assess novelty.
    
    Adds to gem dict:
      - novelty_score (1-10): 10 = completely unknown, 1 = old news everywhere
      - novelty_verdict: "novel", "emerging", or "known"
      - novelty_reason: explanation
      - mainstream_coverage: list of outlets already covering it
    """
    ticker = gem.get("gem_ticker", "")
    headline = gem.get("gem_headline", "")
    category = gem.get("gem_category", "other")

    # ── Build search queries ──
    # Multiple queries to catch different angles of the same story
    queries = []

    # Query 1: Direct ticker + headline keywords
    headline_keywords = re.sub(r'[^\w\s]', '', headline)[:80]
    queries.append(f"{ticker} {headline_keywords}")

    # Query 2: Company name + key action words from headline
    company_name = ""
    if ticker in COMPANY_MAP:
        company_name = COMPANY_MAP[ticker][0]  # Primary name
    action_words = []
    for word in ["acquisition", "acquire", "merger", "layoff", "recall", "shutdown",
                  "launch", "leak", "approval", "investigation", "settlement",
                  "partnership", "contract", "patent", "IPO", "delisting"]:
        if word.lower() in headline.lower():
            action_words.append(word)
    if company_name and action_words:
        queries.append(f"{company_name} {' '.join(action_words[:2])}")
    elif company_name:
        queries.append(f"{company_name} {headline_keywords[:40]}")

    # Query 3: Narrower news-focused query
    queries.append(f"{ticker} stock news {headline_keywords[:40]}")

    # ── Execute searches ──
    all_search_results = []
    for q in queries[:3]:  # Max 3 queries per gem
        log.info(f"    Novelty check: searching \"{q[:60]}\"...")
        ddg_results = search_web_duckduckgo(q, max_results=8)
        all_search_results.extend(ddg_results)
        time.sleep(1)

        gnews_results = search_google_news_rss(q)
        all_search_results.extend([{
            "title": r["title"],
            "url": r["url"],
            "snippet": f"[{r['source']}] Published: {r['published']}",
        } for r in gnews_results])
        time.sleep(1)

    # Deduplicate by URL
    seen_urls = set()
    unique_results = []
    for r in all_search_results:
        url_key = r.get("url", "")[:100]
        if url_key and url_key not in seen_urls:
            seen_urls.add(url_key)
            unique_results.append(r)

    # ── Check for major outlet coverage ──
    mainstream_hits = []
    for r in unique_results:
        url_lower = r.get("url", "").lower()
        for outlet in MAJOR_OUTLETS:
            if outlet in url_lower:
                mainstream_hits.append({
                    "outlet": outlet,
                    "title": r["title"][:100],
                    "url": r["url"],
                })
                break

    # ── Build search results summary for DeepSeek ──
    search_summary = ""
    for idx, r in enumerate(unique_results[:15]):
        search_summary += f"{idx+1}. [{r.get('title', 'No title')[:100]}]\n"
        search_summary += f"   URL: {r.get('url', 'N/A')[:100]}\n"
        search_summary += f"   Snippet: {r.get('snippet', '')[:150]}\n\n"

    if not search_summary:
        search_summary = "NO SEARCH RESULTS FOUND - this information may be very new or niche."

    # ── DeepSeek Novelty Assessment ──
    prompt = f"""You are a financial intelligence analyst assessing whether a potential market-moving 
"gem" is genuinely novel (not yet widely known) or already mainstream news.

THE GEM TO VERIFY:
- Ticker: ${ticker}
- Headline: {headline}
- Category: {category}
- Source: {gem.get('source', 'unknown')}
- Why it was flagged: {gem.get('gem_why', 'N/A')[:300]}

WEB SEARCH RESULTS (from DuckDuckGo + Google News, searched just now):
{search_summary}

MAJOR OUTLET COVERAGE FOUND: {len(mainstream_hits)} articles from major outlets
{json.dumps(mainstream_hits[:5], indent=2) if mainstream_hits else "None found"}

ASSESS NOVELTY on a 1-10 scale:
- 9-10: NOVEL — No mainstream coverage found. Information appears to be from grassroots/social media 
  sources only. This is a true gem that hasn't hit the news cycle yet.
- 7-8: EMERGING — Very limited coverage, maybe 1 minor outlet or blog. Still early enough to be 
  actionable. The story is just starting to break.
- 5-6: PARTIALLY KNOWN — Covered by a few outlets but not yet dominant headlines. Some alpha 
  may remain in specific details or angles not widely reported.
- 3-4: WIDELY KNOWN — Multiple major outlets have covered this. Most active market participants 
  likely already know. The stock has probably already reacted.
- 1-2: OLD NEWS — Extensively covered, trending on financial media, already priced in. 
  Definitely NOT a gem.

CRITICAL RULES:
- If 3+ major outlets (Bloomberg, Reuters, CNBC, WSJ, etc.) have articles about this, score ≤ 4.
- If the information appears ONLY on social media/forums with NO mainstream coverage, score ≥ 8.
- If the specific DETAIL or ANGLE in the gem isn't in any search results even though the 
  general topic is known, score it higher (the novel detail is the gem).
- Consider timing: if mainstream articles are from TODAY, score lower. If the gem is about 
  something not yet in ANY search results, score 9-10.

Respond ONLY in this JSON format:
{{
  "novelty_score": 8,
  "verdict": "novel",
  "reason": "No mainstream coverage found. The supply chain leak about the new camera module appears only on Weibo and has not been picked up by English-language tech or financial media.",
  "mainstream_count": 0,
  "hours_ahead_estimate": 24,
  "adjusted_gem_score": 8
}}

verdict must be one of: "novel", "emerging", "known"
- "novel": novelty_score >= 7
- "emerging": novelty_score 5-6  
- "known": novelty_score <= 4

hours_ahead_estimate: your best guess of how many hours ahead of mainstream media this info is 
(0 if already known, 6-48 for emerging, 24-72+ for truly novel)

adjusted_gem_score: the original gem score adjusted for novelty. If the info is already widely 
known, REDUCE the score significantly (e.g., an 8/10 gem that's already on Bloomberg → 3/10).
If truly novel, keep or boost the score."""

    try:
        resp = requests.post(
            DEEPSEEK_API_URL,
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "You are a financial novelty assessor. Determine if information is already widely known or truly novel. Respond only in valid JSON."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
                "max_tokens": 1000,
            },
            timeout=60,
        )

        if resp.status_code != 200:
            log.error(f"DeepSeek novelty check error: {resp.status_code}")
            # On failure, assume novel (don't block gems due to API errors)
            gem["novelty_score"] = 7
            gem["novelty_verdict"] = "novel"
            gem["novelty_reason"] = "Novelty check API error - assuming novel"
            gem["mainstream_coverage"] = mainstream_hits
            gem["hours_ahead"] = -1
            return gem

        result = resp.json()
        ai_text = result["choices"][0]["message"]["content"]
        json_match = re.search(r"\{[\s\S]*\}", ai_text)

        if json_match:
            assessment = json.loads(json_match.group())
            gem["novelty_score"] = assessment.get("novelty_score", 5)
            gem["novelty_verdict"] = assessment.get("verdict", "emerging")
            gem["novelty_reason"] = assessment.get("reason", "")
            gem["mainstream_coverage"] = mainstream_hits
            gem["hours_ahead"] = assessment.get("hours_ahead_estimate", -1)
            # Override gem score with novelty-adjusted score
            gem["original_gem_score"] = gem.get("gem_score", 0)
            gem["gem_score"] = assessment.get("adjusted_gem_score", gem.get("gem_score", 0))
        else:
            log.warning("Could not parse novelty assessment JSON")
            gem["novelty_score"] = 7
            gem["novelty_verdict"] = "novel"
            gem["novelty_reason"] = "Parse error - assuming novel"
            gem["mainstream_coverage"] = mainstream_hits

    except Exception as e:
        log.error(f"Novelty check error: {e}")
        gem["novelty_score"] = 7
        gem["novelty_verdict"] = "novel"
        gem["novelty_reason"] = f"Check failed: {e}"
        gem["mainstream_coverage"] = mainstream_hits

    return gem


def verify_novelty_batch(gems: list[dict]) -> list[dict]:
    """
    Run novelty verification on all gem candidates.
    Returns only gems that pass the novelty filter (verdict != 'known').
    """
    if not gems:
        return []

    verified = []
    rejected = []

    for i, gem in enumerate(gems):
        ticker = gem.get("gem_ticker", "???")
        headline = gem.get("gem_headline", "")[:50]
        log.info(f"  Verifying novelty [{i+1}/{len(gems)}]: ${ticker} - {headline}...")

        check_novelty_single(gem)

        verdict = gem.get("novelty_verdict", "novel")
        novelty = gem.get("novelty_score", 5)
        adjusted = gem.get("gem_score", 0)

        if verdict == "known" or novelty <= 4:
            log.info(f"    ❌ REJECTED (novelty={novelty}/10, verdict={verdict}): already widely known")
            rejected.append(gem)
        elif verdict == "emerging" and adjusted < MIN_GEM_SCORE:
            log.info(f"    ⚠️ REJECTED (novelty={novelty}/10, adjusted_score={adjusted}): partially known, score too low after adjustment")
            rejected.append(gem)
        else:
            hours = gem.get("hours_ahead", -1)
            log.info(f"    ✅ PASSED (novelty={novelty}/10, verdict={verdict}, ~{hours}h ahead)")
            verified.append(gem)

        time.sleep(1)  # Rate limit between checks

    log.info(f"  Novelty verification: {len(verified)} passed, {len(rejected)} rejected as already known")
    return verified

CATEGORY_EMOJI = {
    "product_leak": "📱",
    "takeover_rumor": "🏦",
    "insider_signal": "🕵️",
    "supply_chain": "🏭",
    "regulatory": "⚖️",
    "executive_change": "👔",
    "financial_signal": "📊",
    "competitive_intel": "🎯",
    "china_signal": "🇨🇳",
    "other": "💎",
}

URGENCY_EMOJI = {
    "high": "🔴",
    "medium": "🟡",
    "low": "🟢",
}


def send_discord_header(gems_count: int, posts_scanned: int):
    """Send the daily summary header to Discord."""
    now = datetime.now(timezone.utc)
    embed = {
        "title": "💎 Gem Finder - Daily Intelligence Report",
        "description": (
            f"**Date:** {now.strftime('%B %d, %Y')}\n"
            f"**Posts Scanned:** {posts_scanned:,}\n"
            f"**Gems Found:** {gems_count}\n"
            f"**Min Score Threshold:** {MIN_GEM_SCORE}/10\n"
            f"**Sources:** Reddit, Weibo, Baidu Tieba, Zhihu, Xiaohongshu, HackerNews, StockTwits, RSS\n"
            f"**🌏 Chinese sources scanned with full translation**\n"
            f"**🔍 All gems verified for novelty via web search**"
        ),
        "color": 0x00D4AA if gems_count > 0 else 0x808080,
        "footer": {"text": "Gem Finder v1.2 | Novelty-verified | Not financial advice"},
        "timestamp": now.isoformat(),
    }
    requests.post(DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=10)


def send_gem_to_discord(gem: dict, rank: int):
    """Send a single gem finding to Discord."""
    category = gem.get("gem_category", "other")
    urgency = gem.get("gem_urgency", "medium")
    emoji = CATEGORY_EMOJI.get(category, "💎")
    urg_emoji = URGENCY_EMOJI.get(urgency, "🟡")
    score = gem.get("gem_score", 0)
    ticker = gem.get("gem_ticker", "???")

    # Score bar visual
    filled = "🟩" * score + "⬜" * (10 - score)

    # Language indicator
    lang = gem.get("language", "en")
    lang_tag = ""
    if lang == "zh":
        source_platform = gem.get("source", "").split("/")[0]
        lang_tag = f"🌏 **Translated from Chinese** ({source_platform})\n"

    # Build description
    novelty = gem.get("novelty_score", -1)
    verdict = gem.get("novelty_verdict", "")
    hours_ahead = gem.get("hours_ahead", -1)

    # Novelty badge
    novelty_tag = ""
    if verdict == "novel":
        novelty_tag = f"🆕 **NOVEL** (novelty {novelty}/10)"
        if hours_ahead > 0:
            novelty_tag += f" — est. **~{hours_ahead}h ahead** of mainstream media"
        novelty_tag += "\n"
    elif verdict == "emerging":
        novelty_tag = f"⏳ **EMERGING** (novelty {novelty}/10)"
        if hours_ahead > 0:
            novelty_tag += f" — est. ~{hours_ahead}h ahead"
        novelty_tag += "\n"

    description = (
        f"**Score:** {filled} **{score}/10**\n"
        f"**Urgency:** {urg_emoji} {urgency.upper()}\n"
        f"**Category:** {category.replace('_', ' ').title()}\n"
        f"{novelty_tag}"
        f"{lang_tag}\n"
        f"**Why This Is a Gem:**\n{gem.get('gem_why', 'N/A')[:500]}\n\n"
        f"**⚠️ Risk/Caveat:**\n{gem.get('gem_risk', 'N/A')[:300]}"
    )

    # Add novelty reason
    if gem.get("novelty_reason"):
        description += f"\n\n🔍 **Novelty Check:**\n{gem['novelty_reason'][:300]}"

    # Add original Chinese text snippet for Chinese-sourced gems
    if lang == "zh" and gem.get("original_title"):
        original_snippet = gem["original_title"][:150]
        description += f"\n\n📜 **Original (Chinese):**\n> {original_snippet}"

    embed = {
        "title": f"{emoji} #{rank} — ${ticker} | {gem.get('gem_headline', '')[:200]}",
        "description": description,
        "color": 0xFF4444 if score >= 9 else 0xFF8800 if score >= 7 else 0xFFCC00,
        "fields": [
            {
                "name": "📌 Source",
                "value": f"[{gem['source']}]({gem['url']})" if gem.get("url") else gem["source"],
                "inline": True,
            },
            {
                "name": "📊 Engagement",
                "value": f"⬆️ {gem.get('score', 0)} | 💬 {gem.get('num_comments', 0)}",
                "inline": True,
            },
            {
                "name": "🏷️ Tickers",
                "value": " ".join(f"`${t}`" for t in gem.get("matched_tickers", [])),
                "inline": True,
            },
        ],
        "footer": {"text": f"Found: {gem.get('created', 'N/A')[:19]} | Author: {gem.get('author', 'N/A')} | Lang: {lang.upper()}"},
    }

    try:
        resp = requests.post(DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=10)
        if resp.status_code == 429:
            retry_after = resp.json().get("retry_after", 5)
            log.warning(f"Discord rate limit, sleeping {retry_after}s...")
            time.sleep(retry_after)
            requests.post(DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=10)
        time.sleep(1)  # Avoid Discord rate limits
    except Exception as e:
        log.error(f"Error sending gem to Discord: {e}")


def send_no_gems_message():
    """Send a message when no gems were found."""
    embed = {
        "title": "💤 No Gems Found Today",
        "description": (
            "The daily scan completed but no posts met the minimum gem score threshold.\n"
            "This is normal — most days won't have major unreported signals.\n\n"
            "The system scanned Reddit, Weibo, Baidu Tieba, Zhihu, Xiaohongshu, HackerNews, StockTwits, and RSS feeds."
        ),
        "color": 0x808080,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    requests.post(DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=10)


# ─── Main Pipeline ───────────────────────────────────────────────────────────

def run_gem_finder():
    """Main execution pipeline."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log.info(f"═══ Gem Finder Run {run_id} ═══")
    log.info(f"Looking back {LOOKBACK_HOURS}h | Min score: {MIN_GEM_SCORE}")

    conn = init_db()

    # ── Phase 1: Collect from all sources ──
    log.info("Phase 1: Collecting posts from all sources...")
    all_posts = []

    log.info("[1/8] Scanning Reddit...")
    all_posts.extend(scan_reddit())

    log.info("[2/8] Scanning Weibo...")
    all_posts.extend(scan_weibo())

    log.info("[3/8] Scanning Baidu Tieba...")
    all_posts.extend(scan_baidu_tieba())

    log.info("[4/8] Scanning Zhihu...")
    all_posts.extend(scan_zhihu())

    log.info("[5/8] Scanning Xiaohongshu...")
    all_posts.extend(scan_xiaohongshu())

    log.info("[6/8] Scanning HackerNews...")
    all_posts.extend(scan_hackernews())

    log.info("[7/8] Scanning StockTwits...")
    all_posts.extend(scan_stocktwits())

    log.info("[8/8] Scanning RSS Feeds...")
    all_posts.extend(scan_rss_feeds())

    total_collected = len(all_posts)
    chinese_count = sum(1 for p in all_posts if p.get("language") == "zh")
    log.info(f"Total posts collected: {total_collected} ({chinese_count} Chinese)")

    # ── Phase 2: Pre-filter (keyword matching) ──
    log.info("Phase 2: Pre-filtering for company mentions + gem signals...")
    candidates = pre_filter_posts(all_posts)

    # Remove already-seen posts
    new_candidates = []
    for c in candidates:
        if not is_seen(conn, c["_hash"]):
            new_candidates.append(c)
    candidates = new_candidates
    log.info(f"Candidates after filtering: {len(candidates)} (removed {len(new_candidates) - len(candidates) if len(new_candidates) > len(candidates) else 0} seen)")

    if not candidates:
        log.info("No new candidates found. Sending no-gems message.")
        send_discord_header(0, total_collected)
        send_no_gems_message()
        conn.close()
        return

    # ── Phase 3: AI Analysis (DeepSeek) ──
    log.info(f"Phase 3: AI analysis of {len(candidates)} candidates via DeepSeek...")
    scored_gems = analyze_gems_batch(candidates)

    # Filter to gems meeting minimum score
    gems = [g for g in scored_gems if g.get("gem_score", 0) >= MIN_GEM_SCORE]
    gems.sort(key=lambda x: x.get("gem_score", 0), reverse=True)

    # Cap at top 15 gems before novelty check (to limit API calls)
    gems = gems[:15]

    log.info(f"Phase 3 complete: {len(gems)} gems scored {MIN_GEM_SCORE}+")

    if not gems:
        log.info("No gems met minimum score. Sending no-gems message.")
        send_discord_header(0, total_collected)
        send_no_gems_message()
        conn.close()
        return

    # ── Phase 3.5: Novelty Verification (Web Search + DeepSeek) ──
    log.info(f"Phase 3.5: Verifying novelty of {len(gems)} gems via web search...")
    gems = verify_novelty_batch(gems)

    # Re-sort by adjusted score after novelty check
    gems.sort(key=lambda x: x.get("gem_score", 0), reverse=True)

    # Re-filter in case novelty check reduced scores below threshold
    gems = [g for g in gems if g.get("gem_score", 0) >= MIN_GEM_SCORE]

    log.info(f"Phase 3.5 complete: {len(gems)} gems survived novelty filter")

    # ── Phase 4: Send to Discord ──
    log.info("Phase 4: Sending results to Discord...")
    send_discord_header(len(gems), total_collected)

    if gems:
        for rank, gem in enumerate(gems, 1):
            send_gem_to_discord(gem, rank)
            # Mark as seen
            mark_seen(
                conn,
                gem["_hash"],
                gem["source"],
                gem.get("gem_ticker", ""),
                gem.get("gem_headline", ""),
                gem.get("gem_score", 0),
            )
            log.info(f"  Gem #{rank}: [{gem.get('gem_score')}/10] ${gem.get('gem_ticker')} - {gem.get('gem_headline', '')[:60]}")
    else:
        send_no_gems_message()

    # ── Cleanup ──
    conn.execute(
        "INSERT INTO run_log VALUES (?, ?, ?, ?, ?)",
        (
            run_id,
            datetime.now(timezone.utc).isoformat(),
            datetime.now(timezone.utc).isoformat(),
            len(gems),
            total_collected,
        ),
    )
    conn.commit()
    conn.close()

    log.info(f"═══ Run complete. {len(gems)} gems sent to Discord. ═══")


if __name__ == "__main__":
    run_gem_finder()
