"""
Gem Finder v2.0 - Market Intelligence Radar
Complete rewrite with working data sources, better quality gates,
and focus on actionable intelligence for liquid US-traded stocks.

Sources:
  - Reddit (improved subreddit targeting, filters article reposts)
  - LinkedIn (executive changes, hiring signals via search dorking)
  - SEC EDGAR (8-K filings, insider transactions)
  - Google News RSS (targeted niche queries, non-mainstream only)
  - HackerNews (tech signals, non-mainstream links only)
  - Patent filings (via search)
  - GitHub (product development signals)
  - Court filings (CourtListener/RECAP)
  - Job posting signals (WARN notices, stealth hiring)
"""

import os, re, json, time, logging, hashlib, sqlite3, requests
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, unquote

# ─── Config ──────────────────────────────────────────────────────────────────

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK",
    "https://discordapp.com/api/webhooks/1474952931051831428/U3NpVQpkD1CsyjUztm30apV7MCgWP3Z9n1dWJKxdlFCBeyih8DG0XeSqZRZPb_3L25Yl")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-1cf5b2ab46a14eb6978ff7ba7ce3f3e3")
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DB_PATH = os.getenv("DB_PATH", "gem_finder.db")
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "26"))
MIN_GEM_SCORE = int(os.getenv("MIN_GEM_SCORE", "7"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("gem_finder")

# ─── Tracked Companies (liquid US-traded only) ───────────────────────────────

COMPANY_MAP = {
    "AAPL": ["apple", "iphone", "ipad", "macbook", "apple vision"],
    "MSFT": ["microsoft", "windows", "azure", "xbox", "copilot"],
    "GOOGL": ["google", "alphabet", "youtube", "waymo", "deepmind", "gemini ai"],
    "AMZN": ["amazon", "aws", "alexa"], "META": ["meta platforms", "facebook", "instagram", "whatsapp"],
    "NVDA": ["nvidia", "geforce", "rtx", "cuda", "jensen huang", "blackwell"],
    "TSLA": ["tesla", "cybertruck", "autopilot", "fsd", "gigafactory"],
    "TSM": ["tsmc", "taiwan semiconductor"], "AVGO": ["broadcom"],
    "ORCL": ["oracle cloud"], "AMD": ["amd", "radeon", "ryzen", "epyc", "lisa su"],
    "INTC": ["intel", "intel foundry"], "ARM": ["arm holdings"],
    "SMCI": ["supermicro", "super micro"], "MU": ["micron technology"],
    "QCOM": ["qualcomm", "snapdragon"], "ASML": ["asml"],
    "MRVL": ["marvell technology"], "ON": ["onsemi"],
    "CRM": ["salesforce"], "NOW": ["servicenow"], "SNOW": ["snowflake computing"],
    "PLTR": ["palantir"], "NET": ["cloudflare"], "DDOG": ["datadog"],
    "ZS": ["zscaler"], "CRWD": ["crowdstrike"], "PANW": ["palo alto networks"],
    "FTNT": ["fortinet"], "S": ["sentinelone", "sentinel one"], "MDB": ["mongodb"],
    "SHOP": ["shopify"], "SQ": ["block inc", "cash app"], "COIN": ["coinbase"],
    "HOOD": ["robinhood"], "WMT": ["walmart"], "COST": ["costco"],
    "TGT": ["target corporation"], "NKE": ["nike"], "SBUX": ["starbucks"],
    "MCD": ["mcdonalds"], "DIS": ["disney", "disney+", "espn"],
    "NFLX": ["netflix"], "SPOT": ["spotify"], "PARA": ["paramount global"],
    "WBD": ["warner bros discovery"], "UBER": ["uber"], "ABNB": ["airbnb"],
    "DASH": ["doordash"], "LLY": ["eli lilly", "mounjaro", "zepbound"],
    "NVO": ["novo nordisk", "ozempic", "wegovy"], "PFE": ["pfizer"],
    "MRNA": ["moderna"], "ABBV": ["abbvie"], "JNJ": ["johnson & johnson"],
    "BMY": ["bristol-myers"], "GILD": ["gilead"], "AMGN": ["amgen"],
    "REGN": ["regeneron"], "VRTX": ["vertex pharmaceuticals"],
    "BIIB": ["biogen"], "ISRG": ["intuitive surgical"],
    "JPM": ["jpmorgan", "jp morgan"], "GS": ["goldman sachs"],
    "MS": ["morgan stanley"], "BAC": ["bank of america"],
    "V": ["visa inc", "visa payments"],
    "MA": ["mastercard"],
    "RIVN": ["rivian"], "LCID": ["lucid motors"], "F": ["ford motor"], "GM": ["general motors"],
    "XOM": ["exxonmobil"], "CVX": ["chevron"], "COP": ["conocophillips"],
    "OXY": ["occidental petroleum"], "LMT": ["lockheed martin"],
    "BA": ["boeing"], "RTX": ["raytheon"], "GE": ["general electric"],
    "HON": ["honeywell"], "CAT": ["caterpillar"],
    "BABA": ["alibaba"], "PDD": ["pinduoduo", "temu"], "JD": ["jd.com"],
    "BIDU": ["baidu"], "NIO": ["nio"], "XPEV": ["xpeng"], "LI": ["li auto"],
    "MSTR": ["microstrategy", "michael saylor"],
    "GME": ["gamestop"], "SOFI": ["sofi technologies"],
    "RKLB": ["rocket lab"], "IONQ": ["ionq"],
}

KEYWORD_TO_TICKER = {}
# Tickers that are too short / common English words - only match with $ prefix
AMBIGUOUS_TICKERS = {"V", "S", "F", "ON", "MA", "GE", "LI"}
for ticker, keywords in COMPANY_MAP.items():
    for kw in keywords: KEYWORD_TO_TICKER[kw.lower()] = ticker
    if ticker not in AMBIGUOUS_TICKERS:
        KEYWORD_TO_TICKER[ticker.lower()] = ticker
    KEYWORD_TO_TICKER[f"${ticker.lower()}"] = ticker

GEM_SIGNALS = [
    "leak", "leaked", "prototype", "unreleased", "upcoming", "teardown", "fcc filing",
    "certification", "benchmark", "acquisition", "acquire", "takeover", "merger",
    "buyout", "bid for", "in talks", "going private", "insider buying", "insider selling",
    "insider purchase", "ceo", "cfo", "stepping down", "fired", "hired", "poached",
    "restructuring", "layoff", "layoffs", "mass layoff", "rif",
    "supply chain", "factory", "production halt", "shortage", "ramping", "mass production",
    "fda approval", "fda reject", "patent granted", "lawsuit filed", "antitrust",
    "investigation", "sec probe", "subpoena", "settlement", "recall",
    "guidance raise", "guidance cut", "beat estimates", "miss estimates",
    "downgrade", "upgrade", "short squeeze", "short interest",
    "13d filing", "activist investor", "poison pill",
    "contract awarded", "partnership", "government contract", "defense contract",
]

KNOWN_OUTLETS = [
    "reuters.com", "bloomberg.com", "apnews.com", "wsj.com", "ft.com",
    "nytimes.com", "washingtonpost.com", "bbc.com", "bbc.co.uk",
    "guardian.com", "economist.com", "forbes.com", "fortune.com",
    "businessinsider.com", "insider.com", "axios.com", "politico.com",
    "cnbc.com", "marketwatch.com", "finance.yahoo.com", "seekingalpha.com",
    "barrons.com", "thestreet.com", "fool.com", "investopedia.com",
    "benzinga.com", "zacks.com", "foxbusiness.com", "cnn.com",
    "morningstar.com", "tipranks.com",
    "techcrunch.com", "theverge.com", "engadget.com", "arstechnica.com",
    "wired.com", "zdnet.com", "cnet.com", "gizmodo.com",
    "venturebeat.com", "techradar.com", "tomsguide.com", "tomshardware.com",
    "pcmag.com", "theinformation.com", "semafor.com", "techmeme.com",
    "macrumors.com", "9to5mac.com", "9to5google.com", "appleinsider.com",
    "androidcentral.com", "androidauthority.com", "xda-developers.com",
    "gsmarena.com", "notebookcheck.com",
    "wccftech.com", "videocardz.com", "pcgamer.com", "ign.com",
    "gamespot.com", "polygon.com", "windowscentral.com", "neowin.net",
    "digitaltrends.com",
    "electrek.co", "insideevs.com", "teslarati.com", "thedrive.com",
    "caranddriver.com", "autoblog.com", "cleantechnica.com",
    "statnews.com", "fiercepharma.com", "fiercebiotech.com",
    "biopharmadive.com", "biospace.com", "endpoints.com",
    "coindesk.com", "cointelegraph.com", "theblock.co", "decrypt.co",
    "semianalysis.com", "eetimes.com", "digitimes.com", "trendforce.com",
    "scmp.com", "nikkei.com",
    "news.google.com", "msn.com", "news.yahoo.com", "medium.com", "substack.com",
]

# ─── DB ──────────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS seen_posts (content_hash TEXT PRIMARY KEY, source TEXT, ticker TEXT, title TEXT, score REAL, found_at TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS run_log (run_id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT, gems_found INTEGER, posts_scanned INTEGER)")
    conn.commit()
    return conn

def is_seen(conn, h): return conn.execute("SELECT 1 FROM seen_posts WHERE content_hash=?", (h,)).fetchone() is not None
def mark_seen(conn, h, src, tkr, ttl, sc):
    conn.execute("INSERT OR IGNORE INTO seen_posts VALUES(?,?,?,?,?,?)", (h, src, tkr, ttl, sc, datetime.now(timezone.utc).isoformat()))
    conn.commit()
def content_hash(t): return hashlib.sha256(t.encode()).hexdigest()[:16]

# ─── HTTP ────────────────────────────────────────────────────────────────────

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"})

def sget(url, **kw):
    kw.setdefault("timeout", 15)
    try:
        r = S.get(url, **kw)
        return r if r.status_code == 200 else None
    except: return None

def spost(url, **kw):
    kw.setdefault("timeout", 15)
    try:
        r = S.post(url, **kw)
        return r if r.status_code == 200 else None
    except: return None

def mkpost(source, title, body, url, score=0, comments=0, author="", created=None):
    return {"source": source, "title": (title or "")[:300], "body": (body or "")[:3000],
            "url": url or "", "score": score, "num_comments": comments,
            "created": created or datetime.now(timezone.utc).isoformat(), "author": author}

def search_ddg(query, n=10):
    r = spost("https://html.duckduckgo.com/html/", data={"q": query, "df": "d"})
    if not r: return []
    results = []
    for link, title, snippet in re.findall(r'class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>.*?class="result__snippet"[^>]*>(.*?)</(?:td|div)', r.text, re.DOTALL)[:n]:
        u = link
        m = re.search(r'uddg=([^&]+)', link)
        if m: u = unquote(m.group(1))
        results.append({"title": re.sub(r'<[^>]+>', '', title).strip(), "url": u, "snippet": re.sub(r'<[^>]+>', '', snippet).strip()})
    return results

def search_gnews_rss(query, filter_known=True):
    r = sget(f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en")
    if not r: return []
    posts = []
    for item in re.findall(r"<item>(.*?)</item>", r.text, re.DOTALL)[:10]:
        t = re.search(r"<title>(.*?)</title>", item, re.DOTALL)
        l = re.search(r"<link>(.*?)</link>", item, re.DOTALL)
        s = re.search(r"<source[^>]*>(.*?)</source>", item, re.DOTALL)
        p = re.search(r"<pubDate>(.*?)</pubDate>", item, re.DOTALL)
        if not t or not l: continue
        tt = re.sub(r"<!\[CDATA\[|\]\]>", "", t.group(1)).strip()
        ll = l.group(1).strip()
        sn = re.sub(r"<!\[CDATA\[|\]\]>", "", s.group(1)).strip() if s else ""
        if filter_known and (any(o in ll.lower() for o in KNOWN_OUTLETS) or any(o in sn.lower() for o in KNOWN_OUTLETS)):
            continue
        posts.append({"title": tt, "url": ll, "snippet": sn, "published": p.group(1).strip() if p else ""})
    return posts

# ─── Source 1: Reddit ────────────────────────────────────────────────────────

REDDIT_SUBS = [
    "wallstreetbets", "stocks", "investing", "stockmarket", "options",
    "SecurityAnalysis", "valueinvesting", "smallstreetbets",
    "pennystocks", "spacs", "Biotechplays", "maxjustrisk",
    "FluentInFinance", "unusual_whales",
    "technology", "Apple", "nvidia", "Amd", "intel",
    "hardware", "gadgets", "biotech", "electricvehicles", "teslamotors",
    "cybersecurity", "China", "ChinaTech", "supplychain",
    "news", "worldnews", "business",
]

def scan_reddit():
    all_posts = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    for sub in REDDIT_SUBS:
        log.info(f"  r/{sub}...")
        for sort in ["new", "hot"]:
            r = sget(f"https://old.reddit.com/r/{sub}/{sort}.json?limit=50", headers={"User-Agent": "GemFinder/2.0"})
            if not r: continue
            try: data = r.json()
            except: continue
            for child in data.get("data", {}).get("children", []):
                p = child.get("data", {})
                created = datetime.fromtimestamp(p.get("created_utc", 0), tz=timezone.utc)
                if created < cutoff: continue
                # Skip article reposts from known outlets
                link = p.get("url", "")
                if any(o in link.lower() for o in KNOWN_OUTLETS[:50]) and not p.get("selftext"):
                    continue
                all_posts.append(mkpost(f"reddit/r/{sub}", p.get("title", ""),
                    p.get("selftext", "") or "", f"https://reddit.com{p.get('permalink', '')}",
                    p.get("score", 0), p.get("num_comments", 0), p.get("author", ""), created.isoformat()))
            time.sleep(2)
    log.info(f"  Reddit: {len(all_posts)} posts")
    return all_posts

# ─── Source 2: LinkedIn (via DuckDuckGo) ─────────────────────────────────────

LINKEDIN_BATCHES = [
    "Apple OR Google OR Microsoft OR Amazon OR Meta",
    "NVIDIA OR AMD OR Intel OR Tesla OR Broadcom",
    "Salesforce OR Palantir OR CrowdStrike OR Snowflake",
    "Pfizer OR Moderna OR Eli Lilly OR Novo Nordisk",
    "Disney OR Netflix OR Uber OR Airbnb OR Coinbase",
    "JPMorgan OR Goldman Sachs OR Boeing OR Lockheed",
    "Alibaba OR Pinduoduo OR NIO OR Xpeng OR Baidu",
]

LINKEDIN_QUERIES = [
    'site:linkedin.com/in "excited to announce" "joined" ({co})',
    'site:linkedin.com/in "stepping down" OR "leaving" OR "last day" ({co})',
    'site:linkedin.com "laid off" OR "let go" OR "affected by layoffs" ({co})',
    'site:linkedin.com/in "due diligence" OR "integration" ({co})',
    'site:linkedin.com/posts "insider" OR "not public" OR "unannounced" ({co})',
]

def scan_linkedin():
    posts = []
    for batch in LINKEDIN_BATCHES:
        for qtpl in LINKEDIN_QUERIES:
            q = qtpl.replace("({co})", batch)
            log.info(f"  LinkedIn: {batch[:30]}...")
            for r in search_ddg(q, n=5):
                if "linkedin.com" not in r["url"].lower(): continue
                posts.append(mkpost("linkedin", r["title"], r["snippet"], r["url"]))
            time.sleep(3)
    log.info(f"  LinkedIn: {len(posts)} posts")
    return posts

# ─── Source 3: SEC EDGAR ─────────────────────────────────────────────────────

def scan_sec_edgar():
    posts = []
    # 8-K filings (material events)
    log.info("  EDGAR: 8-K filings...")
    r = sget("https://efts.sec.gov/LATEST/search-index?q=%228-K%22&dateRange=custom&startdt=" +
             (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d") +
             "&enddt=" + datetime.now(timezone.utc).strftime("%Y-%m-%d"),
             headers={"User-Agent": "GemFinder research@gemfinder.io", "Accept": "application/json"})
    if r:
        try:
            for hit in r.json().get("hits", {}).get("hits", [])[:20]:
                src = hit.get("_source", {})
                names = src.get("display_names", [""])
                posts.append(mkpost("sec_edgar/8k", names[0] if names else "SEC Filing",
                    src.get("file_description", ""), f"https://efts.sec.gov/LATEST/search-index?q={quote(names[0] if names else '')}"))
        except: pass

    # EDGAR full-text for M&A keywords
    for kw in ["acquisition agreement", "merger agreement", "going private", "tender offer", "poison pill"]:
        log.info(f"  EDGAR: '{kw}'...")
        r = sget(f"https://efts.sec.gov/LATEST/search-index?q=%22{quote(kw)}%22&dateRange=custom&startdt=" +
                 (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d") +
                 "&enddt=" + datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                 headers={"User-Agent": "GemFinder research@gemfinder.io", "Accept": "application/json"})
        if r:
            try:
                for hit in r.json().get("hits", {}).get("hits", [])[:10]:
                    src = hit.get("_source", {})
                    names = src.get("display_names", [""])
                    posts.append(mkpost(f"sec_edgar/{kw[:15]}", names[0] if names else "SEC",
                        f"{kw}: {src.get('file_description', '')}", "https://www.sec.gov/cgi-bin/browse-edgar"))
            except: pass
        time.sleep(1)

    # EDGAR ATOM feed for latest 8-Ks
    r = sget("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=40&search_text=&start=0&output=atom",
             headers={"User-Agent": "GemFinder research@gemfinder.io"})
    if r:
        for entry in re.findall(r"<entry>(.*?)</entry>", r.text, re.DOTALL)[:40]:
            t = re.search(r"<title[^>]*>(.*?)</title>", entry, re.DOTALL)
            l = re.search(r'<link[^>]+href="([^"]+)"', entry)
            s = re.search(r"<summary[^>]*>(.*?)</summary>", entry, re.DOTALL)
            if t and l:
                posts.append(mkpost("sec_edgar/atom", re.sub(r"<[^>]+>", "", t.group(1)).strip(),
                    re.sub(r"<[^>]+>", "", s.group(1)).strip() if s else "", l.group(1)))

    log.info(f"  EDGAR: {len(posts)} filings")
    return posts

# ─── Source 4: Google News (niche queries, non-mainstream only) ──────────────

GNEWS_QUERIES = [
    '"supply chain" "not yet announced"', '"factory workers" leak prototype',
    '"in talks to acquire"', '"exploring strategic alternatives"',
    '"poison pill" OR "shareholder rights plan"', '"activist stake" OR "13D filing"',
    '"insider buying" cluster', '"CEO purchase" shares million',
    '"FDA advisory committee" recommend', '"patent infringement" ruling',
    '"FCC filing" reveals', '"firmware" leak OR teardown',
    '"mass layoff" OR "WARN notice"', '"stealth startup" hiring',
    '"trade secret" lawsuit', '"whistleblower" complaint SEC',
    '"going private" transaction', '"tender offer" announced',
]

def scan_google_news():
    posts = []
    for q in GNEWS_QUERIES:
        log.info(f"  GNews: {q[:50]}...")
        for r in search_gnews_rss(q, filter_known=True):
            posts.append(mkpost(f"gnews/{r.get('snippet', '')[:25]}", r["title"], r.get("snippet", ""), r["url"],
                                created=r.get("published")))
        time.sleep(2)
    log.info(f"  GNews (non-mainstream): {len(posts)}")
    return posts

# ─── Source 5: HackerNews ───────────────────────────────────────────────────

def scan_hackernews():
    posts = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    for ep in ["topstories", "newstories"]:
        r = sget(f"https://hacker-news.firebaseio.com/v0/{ep}.json")
        if not r: continue
        try: ids = r.json()[:80]
        except: continue
        for sid in ids:
            sr = sget(f"https://hacker-news.firebaseio.com/v0/item/{sid}.json")
            if not sr: continue
            try: s = sr.json()
            except: continue
            if not s or s.get("type") != "story": continue
            created = datetime.fromtimestamp(s.get("time", 0), tz=timezone.utc)
            if created < cutoff: continue
            url = s.get("url", "")
            if any(o in url.lower() for o in KNOWN_OUTLETS[:50]): continue
            posts.append(mkpost("hackernews", s.get("title", ""), s.get("text", "") or "",
                url or f"https://news.ycombinator.com/item?id={sid}",
                s.get("score", 0), s.get("descendants", 0), s.get("by", ""), created.isoformat()))
            time.sleep(0.1)
    log.info(f"  HackerNews: {len(posts)} (non-mainstream)")
    return posts

# ─── Source 6: Patents ───────────────────────────────────────────────────────

PATENT_COS = ["Apple", "Google", "Microsoft", "NVIDIA", "Tesla", "Meta", "Amazon", "AMD",
              "Intel", "Qualcomm", "Eli Lilly", "Moderna", "Pfizer", "Regeneron"]

def scan_patents():
    posts = []
    for co in PATENT_COS:
        for r in search_ddg(f'"{co}" patent application OR patent granted site:patents.google.com', n=3):
            posts.append(mkpost(f"patent/{co}", r["title"], r["snippet"], r["url"]))
        time.sleep(2)
    log.info(f"  Patents: {len(posts)}")
    return posts

# ─── Source 7: GitHub ────────────────────────────────────────────────────────

GITHUB_ORGS = [("apple", ""), ("microsoft", "preview OR unreleased"), ("google", "experimental"),
               ("meta-llama", ""), ("openai", ""), ("nvidia", "preview")]

def scan_github():
    posts = []
    d = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%d")
    for org, q in GITHUB_ORGS:
        r = sget(f"https://api.github.com/search/repositories?q=org:{org}+{quote(q)}+created:>{d}&sort=updated&per_page=5",
                 headers={"Accept": "application/vnd.github.v3+json"})
        if r:
            try:
                for item in r.json().get("items", [])[:5]:
                    posts.append(mkpost(f"github/{org}", item.get("full_name", ""),
                        item.get("description", "") or "", item.get("html_url", ""), item.get("stargazers_count", 0)))
            except: pass
        time.sleep(2)
    log.info(f"  GitHub: {len(posts)}")
    return posts

# ─── Source 8: Job Signals ───────────────────────────────────────────────────

def scan_jobs():
    posts = []
    for q in ['"WARN notice" layoff', '"stealth" "unannounced product" hiring engineer',
              '"confidential" "product launch" hiring']:
        for r in search_ddg(q, n=5):
            if any(o in r["url"].lower() for o in KNOWN_OUTLETS): continue
            posts.append(mkpost("job_signal", r["title"], r["snippet"], r["url"]))
        time.sleep(2)
    log.info(f"  Jobs: {len(posts)}")
    return posts

# ─── Source 9: Court Filings ─────────────────────────────────────────────────

def scan_courts():
    posts = []
    d = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    r = sget(f"https://www.courtlistener.com/api/rest/v4/search/?q=acquisition+OR+patent+OR+antitrust&type=r&order_by=dateFiled+desc&filed_after={d}")
    if r:
        try:
            for res in r.json().get("results", [])[:20]:
                posts.append(mkpost("court/recap", res.get("caseName", ""), res.get("snippet", ""),
                    f"https://www.courtlistener.com{res.get('absolute_url', '')}"))
        except: pass
    log.info(f"  Courts: {len(posts)}")
    return posts

# ─── Matching ────────────────────────────────────────────────────────────────

def extract_tickers(text):
    tl = text.lower()
    found = set()
    for kw, tk in KEYWORD_TO_TICKER.items():
        if kw in tl: found.add(tk)
    for tag in re.findall(r"\$([A-Z]{1,5})\b", text):
        if tag in COMPANY_MAP: found.add(tag)
    return list(found)

def has_gem_signals(text):
    tl = text.lower()
    return [s for s in GEM_SIGNALS if s in tl]

def pre_filter(posts):
    cands, seen = [], set()
    for p in posts:
        txt = f"{p['title']} {p['body']}"
        h = content_hash(txt)
        if h in seen: continue
        seen.add(h)
        tickers = extract_tickers(txt)
        if not tickers: continue
        signals = has_gem_signals(txt)
        if not signals: continue
        p["matched_tickers"] = tickers
        p["matched_signals"] = signals
        p["_hash"] = h
        cands.append(p)
    return cands

# ─── DeepSeek Scoring ───────────────────────────────────────────────────────

def score_gems(candidates):
    if not candidates: return []
    scored = []
    for i in range(0, len(candidates), 10):
        batch = candidates[i:i+10]
        ptxt = ""
        for idx, c in enumerate(batch):
            ptxt += f"\n--- POST {idx+1} ---\nSource: {c['source']}\nTickers: {', '.join(c['matched_tickers'])}\nSignals: {', '.join(c['matched_signals'][:5])}\nTitle: {c['title'][:200]}\nBody: {c['body'][:600]}\nEngagement: score={c['score']}, comments={c['num_comments']}\nURL: {c['url']}\n"

        prompt = f"""Analyze {len(batch)} posts as potential stock-moving "gems."

QUALITY RULES:
1. Ticker MUST be a valid US stock ticker from this list: {', '.join(list(COMPANY_MAP.keys())[:40])}...
   NEVER output "None" or long text as ticker. Use the best-matching ticker.
2. Only score 7+ for SPECIFIC, ACTIONABLE info about a LIQUID stock.
3. Micro-cap penny stock pumps = score 1-2. Generic macro observations = 1-3.
4. Product leaks only matter if UNEXPECTED. Known roadmap items = low score.
5. Insider buying only matters if >$500K or a cluster.
6. Headlines must be CLEAR ENGLISH describing the specific finding.

SCORING:
9-10: Confirmed insider info, supply chain leak, imminent M&A details
7-8: Credible first-hand signal (employee, regulator, factory worker)
5-6: Interesting but unverified
1-4: Noise, opinion, known info, penny pump

JSON response:
{{"analyses": [{{"post_number": 1, "gem_score": 8, "ticker": "AAPL", "category": "product_leak", "headline": "Clear English headline", "why_gem": "Why actionable", "risk_note": "Key risk", "urgency": "high"}}]}}

Categories: product_leak, takeover_rumor, insider_signal, supply_chain, regulatory, executive_change, financial_signal, competitive_intel, legal_filing, other

POSTS:
{ptxt}"""

        try:
            resp = requests.post(DEEPSEEK_API_URL, headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={"model": "deepseek-chat", "messages": [
                    {"role": "system", "content": "Financial intelligence analyst. Valid US tickers only. JSON only."},
                    {"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 4000}, timeout=90)
            if resp.status_code != 200:
                log.error(f"DeepSeek: {resp.status_code}")
                continue
            ai = resp.json()["choices"][0]["message"]["content"]
            m = re.search(r"\{[\s\S]*\}", ai)
            if not m: continue
            for a in json.loads(m.group()).get("analyses", []):
                idx = a.get("post_number", 1) - 1
                if not (0 <= idx < len(batch)): continue
                c = batch[idx]
                tk = a.get("ticker", "")
                if tk not in COMPANY_MAP:
                    tk = c["matched_tickers"][0] if c["matched_tickers"] else None
                if not tk: continue
                c.update(gem_score=a.get("gem_score", 0), gem_category=a.get("category", "other"),
                    gem_headline=a.get("headline", c["title"][:100]), gem_why=a.get("why_gem", ""),
                    gem_risk=a.get("risk_note", ""), gem_urgency=a.get("urgency", "medium"), gem_ticker=tk)
                scored.append(c)
        except Exception as e:
            log.error(f"Score error: {e}")
        time.sleep(1)
    return scored

# ─── Novelty ─────────────────────────────────────────────────────────────────

def check_novelty(gem):
    tk = gem.get("gem_ticker", "")
    hl = gem.get("gem_headline", "")
    src = gem.get("source", "")
    # Hard reject from known outlet
    for o in KNOWN_OUTLETS:
        short = o.replace(".com","").replace(".co","").replace(".net","")
        if short in src.lower() or o in src.lower():
            gem.update(novelty_score=2, novelty_verdict="known", novelty_reason=f"Source is {o}",
                       mainstream_coverage=[{"outlet":o}], hours_ahead=0, original_gem_score=gem.get("gem_score",0), gem_score=2)
            return gem

    kw = re.sub(r'[^\w\s]', '', hl)[:80]
    queries = [f"{tk} {kw}", f"{tk} stock news {kw[:40]}"]
    co = COMPANY_MAP.get(tk, [tk.lower()])[0]
    queries.append(f"{co} {kw[:40]}")

    all_res = []
    for q in queries:
        log.info(f"    Novelty: \"{q[:60]}\"...")
        all_res.extend(search_ddg(q, n=8))
        time.sleep(1)
        for nr in search_gnews_rss(q, filter_known=False):
            all_res.append({"title": nr["title"], "url": nr["url"], "snippet": nr.get("snippet","")})
        time.sleep(1)

    seen_u = set()
    uniq = []
    for r in all_res:
        k = r.get("url","")[:100]
        if k and k not in seen_u: seen_u.add(k); uniq.append(r)

    hits = []
    for r in uniq:
        ul = r.get("url","").lower()
        for o in KNOWN_OUTLETS:
            if o in ul: hits.append({"outlet":o, "title":r["title"][:100]}); break

    if len(hits) >= 2:
        gem.update(novelty_score=2, novelty_verdict="known",
            novelty_reason=f"Covered by {len(hits)} outlets: {', '.join(h['outlet'] for h in hits[:5])}",
            mainstream_coverage=hits, hours_ahead=0, original_gem_score=gem.get("gem_score",0), gem_score=2)
        log.info(f"    Fast-reject: {len(hits)} outlets")
        return gem

    ss = "\n".join(f"{i+1}. [{r.get('title','')[:100]}] {r.get('url','')[:80]}" for i,r in enumerate(uniq[:12])) or "NO RESULTS"

    prompt = f"""Novelty check for: ${tk} — {hl}
Source: {src} | Known outlet hits: {len(hits)}

Search results:
{ss}

RULES: ANY known outlet coverage → score ≤4. ONLY social media/forums with zero outlet coverage → 8-10.
JSON: {{"novelty_score":8,"verdict":"novel","reason":"...","hours_ahead_estimate":36,"adjusted_gem_score":8}}
verdict: "novel"(≥7), "emerging"(5-6), "known"(≤4)"""

    try:
        resp = requests.post(DEEPSEEK_API_URL, headers={"Authorization":f"Bearer {DEEPSEEK_API_KEY}","Content-Type":"application/json"},
            json={"model":"deepseek-chat","messages":[{"role":"system","content":"Novelty assessor. JSON only."},
                {"role":"user","content":prompt}],"temperature":0.2,"max_tokens":500}, timeout=60)
        if resp.status_code == 200:
            ai = resp.json()["choices"][0]["message"]["content"]
            m = re.search(r"\{[\s\S]*\}", ai)
            if m:
                a = json.loads(m.group())
                gem.update(novelty_score=a.get("novelty_score",5), novelty_verdict=a.get("verdict","emerging"),
                    novelty_reason=a.get("reason",""), hours_ahead=a.get("hours_ahead_estimate",-1),
                    original_gem_score=gem.get("gem_score",0), gem_score=a.get("adjusted_gem_score",gem["gem_score"]),
                    mainstream_coverage=hits)
                return gem
    except Exception as e:
        log.error(f"Novelty API: {e}")

    if hits: gem.update(novelty_score=5, novelty_verdict="emerging", novelty_reason="1 hit", mainstream_coverage=hits)
    else: gem.update(novelty_score=8, novelty_verdict="novel", novelty_reason="No coverage found", mainstream_coverage=hits)
    return gem

def verify_novelty(gems):
    verified = []
    for i, g in enumerate(gems):
        log.info(f"  [{i+1}/{len(gems)}] ${g.get('gem_ticker','???')} - {g.get('gem_headline','')[:50]}...")
        check_novelty(g)
        v, n, adj = g.get("novelty_verdict",""), g.get("novelty_score",5), g.get("gem_score",0)
        if v == "known": log.info(f"    ❌ KNOWN ({n}/10)")
        elif v == "emerging": log.info(f"    ⚠️ EMERGING ({n}/10, adj={adj})")
        elif adj >= MIN_GEM_SCORE:
            log.info(f"    ✅ TRUE GEM ({n}/10, ~{g.get('hours_ahead',-1)}h ahead)")
            verified.append(g)
        else: log.info(f"    ⚠️ Score dropped ({adj})")
        time.sleep(1)
    log.info(f"  Novelty: {len(verified)} true gems / {len(gems)} checked")
    return verified

# ─── Discord ─────────────────────────────────────────────────────────────────

CAT_EMOJI = {"product_leak":"📱","takeover_rumor":"🏦","insider_signal":"🕵️","supply_chain":"🏭",
    "regulatory":"⚖️","executive_change":"👔","financial_signal":"📊","competitive_intel":"🎯",
    "legal_filing":"⚖️","other":"💎"}

def send_header(gc, ps, ss):
    now = datetime.now(timezone.utc)
    requests.post(DISCORD_WEBHOOK, json={"embeds": [{"title": "💎 Gem Finder v2.0 — Daily Report",
        "description": f"**{now.strftime('%B %d, %Y')}**\n**Scanned:** {ps:,}\n**True Gems:** {gc}\n**Sources:** {ss}\n**Filter:** Score ≥{MIN_GEM_SCORE} + novelty verified + liquid US stocks",
        "color": 0x00D4AA if gc > 0 else 0x808080,
        "footer": {"text": "Gem Finder v2.0 | Not financial advice"}, "timestamp": now.isoformat()}]}, timeout=10)

def send_gem(g, rank):
    cat = g.get("gem_category","other"); sc = g.get("gem_score",0); tk = g.get("gem_ticker","???")
    n = g.get("novelty_score",-1); h = g.get("hours_ahead",-1)
    urg = g.get("gem_urgency","medium")
    bar = "🟩"*sc + "⬜"*(10-sc)
    ue = {"high":"🔴","medium":"🟡","low":"🟢"}.get(urg,"🟡")
    nt = f"🆕 **NOVEL** ({n}/10) — ~{h}h ahead\n" if n >= 7 and h > 0 else f"🆕 **NOVEL** ({n}/10)\n" if n >= 7 else ""
    desc = f"**Score:** {bar} **{sc}/10**\n**Urgency:** {ue} {urg.upper()} | **Category:** {cat.replace('_',' ').title()}\n{nt}\n**Why Gem:**\n{g.get('gem_why','N/A')[:500]}\n\n**⚠️ Risk:** {g.get('gem_risk','N/A')[:300]}"
    if g.get("novelty_reason"): desc += f"\n\n🔍 **Novelty:** {g['novelty_reason'][:250]}"
    embed = {"title": f"{CAT_EMOJI.get(cat,'💎')} #{rank} — ${tk} | {g.get('gem_headline','')[:200]}",
        "description": desc, "color": 0xFF4444 if sc>=9 else 0xFF8800 if sc>=7 else 0xFFCC00,
        "fields": [
            {"name":"📌 Source","value":f"[{g['source'][:40]}]({g['url']})" if g.get("url") else g["source"],"inline":True},
            {"name":"📊 Engagement","value":f"⬆️ {g.get('score',0)} | 💬 {g.get('num_comments',0)}","inline":True},
            {"name":"🏷️ Tickers","value":" ".join(f"`${t}`" for t in g.get("matched_tickers",[])[:5]),"inline":True}],
        "footer":{"text":f"{g.get('author','')[:30]} | {g.get('source','')}"}}
    try:
        r = requests.post(DISCORD_WEBHOOK, json={"embeds":[embed]}, timeout=10)
        if r.status_code == 429: time.sleep(r.json().get("retry_after",5)); requests.post(DISCORD_WEBHOOK, json={"embeds":[embed]}, timeout=10)
        time.sleep(1)
    except Exception as e: log.error(f"Discord: {e}")

def send_no_gems():
    requests.post(DISCORD_WEBHOOK, json={"embeds":[{"title":"💤 No True Gems Today",
        "description":"All candidates were already public or didn't meet quality thresholds. Real alpha is rare.",
        "color":0x808080,"timestamp":datetime.now(timezone.utc).isoformat()}]}, timeout=10)

# ─── Main ────────────────────────────────────────────────────────────────────

def run_gem_finder():
    rid = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log.info(f"═══ Gem Finder v2.0 — {rid} ═══")
    conn = init_db()
    all_posts = []
    counts = {}

    def collect(name, fn):
        log.info(f"[{name}]")
        p = fn(); counts[name] = len(p); all_posts.extend(p)

    collect("Reddit", scan_reddit)
    collect("LinkedIn", scan_linkedin)
    collect("SEC EDGAR", scan_sec_edgar)
    collect("Google News", scan_google_news)
    collect("HackerNews", scan_hackernews)
    collect("Patents", scan_patents)
    collect("GitHub", scan_github)
    collect("Job Signals", scan_jobs)
    collect("Courts", scan_courts)

    total = len(all_posts)
    ss = " | ".join(f"{k}:{v}" for k,v in counts.items() if v > 0)
    log.info(f"Total: {total} | {ss}")

    log.info("Phase 2: Pre-filter...")
    cands = [c for c in pre_filter(all_posts) if not is_seen(conn, c["_hash"])]
    log.info(f"Candidates: {len(cands)}")

    if not cands:
        send_header(0, total, ss); send_no_gems(); conn.close(); return

    log.info(f"Phase 3: Scoring {len(cands)}...")
    scored = score_gems(cands)
    gems = sorted([g for g in scored if g.get("gem_score",0) >= MIN_GEM_SCORE], key=lambda x: x.get("gem_score",0), reverse=True)[:15]
    log.info(f"Phase 3: {len(gems)} scored {MIN_GEM_SCORE}+")

    if not gems:
        send_header(0, total, ss); send_no_gems(); conn.close(); return

    log.info(f"Phase 3.5: Novelty check {len(gems)}...")
    gems = verify_novelty(gems)
    gems = sorted([g for g in gems if g.get("gem_score",0) >= MIN_GEM_SCORE], key=lambda x: x.get("gem_score",0), reverse=True)

    log.info(f"Phase 4: Sending {len(gems)}...")
    send_header(len(gems), total, ss)
    if gems:
        for rank, g in enumerate(gems, 1):
            send_gem(g, rank)
            mark_seen(conn, g["_hash"], g["source"], g.get("gem_ticker",""), g.get("gem_headline",""), g.get("gem_score",0))
            log.info(f"  #{rank}: [{g.get('gem_score')}/10] ${g.get('gem_ticker')} — {g.get('gem_headline','')[:60]}")
    else:
        send_no_gems()

    conn.execute("INSERT INTO run_log VALUES(?,?,?,?,?)", (rid, datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat(), len(gems), total))
    conn.commit(); conn.close()
    log.info(f"═══ Done. {len(gems)} gems. ═══")

if __name__ == "__main__":
    run_gem_finder()
