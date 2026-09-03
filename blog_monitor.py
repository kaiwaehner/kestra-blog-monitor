#!/usr/bin/env python3
"""
Blog Monitor v2
===============
Checks RSS feeds and scrapes blogs without RSS, ranks the new posts with the
Anthropic API, and sends one grouped HTML digest per day.

Changes vs v1
-------------
* Credentials read from environment, never from config.json
* State saved only after the mail was accepted by the SMTP server
* Atomic state writes, per-source pruning, schema v1 -> v2 migration
* First run of a new source is seeded silently instead of mailed
* Feed autodiscovery: scrape sources are promoted to RSS automatically
* Conditional GET via ETag and Last-Modified
* Parallel fetching
* Repeated failures reported once, then only every N days
* Anchor-text titles for scraped posts instead of URL slugs
* Navigation links filtered out, per-run cap against layout changes
* Top News block ranked by Claude, grouped digest by content pillar

Environment
-----------
BLOG_MONITOR_PASSWORD   Gmail app password (required)
ANTHROPIC_API_KEY       Anthropic API key (optional, ranking is skipped without it)
"""

import json
import os
import random
import re
import smtplib
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape, unescape
from html.parser import HTMLParser
from pathlib import Path

import feedparser

socket.setdefaulttimeout(30)

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config.json"
STATE_FILE  = BASE_DIR / "seen_posts.json"
ENV_FILE    = BASE_DIR / ".env"

# ── KAI / KATE brand colors ────────────────────────────────────────────────
MAGENTA = "#C34CC2"
BLACK   = "#111111"
GRAY    = "#888888"
LIGHTBG = "#f7f3f7"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

HEADERS = {
    "User-Agent":      UA,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8,de;q=0.6",
    "Referer":         "https://www.google.com",
}

# Links that look like navigation rather than articles
NOISE_PATTERNS = (
    "/page/", "/pages/", "/category/", "/categories/", "/tag/", "/tags/",
    "/author/", "/authors/", "/topic/", "/topics/", "/archive/", "/archives/",
    "/feed", "/rss", "/search", "/subscribe", "/newsletter", "/sitemap",
    "/privacy", "/legal", "/terms", "/cookie", "/login", "/signup",
    "/contact", "/careers", "/pricing", "/demo", "/wp-content/", "/wp-json/",
)

FEED_CANDIDATES = ("/feed/", "/feed", "/rss.xml", "/rss/", "/index.xml",
                   "/atom.xml", "/feed.xml", "/blog/feed/", "/blog/rss.xml")

CTA_RE = re.compile(
    r"\s*(read|learn|find out|discover|see)\s+more\s*$|\s*weiterlesen\s*$", re.I
)
FEED_LINK_RE = re.compile(r"<link[^>]+application/(?:rss|atom)\+xml[^>]*>", re.I)
HREF_RE      = re.compile(r'href=["\']([^"\']+)["\']', re.I)
TAG_RE       = re.compile(r"<[^>]+>")


# ═══════════════════════════════════════════════════════════════════════════
#  Config, environment, state
# ═══════════════════════════════════════════════════════════════════════════
def load_env():
    """Reads .env into os.environ without overwriting existing values."""
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def load_config():
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


def load_state():
    """Loads state and migrates the v1 schema {name: [ids]} to v2."""
    if not STATE_FILE.exists():
        return {"version": 2, "sources": {}}

    with open(STATE_FILE, encoding="utf-8") as f:
        raw = json.load(f)

    if raw.get("version") == 2:
        return raw

    print("[INFO] Migrating state from v1 to v2")
    migrated = {"version": 2, "sources": {}}
    for name, ids in raw.items():
        if not isinstance(ids, list):
            continue
        # v1 stored ids without normalization. v2 normalizes link-derived ids
        # but keeps feed GUIDs raw, so record both forms. Truncating here would
        # make every dropped id look like a new post on the next run.
        seen, known = [], set()
        for pid in ids:
            if not isinstance(pid, str):
                continue
            forms = [pid]
            if pid.startswith("http"):
                forms.append(normalize_url(pid))
            for form in forms:
                if form and form not in known:
                    known.add(form)
                    seen.append(form)
        migrated["sources"][name] = {
            "seen": seen, "seeded": True, "fail_count": 0,
        }
    total = sum(len(v["seen"]) for v in migrated["sources"].values())
    print(f"[INFO] Migrated {len(migrated['sources'])} source(s), {total} ids")
    return migrated


def save_state(state, keep):
    """Prunes each source, then writes atomically so a crash cannot corrupt it."""
    for entry in state["sources"].values():
        if len(entry.get("seen", [])) > keep:
            entry["seen"] = entry["seen"][-keep:]

    tmp = STATE_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_FILE)


def source_state(state, name):
    return state["sources"].setdefault(
        name, {"seen": [], "seeded": False, "fail_count": 0}
    )


# ═══════════════════════════════════════════════════════════════════════════
#  HTTP helpers
# ═══════════════════════════════════════════════════════════════════════════
def http_get(url, timeout, etag=None, modified=None, max_bytes=6_000_000,
             attempts=3):
    """Returns (status, body_bytes, etag, last_modified). Status 304 means unchanged.

    Retries on rate limits, gateway errors and timeouts. A single daily request
    that gets a 429 is being bot-filtered, not throttled, and those blocks often
    only hit the first attempt.
    """
    headers = dict(HEADERS)
    if etag:
        headers["If-None-Match"] = etag
    if modified:
        headers["If-Modified-Since"] = modified

    last = None
    for attempt in range(attempts):
        if attempt:
            time.sleep(4 * attempt + random.uniform(0, 2))
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read(max_bytes)
                return (resp.status, body,
                        resp.headers.get("ETag"), resp.headers.get("Last-Modified"))
        except urllib.error.HTTPError as e:
            if e.code == 304:
                return 304, b"", etag, modified
            last = e
            if e.code not in (403, 408, 429, 500, 502, 503, 504):
                raise
        except (TimeoutError, socket.timeout, urllib.error.URLError) as e:
            last = e
    raise last


def looks_like_feed(body):
    head = body[:2000].lower()
    return b"<rss" in head or b"<feed" in head or b"<rdf:rdf" in head


def discover_feed(page_url, timeout):
    """Finds a feed for an HTML page: declared link first, then common paths."""
    try:
        status, body, _, _ = http_get(page_url, timeout)
        if status == 200:
            html = body.decode("utf-8", "ignore")
            for tag in FEED_LINK_RE.findall(html):
                m = HREF_RE.search(tag)
                if not m:
                    continue
                candidate = urllib.parse.urljoin(page_url, unescape(m.group(1)))
                try:
                    st, bd, _, _ = http_get(candidate, timeout)
                    if st == 200 and looks_like_feed(bd):
                        return candidate
                except Exception:
                    continue
    except Exception:
        pass

    parts = urllib.parse.urlsplit(page_url)
    root  = f"{parts.scheme}://{parts.netloc}"
    bases = [page_url.rstrip("/")]
    if root not in bases:
        bases.append(root)

    for base in bases:
        for suffix in FEED_CANDIDATES:
            try:
                st, bd, _, _ = http_get(base + suffix, timeout)
                if st == 200 and looks_like_feed(bd):
                    return base + suffix
            except Exception:
                continue
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  Fetchers
# ═══════════════════════════════════════════════════════════════════════════
TRACKING_PREFIXES = (
    "utm_", "swpmtx", "ref=", "fbclid", "gclid", "gclsrc", "mkt_tok",
    "mc_cid", "mc_eid", "_ga", "_gl", "hsa_", "hsctatracking", "msclkid",
    "igshid", "trk=", "trkcampaign", "sfmc", "vero_", "wickedid", "cmpid",
    "campaignid", "s_kwcid", "yclid", "li_fat_id", "epik",
)


def normalize_url(url):
    url = url.split("#")[0]
    base, sep, query = url.partition("?")
    base = base.rstrip("/")
    if not sep:
        return base
    keep = [p for p in query.split("&")
            if p and not p.lower().startswith(TRACKING_PREFIXES)]
    return base + ("?" + "&".join(keep) if keep else "")


def clean_text(value, limit=300):
    text = TAG_RE.sub(" ", value or "")
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def format_date(entry):
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed:
        try:
            return datetime(*parsed[:6]).strftime("%d %b %Y")
        except Exception:
            pass
    return clean_text(entry.get("published") or entry.get("updated") or "", 40)


CONTROL_RE = re.compile(
    rb"[\x00-\x08\x0b\x0c\x0e-\x1f]|&#x?0*(?:[0-8bcefBCEF]|1[0-9a-fA-F]);"
)


def salvage_feed(feed_url, timeout):
    """Some publishers emit XML with stray control characters. Download it,
    strip them, and reparse. Returns None when that does not help either."""
    try:
        status, body, _, _ = http_get(feed_url, timeout)
    except Exception:
        return None
    if status != 200 or not body:
        return None
    cleaned = CONTROL_RE.sub(b"", body)
    try:
        return feedparser.parse(cleaned)
    except Exception:
        return None


def fetch_rss(source, st, timeout):
    """Returns (items, error, meta). items is a list of post dicts."""
    feed_url = st.get("discovered_feed") or source.get("feed_url")
    if not feed_url:
        return [], "no feed_url configured", {}

    feed = feedparser.parse(
        feed_url,
        etag=st.get("etag"),
        modified=st.get("modified"),
        agent=UA,
    )
    status = getattr(feed, "status", None)

    # A permanent or temporary redirect leaves feedparser with no entries, so
    # follow it once and remember the target
    if status in (301, 302, 307, 308) and getattr(feed, "href", None):
        target = feed.href
        if target != feed_url:
            feed = feedparser.parse(target, agent=UA)
            status = getattr(feed, "status", None)
            if feed.entries:
                feed_url = target

    if status == 304:
        return [], None, {"unchanged": True}

    if not feed.entries:
        salvaged = salvage_feed(feed_url, timeout)
        if salvaged is not None and salvaged.entries:
            print(f"[XML ] {source['name']}: malformed feed cleaned up")
            feed = salvaged
        else:
            reason = str(feed.get("bozo_exception", f"no entries (HTTP {status})"))
            return [], reason, {}

    meta = {}
    if feed_url != (st.get("discovered_feed") or source.get("feed_url")):
        meta["discovered_feed"] = feed_url
    if getattr(feed, "etag", None):
        meta["etag"] = feed.etag
    if getattr(feed, "modified", None):
        meta["modified"] = feed.modified

    items = []
    for entry in feed.entries:
        link = entry.get("link") or ""
        pid  = entry.get("id") or normalize_url(link)
        if not pid:
            continue
        items.append({
            "id":        pid,
            "title":     clean_text(entry.get("title", "")) or "No title",
            "link":      link or feed_url,
            "published": format_date(entry),
            "summary":   clean_text(entry.get("summary", ""), 240),
        })
    return items, None, meta


SITEMAP_URL_RE = re.compile(r"<url\b[^>]*>(.*?)</url>", re.S | re.I)
SITEMAP_LOC_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>", re.I)
SITEMAP_MOD_RE = re.compile(r"<lastmod>\s*([^<\s]+)\s*</lastmod>", re.I)
SITEMAP_IDX_RE = re.compile(r"<sitemap\b[^>]*>(.*?)</sitemap>", re.S | re.I)


def parse_sitemap(body):
    """Returns (entries, children). entries are (loc, lastmod) for a urlset,
    children are (loc, lastmod) for a sitemapindex."""
    text = body.decode("utf-8", "ignore")
    if "<sitemapindex" in text[:2000].lower():
        children = []
        for block in SITEMAP_IDX_RE.findall(text):
            loc = SITEMAP_LOC_RE.search(block)
            mod = SITEMAP_MOD_RE.search(block)
            if loc:
                children.append((unescape(loc.group(1)),
                                 mod.group(1) if mod else ""))
        return [], children

    entries = []
    for block in SITEMAP_URL_RE.findall(text):
        loc = SITEMAP_LOC_RE.search(block)
        mod = SITEMAP_MOD_RE.search(block)
        if loc:
            entries.append((unescape(loc.group(1)), mod.group(1) if mod else ""))
    if not entries:   # some sitemaps omit the <url> wrapper
        entries = [(unescape(m), "") for m in SITEMAP_LOC_RE.findall(text)]
    return entries, []


def fetch_sitemap(source, st, timeout):
    """Reads article URLs from a sitemap. Works on blogs that render client-side,
    because a sitemap is static XML that needs no JavaScript."""
    sm_url   = source.get("sitemap_url", "")
    pat      = source.get("path_filter", "")
    max_kids = source.get("max_index_children", 6)

    if not sm_url:
        return [], "no sitemap_url configured", {}

    status, body, etag, modified = http_get(
        sm_url, timeout, etag=st.get("etag"), modified=st.get("modified")
    )
    if status == 304:
        return [], None, {"unchanged": True}

    meta = {}
    if etag:
        meta["etag"] = etag
    if modified:
        meta["modified"] = modified

    entries, children = parse_sitemap(body)

    if children:
        # Prefer children whose own URL hints at the wanted section, then the
        # most recently modified, so a 109-file index costs a handful of requests
        tokens = [t for t in re.split(r"[/\-_]", pat.lower()) if len(t) > 3]

        def score(child):
            loc, mod = child
            low = loc.lower()
            return (-sum(t in low for t in tokens), -_sort_key(mod))

        for loc, _ in sorted(children, key=score)[:max_kids]:
            try:
                st2, body2, _, _ = http_get(loc, timeout)
                if st2 == 200:
                    sub, _ = parse_sitemap(body2)
                    entries.extend(sub)
            except Exception:
                continue
        meta["index_children"] = len(children)

    matched = [(loc, mod) for loc, mod in entries
               if (not pat or pat in loc) and not loc.endswith(".xml")]
    matched.sort(key=lambda e: -_sort_key(e[1]))

    items, rejected = [], 0
    for loc, mod in matched:
        link  = normalize_url(loc)
        title = title_from_slug(link)
        if not looks_like_headline(title, link):
            rejected += 1
            continue
        items.append({
            "id":        link,
            "title":     title,
            "link":      link,
            "published": (mod[:10] if mod else ""),
            "summary":   "",
        })

    if rejected:
        print(f"[FILT] {source['name']}: {rejected} non-article url(s) dropped")
    if not items:
        return [], f"sitemap had no urls matching {pat!r}", meta
    return items, None, meta


def _sort_key(lastmod):
    """Turns an ISO-ish lastmod into a sortable number, 0 when absent.

    Padded to a fixed width so 2026-07-29 does not lose against
    2026-07-28T10:00:00Z just because the latter carries more digits.
    """
    if not lastmod:
        return 0
    digits = re.sub(r"\D", "", lastmod)[:14]
    return int(digits.ljust(14, "0")) if digits else 0


class LinkParser(HTMLParser):
    """Collects hrefs plus their anchor text so titles are readable."""

    def __init__(self, base_url="", link_filter=""):
        super().__init__(convert_charrefs=True)
        self.base_url    = base_url
        self.link_filter = link_filter
        self.links       = {}
        self._href       = None
        self._buf        = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            if self._href is not None:
                self._buf.append(" ")   # keep nested elements from fusing
            return
        href = dict(attrs).get("href", "")
        if not href or href in ("#", "/"):
            self._href = None
            return
        if self.link_filter and self.link_filter not in href:
            self._href = None
            return
        if href.startswith("http"):
            full = href
        elif href.startswith("/"):
            full = self.base_url.rstrip("/") + href
        else:
            self._href = None
            return
        self._href = normalize_url(full)
        self._buf  = []

    def handle_data(self, data):
        if self._href:
            self._buf.append(data)

    def handle_endtag(self, tag):
        if tag != "a":
            if self._href is not None:
                self._buf.append(" ")
            return
        if not self._href:
            return
        text = re.sub(r"\s+", " ", "".join(self._buf)).strip()
        text = CTA_RE.sub("", text).strip(" \u00b7|-\u2192")
        previous = self.links.get(self._href, "")
        if len(text) > len(previous):
            self.links[self._href] = text
        self._href = None
        self._buf  = []


def title_from_slug(url):
    slug = url.rstrip("/").split("/")[-1]
    slug = re.sub(r"\.(html?|php|aspx)$", "", slug, flags=re.I)
    slug = re.sub(r"^\d{4}[-/]\d{2}[-/]\d{2}[-_]?", "", slug)
    return slug.replace("-", " ").replace("_", " ").strip().title() or url


def clean_scraped_title(text):
    """Strips the date, category and author noise many blog index pages inline."""
    t = re.sub(r"^[A-Z]{3,9}\s+\d{1,2},?\s+\d{4}\s*", "", text)   # "JUL 24, 2026 "
    t = re.sub(r"^\d{1,2}\.\s*\w+\s+\d{4}\s*", "", t)             # "24. Juli 2026 "
    t = re.sub(r"\s*\+\d+\s*$", "", t)                            # " +3" co-authors
    t = re.sub(r"\s{2,}", " ", t).strip(" \u00b7|-\u2013\u2014")
    return t or text


def looks_like_headline(title, url):
    """Category and navigation links survive the URL filter, so gate on shape."""
    words = len(re.findall(r"[A-Za-z0-9\u00c0-\u024f]{2,}", title))
    slug  = url.rstrip("/").split("/")[-1]
    if words >= 4:
        return True
    if slug.count("-") >= 3:
        return True
    return False


def is_noise(url, link_filter):
    low = url.lower()
    if any(n in low for n in NOISE_PATTERNS):
        return True
    if link_filter and low.rstrip("/").endswith(link_filter.strip("/").lower()):
        return True
    path = urllib.parse.urlsplit(url).path
    if len([p for p in path.split("/") if p]) < 2:
        return True
    return False


def fetch_scrape(source, st, timeout):
    url = source["url"]
    status, body, etag, modified = http_get(
        url, timeout, etag=st.get("etag"), modified=st.get("modified")
    )
    if status == 304:
        return [], None, {"unchanged": True}

    html   = body.decode("utf-8", "ignore")
    parser = LinkParser(source.get("base_url", ""), source.get("link_filter", "/blog/"))
    parser.feed(html)

    meta = {}
    if etag:
        meta["etag"] = etag
    if modified:
        meta["modified"] = modified

    items, rejected = [], 0
    for link, anchor in parser.links.items():
        if is_noise(link, source.get("link_filter", "")):
            continue
        anchor = clean_scraped_title(anchor)
        title  = anchor if (10 <= len(anchor) <= 200) else title_from_slug(link)
        if not looks_like_headline(title, link):
            rejected += 1
            continue
        items.append({
            "id":        link,
            "title":     title,
            "link":      link,
            "published": "",
            "summary":   "",
        })

    if rejected:
        print(f"[FILT] {source['name']}: {rejected} non-article link(s) dropped")

    if not items:
        return [], "no article links found (page may render client-side)", meta
    return items, None, meta


def process_source(source, st, settings):
    """Runs in a worker thread. Never raises."""
    name    = source["name"]
    timeout = source.get("timeout", settings["request_timeout"])
    result  = {"source": source, "items": [], "error": None,
               "meta": {}, "promoted": None}

    method = source.get("method", "rss")
    try:
        if method == "sitemap":
            items, error, meta = fetch_sitemap(source, st, timeout)
        elif method == "rss" or st.get("discovered_feed"):
            items, error, meta = fetch_rss(source, st, timeout)
        else:
            items, error, meta = fetch_scrape(source, st, timeout)
    except Exception as e:
        items, error, meta = [], f"{type(e).__name__}: {e}", {}

    # Self-healing: a broken feed or an empty scrape triggers autodiscovery.
    # Retried after discovery_retry_days so a site that adds a feed later is
    # picked up, instead of being written off forever.
    retry_days = settings.get("discovery_retry_days", 14)
    last_try   = st.get("discovery_tried")
    due        = True
    if last_try is True:
        due = False
    elif isinstance(last_try, str):
        try:
            due = (datetime.now(timezone.utc).date()
                   - datetime.strptime(last_try, "%Y-%m-%d").date()).days >= retry_days
        except ValueError:
            due = True

    if (error and settings.get("auto_discover_feeds") and method != "sitemap"
            and source.get("discover", True) and due):
        page = source.get("url") or source.get("feed_url", "")
        seed = page
        if method == "rss":
            parts = urllib.parse.urlsplit(page)
            seed = f"{parts.scheme}://{parts.netloc}"
        found = None
        try:
            found = discover_feed(seed, timeout)
        except Exception:
            pass
        meta["discovery_tried"] = datetime.now(timezone.utc).date().isoformat()
        if found and found != (st.get("discovered_feed") or source.get("feed_url")):
            probe = dict(source)
            probe["feed_url"] = found
            try:
                items2, error2, meta2 = fetch_rss(probe, {}, timeout)
                if items2 and not error2:
                    items, error = items2, None
                    meta.update(meta2)
                    meta["discovered_feed"] = found
                    result["promoted"] = found
            except Exception:
                pass

    result["items"] = items
    result["error"] = error
    result["meta"]  = meta
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  Anthropic ranking
# ═══════════════════════════════════════════════════════════════════════════
RANKING_SYSTEM = """You rank blog posts for Kai Waehner.

Who he is: Global Field CTO at Kestra (workflow orchestration) and independent
Advisory Field CTO through his own company. Previously at Talend, TIBCO, and
Confluent. He publishes vendor-neutral technology landscapes and architecture
analysis for enterprise architects, CTOs, CDOs, and senior data engineers.

His four content pillars:
1. Data Integration: event streaming, APIs, batch, CDC, iPaaS, lakehouse
2. Process Intelligence and Workflow Orchestration: process mining, orchestration,
   decision gates, business process automation
3. Trusted Agentic AI: agent governance, vendor lock-in, trust, agent architecture
4. Enterprise Architecture: patterns, trade-offs, migration, build versus buy

RANK HIGH:
- Vendor strategy shifts: acquisitions, funding rounds, pivots, license changes such
  as a move to BSL or SSPL, leadership changes, layoffs, product end-of-life
- Competitive moves in orchestration and process intelligence: Airflow, Astronomer,
  Camunda, Celonis, Dagster, Inngest, Kestra, n8n, Orkes, Prefect, ServiceNow,
  Temporal, UiPath, Windmill
- Architecture deep dives with real numbers: benchmarks, failure post-mortems,
  migration reports, cost analyses, capacity limits
- Named enterprise case studies with concrete architecture, especially manufacturing,
  automotive, telco, financial services, retail, and energy
- Protocols and standards: Kafka protocol, Iceberg, MCP, AMQP, MQTT, OpenTelemetry,
  BPMN, SQL engines
- Regulation and governance with architectural consequences: EU AI Act, data
  sovereignty, audit and lineage requirements
- Anything that would change a vendor's position in one of his landscape reports
- Credible contrarian analysis he could argue with or against

RANK LOW OR LEAVE OUT:
- Beginner tutorials, getting-started guides, listicles, top-N tips
- Event, webinar, and conference announcements without technical substance
- Minor release notes and dot-release changelogs
- Marketing copy, testimonials without architecture detail, we-are-excited posts
- Generic AI hype with no architectural claim
- Certification, training, partner, and hiring announcements
- Award and analyst-recognition press releases

Scoring, where the title and source are all the evidence you have:
5 = would likely change how he writes or advises this week
4 = clearly worth reading today
3 = relevant to a pillar, worth a look
2 = tangential
1 = noise

Judge conservatively. A title that reads like marketing scores low even from a
strong source. Fewer good picks beat filling a quota: returning four items is the
right answer when only four deserve it.

Spread the picks across sources. When one blog publishes a batch on the same day,
score its strongest one or two items and leave the rest out, even if they would
otherwise clear the threshold. A block dominated by a single vendor is less useful
than a block covering several.

Some titles arrive from scraped index pages and carry leftover noise such as a
category name or an author. Judge the article, not the formatting.

Style rules for the reason field, these are strict:
- Maximum 14 words, one clause, no trailing period
- Never use an em dash or an en dash
- Never use the words genuinely, truly, honestly, or substrate
- Say what makes it relevant, not that it is relevant

Return only a JSON array, no prose and no code fences:
[{"id": 12, "score": 5, "reason": "..."}]
Sort by score descending. Include only items scoring at or above the threshold given."""


def rank_posts(posts, cfg):
    """Returns (ranked_list, note). ranked_list holds post dicts with score and reason."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not cfg.get("enabled", True):
        return [], None
    if not api_key or "REPLACE" in api_key.upper() or api_key == "sk-ant-...":
        return [], "no Anthropic API key configured, Top News skipped"
    # Sources marked "rank": false never compete for Top News. Keeping them out
    # of the candidate list also means not paying tokens for titles that could
    # only ever score a 1 against the professional criteria in RANKING_SYSTEM.
    pool = [p for p in posts if p.get("rankable", True)]
    if len(pool) <= 3:
        return [], None

    candidates = pool[: cfg.get("max_candidates", 400)]
    lines = []
    for i, p in enumerate(candidates):
        extra = f" | {p['summary'][:140]}" if p.get("summary") else ""
        lines.append(f"{i}. [{p['pillar']}] [{p['blog']}] {p['title'][:180]}{extra}")

    max_items = cfg.get("max_items", 20)
    min_score = cfg.get("min_score", 3)
    user = (
        f"{len(candidates)} new posts from today. Return at most {max_items} items, "
        f"scoring {min_score} or higher.\n\n" + "\n".join(lines)
    )

    payload = {
        "model": cfg.get("model", "claude-sonnet-5"),
        "max_tokens": cfg.get("max_tokens", 8000),
        "system": RANKING_SYSTEM,
        "messages": [{"role": "user", "content": user}],
    }

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "content-type":      "application/json",
            "x-api-key":         api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=cfg.get("timeout", 120)) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "ignore")[:200]
        return [], f"ranking failed: HTTP {e.code} {detail}"
    except Exception as e:
        return [], f"ranking failed: {type(e).__name__}: {e}"

    text = "".join(
        block.get("text", "") for block in data.get("content", [])
        if block.get("type") == "text"
    ).strip()
    text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.M).strip()

    truncated = data.get("stop_reason") == "max_tokens"

    parsed = None
    match  = re.search(r"\[.*\]", text, re.S)
    if match:
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            parsed = None

    if parsed is None:
        # A response cut off at max_tokens has no closing bracket, so read the
        # complete objects one by one instead of demanding a whole array
        parsed = []
        for obj in re.findall(r"\{[^{}]*\}", text):
            try:
                parsed.append(json.loads(obj))
            except json.JSONDecodeError:
                continue

    if not parsed:
        print(f"[RANK] unparseable response, first 200 chars: {text[:200]!r}")
        return [], ("ranking response was cut off" if truncated
                    else "ranking returned no usable JSON")

    ranked = []
    for item in parsed:
        try:
            idx   = int(item["id"])
            score = int(item.get("score", 0))
        except (KeyError, TypeError, ValueError):
            continue
        if not 0 <= idx < len(candidates) or score < min_score:
            continue
        post = dict(candidates[idx])
        post["score"]  = score
        post["reason"] = clean_text(str(item.get("reason", "")), 160)
        ranked.append(post)

    ranked.sort(key=lambda p: -p["score"])

    # One talkative vendor should not own the block. Keep the best few per
    # source, then fill the remaining slots with what is left.
    per_source = cfg.get("max_per_source", 3)
    counts, primary, overflow = {}, [], []
    for p in ranked:
        blog = p["blog"]
        if counts.get(blog, 0) < per_source:
            counts[blog] = counts.get(blog, 0) + 1
            primary.append(p)
        else:
            overflow.append(p)
    ranked = (primary + overflow)[:max_items]

    usage = data.get("usage", {})
    note  = (f"ranked by {payload['model']}: {usage.get('input_tokens', 0)} in / "
             f"{usage.get('output_tokens', 0)} out tokens")
    if truncated:
        note += ", response was cut off, raise ranking.max_tokens"
    return ranked, note


# ═══════════════════════════════════════════════════════════════════════════
#  HTML email
# ═══════════════════════════════════════════════════════════════════════════
def render_top_news(ranked):
    if not ranked:
        return ""

    rows = ""
    for n, p in enumerate(ranked, 1):
        dots   = "&#9679;" * p["score"] + f"<span style='color:#ddd'>{'&#9679;' * (5 - p['score'])}</span>"
        reason = f"<div style='color:{GRAY};font-size:12px;margin-top:3px'>{escape(p['reason'])}</div>" if p.get("reason") else ""
        rows += f"""
        <tr>
          <td style='vertical-align:top;padding:0 10px 14px 0;color:{MAGENTA};
                     font-size:13px;font-weight:700;width:22px'>{n}</td>
          <td style='vertical-align:top;padding:0 0 14px 0'>
            <a href='{escape(p["link"])}' style='color:{BLACK};font-weight:600;font-size:15px;
               text-decoration:none;line-height:1.35'>{escape(p["title"])}</a>
            <div style='margin-top:4px;font-size:11px;color:{GRAY}'>
              <span style='color:{MAGENTA};font-weight:700'>{escape(p["blog"])}</span>
              &nbsp;&#183;&nbsp; {escape(p["pillar"])}
              &nbsp;&#183;&nbsp; <span style='color:{MAGENTA}'>{dots}</span>
            </div>
            {reason}
          </td>
        </tr>"""

    return f"""
    <div style='border:2px solid {MAGENTA};border-radius:3px;padding:18px 20px 6px;margin-bottom:30px'>
      <div style='font-family:Montserrat,Arial,sans-serif;font-size:15px;font-weight:800;
                  color:{BLACK};letter-spacing:0.5px;text-transform:uppercase;margin-bottom:14px'>
        Top News <span style='color:{MAGENTA}'>({len(ranked)})</span>
      </div>
      <table cellpadding='0' cellspacing='0' style='width:100%;border-collapse:collapse'>{rows}</table>
    </div>"""


def render_pillars(posts, pillar_order):
    by_pillar = {}
    for p in posts:
        by_pillar.setdefault(p["pillar"], {}).setdefault(p["blog"], []).append(p)

    ordered = [x for x in pillar_order if x in by_pillar]
    ordered += sorted(x for x in by_pillar if x not in pillar_order)

    html = ""
    for pillar in ordered:
        blogs = by_pillar[pillar]
        count = sum(len(v) for v in blogs.values())

        blocks = ""
        for blog in sorted(blogs):
            entries = ""
            for p in blogs[blog]:
                pub = (f"<span style='color:{GRAY};font-size:11px'>"
                       f"{escape(p['published'])}</span><br>") if p["published"] else ""
                entries += f"""
                <div style='margin-bottom:9px'>
                  <a href='{escape(p["link"])}' style='color:{BLACK};font-size:14px;
                     font-weight:500;text-decoration:none;line-height:1.35'>{escape(p["title"])}</a><br>
                  {pub}
                </div>"""
            blocks += f"""
            <div style='margin-bottom:16px'>
              <div style='font-size:12px;font-weight:700;color:{MAGENTA};
                          text-transform:uppercase;letter-spacing:0.4px;margin-bottom:6px'>
                {escape(blog)} <span style='color:{GRAY};font-weight:400'>{len(blogs[blog])}</span>
              </div>
              {entries}
            </div>"""

        html += f"""
        <div style='margin-bottom:30px'>
          <div style='border-left:4px solid {MAGENTA};padding-left:12px;margin-bottom:14px'>
            <span style='color:{BLACK};font-size:16px;font-weight:700;
                         font-family:Montserrat,Arial,sans-serif'>{escape(pillar)}</span>
            <span style='color:{MAGENTA};font-size:13px;font-weight:600'> &nbsp;{count}</span>
          </div>
          {blocks}
        </div>"""
    return html


def render_errors(errors):
    if not errors:
        return ""
    rows = ""
    for e in errors:
        rows += f"""
        <div style='margin-bottom:8px'>
          <strong style='color:{BLACK};font-size:12px'>{escape(e['blog'])}</strong>
          <span style='color:{GRAY};font-size:11px'> &#183; {e['fail_count']}x in a row</span><br>
          <span style='font-size:11px;color:{GRAY}'>{escape(e['url'])}</span><br>
          <span style='font-size:11px;color:{MAGENTA}'>{escape(e['error'][:200])}</span>
        </div>"""
    return f"""
    <div style='background:{LIGHTBG};border-left:4px solid {MAGENTA};border-radius:2px;
                padding:16px;margin-top:30px'>
      <strong style='color:{BLACK};font-size:13px'>{len(errors)} source(s) need attention</strong>
      <div style='margin-top:12px'>{rows}</div>
    </div>"""


def build_html_email(posts, ranked, errors, notes, pillar_order, stats):
    date_str = datetime.now().strftime("%d %B %Y")

    # Anything already in Top News is not repeated below
    top_links = {normalize_url(p["link"]) for p in ranked}
    rest      = [p for p in posts if normalize_url(p["link"]) not in top_links]

    if posts:
        summary = (f"<strong style='color:{MAGENTA}'>{len(posts)} new post(s)</strong> "
                   f"across {len({p['blog'] for p in posts})} source(s)")
        if ranked:
            summary += (f"<div style='color:{GRAY};font-size:11px;margin-top:3px'>"
                        f"{len(ranked)} in Top News, {len(rest)} below</div>")
    else:
        summary = "<strong>No new posts</strong>"

    if rest:
        rest_html = f"""
        <div style='font-family:Montserrat,Arial,sans-serif;font-size:12px;font-weight:800;
                    color:{GRAY};letter-spacing:1px;text-transform:uppercase;
                    margin:0 0 14px 0'>
          Everything else <span style='color:{MAGENTA}'>({len(rest)})</span>
        </div>
        {render_pillars(rest, pillar_order)}"""
    elif ranked:
        rest_html = (f"<div style='color:{GRAY};font-size:12px;margin-bottom:20px'>"
                     f"Nothing beyond the Top News today.</div>")
    else:
        rest_html = render_pillars(posts, pillar_order)

    notes_html = ""
    if notes:
        joined = " &#183; ".join(escape(n) for n in notes)
        notes_html = (f"<div style='color:{GRAY};font-size:11px;margin-top:8px'>"
                      f"{joined}</div>")

    return f"""
    <html><body style='font-family:"Open Sans",Arial,sans-serif;max-width:700px;
                       margin:0 auto;padding:32px 24px;color:{BLACK};background:#ffffff'>

      <div style='border-bottom:3px solid {MAGENTA};padding-bottom:18px;margin-bottom:26px'>
        <h1 style='margin:0;color:{BLACK};font-size:26px;font-weight:800;
                   font-family:Montserrat,Arial,sans-serif;letter-spacing:-0.5px'>Blog Monitor</h1>
        <p style='margin:6px 0 0;color:{MAGENTA};font-size:13px;font-weight:600'>{date_str}</p>
      </div>

      {render_top_news(ranked)}

      <div style='background:{LIGHTBG};border-radius:2px;padding:14px 16px;margin-bottom:28px;
                  font-size:13px;color:{BLACK};line-height:1.5'>
        {summary}
        <div style='color:{GRAY};font-size:11px;margin-top:4px'>
          {stats['checked']} sources checked &#183; {stats['unchanged']} unchanged &#183;
          {stats['seeded']} newly seeded &#183; {stats['failed']} failing
        </div>
        {notes_html}
      </div>

      {rest_html}
      {render_errors(errors)}

      <div style='border-top:2px solid {MAGENTA};margin-top:32px;padding-top:14px;
                  color:{GRAY};font-size:11px;text-align:center'>
        Kai Waehner &nbsp;|&nbsp; Blog Monitor v2 &nbsp;|&nbsp; beelink-server, daily at 08:00
      </div>

    </body></html>"""


def send_email(config, subject, html_body):
    cfg      = config["email"]
    sender   = os.environ.get("BLOG_MONITOR_SENDER", cfg["sender"])
    password = os.environ.get("BLOG_MONITOR_PASSWORD")
    if not password:
        raise RuntimeError("BLOG_MONITOR_PASSWORD is not set, see .env")

    msg = MIMEMultipart("alternative")
    msg["From"]    = sender
    msg["To"]      = cfg["recipient"]
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=60) as server:
        server.login(sender, password)
        server.sendmail(sender, cfg["recipient"], msg.as_string())
    print(f"[INFO] Email sent to {cfg['recipient']}")


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════
def should_report(entry, notify_days, today):
    """First failure is reported, then at most once every notify_days."""
    if entry.get("fail_count", 0) == 1:
        return True
    last = entry.get("last_error_notified")
    if not last:
        return True
    try:
        return (today - datetime.strptime(last, "%Y-%m-%d").date()).days >= notify_days
    except ValueError:
        return True


def main():
    load_env()
    config   = load_config()
    settings = config.get("settings", {})
    settings.setdefault("request_timeout", 20)
    settings.setdefault("max_workers", 10)
    settings.setdefault("auto_discover_feeds", True)
    settings.setdefault("scrape_max_new_per_run", 15)
    keep        = settings.get("state_keep_per_source", 250)
    notify_days = settings.get("error_notify_days", 7)
    dry_run     = "--dry-run" in sys.argv

    state   = load_state()
    sources = config["sources"]
    today   = datetime.now(timezone.utc).date()

    stats = {"checked": len(sources), "unchanged": 0, "seeded": 0, "failed": 0}
    new_posts, errors_to_report, notes, promotions = [], [], [], []

    # Deliberately not a `with` block. Leaving one calls shutdown(wait=True),
    # which joins the worker threads, so a source that never returns would block
    # here even though as_completed already gave up on it.
    #
    # A source can go silent rather than fail: it opens a TCP connection and
    # then sends nothing, or sends one byte every so often. request_timeout does
    # not save you, because a socket timeout restarts on every byte received.
    # Two such sources once turned a four minute run into fifty.
    total_budget = settings.get("fetch_total_timeout", 300)
    pool = ThreadPoolExecutor(max_workers=settings["max_workers"])
    futures = {
        pool.submit(process_source, src,
                    dict(source_state(state, src["name"])), settings): src
        for src in sources
    }
    results, finished = [], set()
    try:
        for fut in as_completed(futures, timeout=total_budget):
            finished.add(fut)
            try:
                results.append(fut.result())
            except Exception as e:
                src = futures[fut]
                results.append({"source": src, "items": [],
                                "error": f"worker crashed: {e}", "meta": {},
                                "promoted": None})
    except FuturesTimeout:
        pass

    # Whatever never came back is reported as a failure rather than silently
    # dropped, so it shows up in the mail and in the fail_count.
    for fut, src in futures.items():
        if fut not in finished:
            results.append({"source": src, "items": [],
                            "error": f"no response within {total_budget}s, abandoned",
                            "meta": {}, "promoted": None})

    pool.shutdown(wait=False, cancel_futures=True)

    # Merge single threaded so the state stays consistent
    for res in sorted(results, key=lambda r: r["source"]["name"]):
        src   = res["source"]
        name  = src["name"]
        entry = source_state(state, name)
        entry.update({k: v for k, v in res["meta"].items() if k != "unchanged"})

        if res["promoted"]:
            promotions.append((name, res["promoted"]))
            print(f"[FEED] {name}: promoted to {res['promoted']}")

        if res["error"]:
            entry["fail_count"] = entry.get("fail_count", 0) + 1
            entry["last_error"] = res["error"]
            stats["failed"] += 1
            print(f"[WARN] {name}: {res['error']}")
            # A previously autodiscovered feed that broke should not lock the
            # source out of scraping forever
            if entry.pop("discovered_feed", None):
                entry.pop("discovery_tried", None)
                print(f"[FEED] {name}: discarding stale feed, will retry scraping")
            if should_report(entry, notify_days, today):
                errors_to_report.append({
                    "blog": name,
                    "url": src.get("feed_url") or src.get("url", ""),
                    "error": res["error"],
                    "fail_count": entry["fail_count"],
                })
                entry["last_error_notified"] = today.isoformat()
            continue

        entry["fail_count"] = 0
        entry.pop("last_error", None)
        entry.pop("last_error_notified", None)

        if res["meta"].get("unchanged"):
            stats["unchanged"] += 1
            print(f"[304 ] {name}: unchanged")
            continue

        seen  = set(entry.get("seen", []))
        fresh = [i for i in res["items"] if i["id"] not in seen]

        def merge_seen():
            """Appends only ids that are not tracked yet, preserving order."""
            existing = list(entry.get("seen", []))
            known    = set(existing)
            for item in res["items"]:
                if item["id"] not in known:
                    existing.append(item["id"])
                    known.add(item["id"])
            entry["seen"] = existing

        # A source whose configured endpoint changed produces a completely
        # different set of ids, for example when a scraper becomes a feed. That
        # is not news, so seed it silently instead of mailing the whole archive.
        endpoint = (f"{src.get('method', 'rss')}:"
                    f"{src.get('feed_url') or src.get('sitemap_url') or src.get('url', '')}")
        first_v2 = "feed_key" not in entry

        if not entry.get("seeded") or entry.get("feed_key") != endpoint:
            if first_v2 and entry.get("seeded"):
                why = "first v2 run"
            elif entry.get("seeded"):
                why = "endpoint changed"
            else:
                why = "new source"
            merge_seen()
            entry["seeded"]   = True
            entry["feed_key"] = endpoint
            stats["seeded"] += 1
            print(f"[SEED] {name}: {len(res['items'])} item(s) marked as seen "
                  f"({why}), no mail")
            continue

        cap = settings["scrape_max_new_per_run"]
        if src.get("method") in ("scrape", "sitemap") and len(fresh) > cap:
            print(f"[WARN] {name}: {len(fresh)} new links, capped at {cap} "
                  f"(layout change?)")
            notes.append(f"{name} produced {len(fresh)} new links, showing {cap}")
            fresh = fresh[:cap]

        merge_seen()
        for item in fresh:
            item["blog"]   = name
            item["pillar"] = src.get("pillar", "Uncategorized")
            item["rankable"] = src.get("rank", True)
            new_posts.append(item)

        if fresh:
            print(f"[NEW ] {name}: {len(fresh)}")
        else:
            print(f"[OK  ] {name}: nothing new")

    # Deduplicate syndicated posts across sources
    seen_links, deduped = set(), []
    for p in new_posts:
        key = normalize_url(p["link"])
        if key in seen_links:
            continue
        seen_links.add(key)
        deduped.append(p)
    if len(deduped) < len(new_posts):
        notes.append(f"{len(new_posts) - len(deduped)} duplicate link(s) removed")
    new_posts = deduped

    ranked, rank_note = [], None
    if new_posts:
        ranked, rank_note = rank_posts(new_posts, config.get("ranking", {}))
        if rank_note:
            notes.append(rank_note)
            print(f"[RANK] {rank_note}")

    if promotions:
        notes.append(f"{len(promotions)} feed(s) autodiscovered, "
                     f"move them into config.json")

    if not new_posts and not errors_to_report:
        print("[INFO] Nothing new and nothing to report. No email sent.")
        if not dry_run:
            save_state(state, keep)
        return

    subject = f"Blog Monitor: {len(new_posts)} new post(s)"
    if ranked:
        subject = f"Blog Monitor: {len(ranked)} top of {len(new_posts)} new post(s)"
    if errors_to_report:
        subject += f" ({len(errors_to_report)} alert(s))"

    html = build_html_email(new_posts, ranked, errors_to_report, notes,
                            config.get("pillar_order", []), stats)

    if dry_run:
        out = BASE_DIR / "preview.html"
        out.write_text(html, encoding="utf-8")
        print(f"[DRY ] {subject}")
        print(f"[DRY ] Preview written to {out}, state not saved")
        return

    try:
        send_email(config, subject, html)
    except Exception as e:
        print(f"[ERROR] Email failed, state NOT saved: {e}")
        sys.exit(1)

    save_state(state, keep)


if __name__ == "__main__":
    main()
    # A thread stuck in a socket read cannot be cancelled, and Python joins every
    # non-daemon thread at interpreter exit. Without this the process would hang
    # after the mail was already sent and the state already written, which under
    # an orchestrator means a task that never finishes and eventually times out,
    # reported as a failure despite the work having succeeded.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)
