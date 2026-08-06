"""One-off option-candidate checker for the OptionsWheel skill.

Fetches live quotes, the options chain and historical prices from the
OptionsWheel API, computes the same metrics as OptionsWheel/src/options_wheel,
and prints a JSON report used to explain whether an option is a good or bad
candidate to sell.

Usage:
  python check_option.py --ticker AAPL --type put --list
  python check_option.py --ticker AAPL --type put --expiration 2026-09-18 --strike 290
  python check_option.py --ticker AAPL --type call --expiration 2026-09-18 --strike 315
  python check_option.py --ticker AAPL --news

Environment:
  OW_API_BASE        base URL of the OptionsWheel API (default http://localhost:7071/api)
  OW_SEC_USER_AGENT  User-Agent for SEC EDGAR requests (default StockQuotesStrategy research@stockquotes.example.com)
"""

from __future__ import annotations

import argparse
import email.utils
import json
import math
import os
import re
import sys
import tempfile
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

SEC_UA = os.environ.get(
    "OW_SEC_USER_AGENT", "StockQuotesStrategy research@stockquotes.example.com"
)
SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_TICKER_MAP_CACHE = os.path.join(tempfile.gettempdir(), "sec_company_tickers.json")
SEC_FILING_FORMS = ("8-K", "10-K", "10-Q", "S-4", "DEF 14A", "13D", "13G", "144")
NEWS_RSS_URL = "https://news.google.com/rss/search"
FOMC_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
TE_CALENDAR_URL = "https://tradingeconomics.com/calendar"
BROWSER_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
MACRO_CACHE_TTL = 24 * 3600
FOMC_CACHE = os.path.join(tempfile.gettempdir(), "sqs_fomc_dates.json")
BLS_CACHE = os.path.join(tempfile.gettempdir(), "sqs_bls_releases.json")

_MONTH_NAMES = {
    m: i for i, m in enumerate(
        ["january", "february", "march", "april", "may", "june",
         "july", "august", "september", "october", "november", "december"],
        start=1,
    )
}
_MACRO_BLS_NAMES = {
    "Employment Situation": "Nonfarm Payrolls / Employment Situation",
    "Consumer Price Index": "CPI",
    "Producer Price Index": "PPI",
}
_MACRO_TAIL_SKIP = re.compile(r"\s*(?:,\s*\d{4}\b|-\s*\d{1,2}\s*,\s*\d{4}\b)")
_TE_EVENT_MAP = {
    "non farm payrolls": "Nonfarm Payrolls / Employment Situation",
    "nonfarm payrolls": "Nonfarm Payrolls / Employment Situation",
    "cpi": "CPI",
    "cpi s.a": "CPI",
    "inflation rate yoy": "CPI",
    "ppi": "PPI",
    "ppi yoy": "PPI",
}

QUOTE_FIELDS_CORE = (
    "symbol,shortName,regularMarketPrice,trailingPE,"
    "averageDailyVolume3Month,marketCap,trailingAnnualDividendYield,"
    "fiftyDayAverage,earningsTimestamp,earningsTimestampStart,"
    "earningsTimestampEnd,dividendDate,trailingAnnualDividendRate"
)
# Fields the OptionsWheel quote endpoint actually supports (quote module only).
QUOTE_FIELDS_CONSENSUS = (
    "regularMarketPrice,beta,fiftyTwoWeekHigh,fiftyTwoWeekLow,"
    "sharesOutstanding,trailingPE,marketCap"
)


def find_options_wheel_src(start=SCRIPT_DIR):
    """Locate OptionsWheel/src by walking up the directory tree."""
    cur = os.path.abspath(start)
    while True:
        candidate = os.path.join(cur, "OptionsWheel", "src")
        if os.path.isdir(os.path.join(candidate, "options_wheel")):
            return candidate
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent


def api_base():
    return os.environ.get("OW_API_BASE", "http://localhost:7071/api").rstrip("/")


def http_get_json(url, timeout=30, retries=3, headers=None):
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout, headers=headers or {})
            if resp.status_code == 200:
                return resp.json()
            last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except requests.RequestException as e:
            last_err = str(e)
        time.sleep(0.6 * (attempt + 1))
    raise RuntimeError(f"API request failed: {url} ({last_err})")


def http_get_text(url, timeout=30, retries=3, headers=None):
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout, headers=headers or {})
            if resp.status_code == 200:
                return resp.text
            last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except requests.RequestException as e:
            last_err = str(e)
        time.sleep(0.6 * (attempt + 1))
    raise RuntimeError(f"Request failed: {url} ({last_err})")


def fetch_quote(symbol):
    base = api_base()
    fields = QUOTE_FIELDS_CORE
    url = f"{base}/yahoo-finance?symbols={symbol}&fields={fields}"
    data = http_get_json(url)
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"No quote found for symbol {symbol}")
    item = next((x for x in data if str(x.get("symbol", "")).upper() == symbol.upper()), data[0])
    return item


def fetch_consensus_quote(symbol):
    """Analyst/consensus fields, fetched separately (API caps at 20 fields/request)."""
    base = api_base()
    url = f"{base}/yahoo-finance?symbols={symbol}&fields={QUOTE_FIELDS_CONSENSUS}"
    data = http_get_json(url)
    if not isinstance(data, list) or not data:
        return {}
    return next((x for x in data if str(x.get("symbol", "")).upper() == symbol.upper()), data[0])


def fetch_quote_full(symbol):
    quote = fetch_quote(symbol)
    try:
        quote.update(fetch_consensus_quote(symbol))
    except Exception:
        pass
    return quote


def fetch_chain(symbol, option_type, expiration_date=None):
    base = api_base()
    api_filter = "calls" if option_type == "call" else "puts"
    url = f"{base}/yahoo-finance-stock-options?ticker={symbol}&filter={api_filter}&limit=50"
    if expiration_date:
        url += f"&expirationDate={expiration_date}"
    return http_get_json(url, timeout=45)


def fetch_history(symbol):
    base = api_base()
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    url = f"{base}/yahoo-finance-historical?ticker={symbol}&from={from_date}&to={to_date}&interval=1d"
    data = http_get_json(url, timeout=45)
    if isinstance(data, dict):
        quotes = data.get("quotes") or data.get("prices") or []
    else:
        quotes = data or []
    if len(quotes) < 50:
        raise RuntimeError(f"Insufficient historical data for {symbol}: {len(quotes)} bars")
    return quotes


def fetch_ticker_cik_map():
    """Ticker -> CIK map from SEC, cached in the temp dir (24h TTL)."""
    import time as _time

    if os.path.exists(SEC_TICKER_MAP_CACHE):
        age = _time.time() - os.path.getmtime(SEC_TICKER_MAP_CACHE)
        if age < 86400:
            with open(SEC_TICKER_MAP_CACHE, "r", encoding="utf-8") as fh:
                return json.load(fh)
    data = http_get_json(SEC_TICKER_MAP_URL, timeout=45, headers={"User-Agent": SEC_UA})
    with open(SEC_TICKER_MAP_CACHE, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    return data


def lookup_cik(symbol):
    mapping = fetch_ticker_cik_map()
    target = symbol.upper()
    for entry in mapping.values():
        if str(entry.get("ticker", "")).upper() == target:
            return str(entry["cik_str"])
    return None


def fetch_sec_filings(symbol, max_age_days=180, limit=10):
    """Recent material filings (8-K, 10-Q/10-K, S-4, 13D/13G, 144) from EDGAR."""
    cik = lookup_cik(symbol)
    if not cik:
        return {"cik": None, "filings": []}
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    data = http_get_json(url, timeout=45, headers={"User-Agent": SEC_UA})
    recent = ((data.get("filings") or {}).get("recent") or {})
    forms = recent.get("form") or []
    filed_dates = recent.get("filingDate") or []
    accessions = recent.get("accessionNumber") or []
    documents = recent.get("primaryDocument") or []
    descriptions = recent.get("primaryDocDescription") or []
    now = datetime.now(timezone.utc)
    out = []
    for i, form in enumerate(forms):
        if form not in SEC_FILING_FORMS:
            continue
        try:
            filed = datetime.strptime(filed_dates[i], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (ValueError, IndexError):
            continue
        if (now - filed).days > max_age_days:
            continue
        desc = descriptions[i] if i < len(descriptions) and descriptions[i] else form
        link = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{accessions[i].replace('-', '')}/{documents[i]}"
        )
        out.append({"form": form, "filed": filed.strftime("%Y-%m-%d"), "description": desc, "link": link})
        if len(out) >= limit:
            break
    return {"cik": cik, "filings": out}


def fetch_news(symbol, max_items=10):
    """Recent headlines from Google News RSS for the ticker."""
    qs = urllib.parse.urlencode(
        {"q": f"{symbol} stock", "hl": "en-US", "gl": "US", "ceid": "US:en"}
    )
    text = http_get_text(f"{NEWS_RSS_URL}?{qs}", timeout=45)
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        return {"error": f"failed to parse news feed: {e}", "news": []}
    items = []
    for item in root.iter("item"):
        title = item.findtext("title")
        if not title:
            continue
        pub = item.findtext("pubDate")
        date = None
        if pub:
            try:
                date = email.utils.parsedate_to_datetime(pub).astimezone(timezone.utc).strftime("%Y-%m-%d")
            except (TypeError, ValueError):
                date = pub
        source_el = item.find("source")
        items.append(
            {
                "title": title,
                "date": date,
                "source": source_el.text if source_el is not None else None,
                "link": item.findtext("link"),
            }
        )
        if len(items) >= max_items:
            break
    return {"news": items}


def _cached_json(cache_path, ttl, loader):
    """Read cache_path if fresh (< ttl seconds), otherwise load and rewrite it."""
    if os.path.exists(cache_path):
        try:
            age = time.time() - os.path.getmtime(cache_path)
            if age < ttl:
                with open(cache_path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
        except (OSError, ValueError):
            pass
    data = loader()
    try:
        with open(cache_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
    except OSError:
        pass
    return data


def fetch_fomc_dates():
    """Scheduled FOMC meetings (Fed rate decisions) from the Fed's official
    calendar page. Uses the last day of each meeting (decision/statement day).
    Covers the current and next calendar year; cached 24h in the OS temp dir.
    """

    def load():
        html = http_get_text(FOMC_URL, timeout=20, retries=1,
                             headers={"User-Agent": BROWSER_UA})
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)
        headings = sorted(
            ((int(m.group(1)), m.start(), m.end())
             for m in re.finditer(r"(\d{4}) FOMC Meetings", text)),
            key=lambda x: x[1],
        )
        this_year = datetime.now(timezone.utc).year
        rx = re.compile(r"\b([A-Z][a-z]{2,8})\s+(\d{1,2})(?!\d)\s*(?:-\s*(\d{1,2})(?!\d))?")
        out = []
        for idx, (year, _start, epos) in enumerate(headings):
            if year not in (this_year, this_year + 1):
                continue
            seg_end = headings[idx + 1][1] if idx + 1 < len(headings) else len(text)
            seg = text[epos:seg_end]
            cut = seg.find("Back to Top")
            if cut != -1:
                seg = seg[:cut]
            for dm in rx.finditer(seg):
                if _MACRO_TAIL_SKIP.match(seg[dm.end():]):
                    continue
                month = _MONTH_NAMES.get(dm.group(1).lower())
                if month is None:
                    continue
                day = int(dm.group(3)) if dm.group(3) else int(dm.group(2))
                try:
                    date = datetime(year, month, day).date()
                except ValueError:
                    continue
                out.append(date)
        out.sort()
        uniq = []
        for d in out:
            if d not in uniq[-1:]:
                uniq.append(d)
        return [{"name": "FOMC / Fed rate decision", "date": d.isoformat(),
                 "source": "federalreserve.gov"} for d in uniq]

    return _cached_json(FOMC_CACHE, MACRO_CACHE_TTL, load)


def fetch_bls_releases():
    """Scheduled BLS releases for the current year (CPI, Employment Situation,
    PPI) from the official release schedule. Cached 24h in the OS temp dir.
    Uses the per-year HTML page (the .asp and .ics mirrors are bot-blocked).
    """

    def load():
        headers = {"User-Agent": BROWSER_UA}
        html = None
        this_year = datetime.now(timezone.utc).year
        for year in (this_year, this_year - 1):
            url = f"https://www.bls.gov/schedule/{year}/home.htm"
            try:
                html = http_get_text(url, timeout=20, retries=2, headers=headers)
                break
            except Exception:
                continue
        if html is None:
            raise RuntimeError("BLS release schedule unavailable")
        row_re = re.compile(
            r'<td class="date-cell"><p>([^<]+)</p></td>\s*'
            r'<td class="time-cell"><p>([^<]+)</p></td>\s*'
            r'<td class="desc-cell"><p><strong>([^<]+)</strong>'
        )
        out = []
        for m in row_re.finditer(html):
            name = m.group(3).strip()
            short = _MACRO_BLS_NAMES.get(name)
            if short is None:
                continue
            dm = re.search(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", m.group(1))
            if not dm:
                continue
            month = _MONTH_NAMES.get(dm.group(1).lower())
            if month is None:
                continue
            try:
                date = datetime(int(dm.group(3)), month, int(dm.group(2))).date()
            except ValueError:
                continue
            out.append({"name": short, "date": date.isoformat(),
                        "time": m.group(2).strip(), "source": "bls.gov"})
        out.sort(key=lambda e: e["date"])
        return out

    return _cached_json(BLS_CACHE, MACRO_CACHE_TTL, load)


def fetch_macro_events_te():
    """Fallback CPI / NFP / PPI schedule from tradingeconomics (near-term US
    calendar window). Used only when BLS is unreachable.
    """
    html = http_get_text(TE_CALENDAR_URL, timeout=30, retries=1,
                         headers={"User-Agent": BROWSER_UA})
    row_re = re.compile(r'<tr\b[^>]*data-country="united states"[^>]*>')
    out = []
    seen = set()
    for m in row_re.finditer(html):
        seg = html[m.start():m.start() + 1600]
        dm = re.search(r"class='\s*(\d{4}-\d{2}-\d{2})", seg)
        ev = re.search(r'data-event="([^"]*)"', seg)
        if not dm or not ev:
            continue
        name = ev.group(1).strip().lower()
        short = _TE_EVENT_MAP.get(name)
        if short is None:
            continue
        key = (short, dm.group(1))
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": short, "date": dm.group(1), "source": "tradingeconomics.com"})
    out.sort(key=lambda e: e["date"])
    return out


def fetch_macro_events(expiration_dt):
    """Scheduled macro events between today and expiration (Fed rate decisions,
    CPI, Nonfarm Payrolls, PPI). Best-effort per source: a source failure is
    recorded in ``errors`` without failing the report. CPI/NFP/PPI fall back to
    the tradingeconomics calendar when BLS is unreachable.
    """
    exp_date = expiration_dt.date() if hasattr(expiration_dt, "date") else expiration_dt
    today = datetime.now(timezone.utc).date()
    events = []
    errors = {}

    def collect(fn):
        for e in fn():
            ev_date = _parse_iso_dt(e["date"]).date()
            if today <= ev_date <= exp_date:
                events.append({
                    "name": e["name"],
                    "date": e["date"],
                    "time": e.get("time"),
                    "days_before_expiry": (exp_date - ev_date).days,
                    "source": e.get("source"),
                })

    for label, fn in (("fomc", fetch_fomc_dates), ("bls", fetch_bls_releases)):
        try:
            collect(fn)
        except Exception as exc:
            errors[label] = str(exc)[:120]
    if "bls" in errors:
        try:
            collect(fetch_macro_events_te)
            errors["bls"] += " (used tradingeconomics fallback)"
        except Exception as exc:
            errors["te"] = str(exc)[:120]
    events.sort(key=lambda e: (e["date"], e["name"]))
    return {"count": len(events), "events": events, "errors": errors}


def _parse_js_kv_block(text, start_key):
    """Extract a flat JS object literal like priceTargets:{k:v,k:v} and return a dict."""
    start = text.find(f"{start_key}:")
    if start < 0:
        return None
    brace = text.find("{", start)
    if brace < 0:
        return None
    depth = 0
    end = None
    for i in range(brace, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        return None
    body = text[brace + 1 : end]
    out = {}
    for m in re.finditer(r'([A-Za-z_][A-Za-z0-9_]*)\s*:\s*("[^"]*"|[\d.]+)', body):
        key, val = m.group(1), m.group(2)
        if val.startswith('"'):
            out[key] = val.strip('"')
        elif "." in val:
            out[key] = float(val)
        else:
            out[key] = int(val)
    return out


def fetch_analyst_consensus_stockanalysis(symbol):
    """Fallback analyst consensus from stockanalysis.com (no API key).

    Data is embedded in the page's JS state as flat objects
    (priceTargets:{...}, currentRatings:{...}). Best-effort: returns None if
    the page cannot be fetched or parsed.
    """
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    url = f"https://stockanalysis.com/stocks/{symbol.lower()}/forecast/"
    text = http_get_text(url, timeout=45, headers=headers)
    price_targets = _parse_js_kv_block(text, "priceTargets")
    ratings = _parse_js_kv_block(text, "currentRatings")
    if not price_targets and not ratings:
        return None
    out = {"source": "stockanalysis.com"}
    if price_targets:
        out["price_target"] = {
            "avg": price_targets.get("avg"),
            "median": price_targets.get("median"),
            "low": price_targets.get("low"),
            "high": price_targets.get("high"),
            "num_analysts": price_targets.get("numPriceTargets"),
        }
    if ratings:
        out["rating"] = {
            "consensus": ratings.get("consensus"),
            "score": ratings.get("score"),
            "count": ratings.get("count"),
            "strong_buy": ratings.get("strongBuy"),
            "buy": ratings.get("buy"),
            "hold": ratings.get("hold"),
            "sell": ratings.get("sell"),
            "strong_sell": ratings.get("strongSel"),
        }
    return out


def fetch_analyst_consensus_api(symbol):
    """Analyst consensus from the OptionsWheel API (yahoo-finance-summary endpoint).

    Uses quoteSummary modules financialData / defaultKeyStatistics /
    recommendationTrend. Returns None if the endpoint is unavailable.
    """
    url = f"{api_base()}/yahoo-finance-summary?ticker={symbol}"
    data = http_get_json(url, timeout=45)
    if not isinstance(data, dict):
        return None

    fin = data.get("financialData") or {}
    stat = data.get("defaultKeyStatistics") or {}
    trend_list = (data.get("recommendationTrend") or {}).get("trend") or []
    trend = trend_list[0] if trend_list else {}

    out = {"source": "yahoo finance"}
    if fin:
        out["price_target"] = {
            "avg": _safe_round(fin.get("targetMeanPrice")),
            "median": _safe_round(fin.get("targetMedianPrice")),
            "low": _safe_round(fin.get("targetLowPrice")),
            "high": _safe_round(fin.get("targetHighPrice")),
            "num_analysts": fin.get("numberOfAnalystOpinions"),
        }
        out["rating"] = {
            "consensus": fin.get("recommendationKey"),
            "score": _safe_round(fin.get("recommendationMean"), 2),
            "count": sum(
                _to_float(trend.get(k)) or 0
                for k in ("strongBuy", "buy", "hold", "sell", "strongSell")
            ) or None,
            "strong_buy": trend.get("strongBuy"),
            "buy": trend.get("buy"),
            "hold": trend.get("hold"),
            "sell": trend.get("sell"),
            "strong_sell": trend.get("strongSell"),
        }
    if stat:
        out["short_interest"] = {
            "shares_short": stat.get("sharesShort"),
            "short_ratio": _safe_round(stat.get("shortRatio"), 2),
            "short_percent_of_float": _safe_round(stat.get("shortPercentOfFloat"), 4),
        }
        out["valuation"] = {
            "revenue_growth": _safe_round(fin.get("revenueGrowth"), 4),
            "gross_margins": _safe_round(fin.get("grossMargins"), 4),
            "profit_margins": _safe_round(stat.get("profitMargins"), 4),
            "forward_pe": _safe_round(stat.get("forwardPE"), 2),
            "peg_ratio": _safe_round(stat.get("pegRatio"), 2),
            "price_to_book": _safe_round(stat.get("priceToBook"), 2),
            "held_percent_institutions": _safe_round(stat.get("heldPercentInstitutions"), 4),
            "held_percent_insiders": _safe_round(stat.get("heldPercentInsiders"), 4),
            "beta": _safe_round(stat.get("beta"), 3),
        }
    if not fin and not stat:
        return None
    return out


def fetch_analyst_consensus(symbol):
    """Analyst consensus, preferring the local API, falling back to stockanalysis.com."""
    try:
        consensus = fetch_analyst_consensus_api(symbol)
        if consensus:
            return consensus
    except Exception:
        pass
    return fetch_analyst_consensus_stockanalysis(symbol)


def research_events(symbol):
    """News, SEC filings, analyst consensus and quote metrics for a ticker (--news mode)."""
    report = {
        "symbol": symbol,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "quote_metrics": None,
        "analyst_consensus": None,
        "news": None,
        "sec_filings": None,
    }
    try:
        quote = fetch_quote_full(symbol)
        spot = _to_float(quote.get("regularMarketPrice"))
        report["quote_metrics"] = {
            "current_price": _safe_round(spot),
            "beta": _safe_round(quote.get("beta"), 3),
            "fifty_two_week_high": _safe_round(quote.get("fiftyTwoWeekHigh")),
            "fifty_two_week_low": _safe_round(quote.get("fiftyTwoWeekLow")),
            "distance_from_52w_high_pct": _safe_round(
                ((spot - _to_float(quote.get("fiftyTwoWeekHigh"))) / _to_float(quote.get("fiftyTwoWeekHigh"))) * 100
                if spot and _to_float(quote.get("fiftyTwoWeekHigh")) else None
            ),
            "shares_outstanding": quote.get("sharesOutstanding"),
            "trailing_pe": _safe_round(quote.get("trailingPE"), 2),
            "market_cap": quote.get("marketCap"),
            "avg_volume_3m": quote.get("averageDailyVolume3Month"),
        }
    except Exception as e:
        report["quote_metrics"] = {"error": str(e)}
    try:
        report["analyst_consensus"] = fetch_analyst_consensus(symbol)
    except Exception as e:
        report["analyst_consensus"] = {"error": str(e)}
    try:
        report["sec_filings"] = fetch_sec_filings(symbol)
    except Exception as e:
        report["sec_filings"] = {"error": str(e)}
    try:
        report["news"] = fetch_news(symbol)
    except Exception as e:
        report["news"] = {"error": str(e)}
    return report


def _to_float(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _safe_round(v, digits=2):
    f = _to_float(v)
    return round(f, digits) if f is not None and math.isfinite(f) else None


def _parse_iso_dt(value):
    from options_wheel.analysis import _parse_expiration
    return _parse_expiration(value)


def _parse_bar_date(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        try:
            return datetime.strptime(v[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def backtest_realized(quotes, strike, option_type, dte, net_premium=None, min_samples=6):
    """Realized historical check for the same strike + tenor.

    Walks the daily bars treating each bar as a potential expiry, replicating a
    short option opened ``dte`` calendar days earlier. Reports how often the
    strike was breached during the window and how often the option was ITM /
    unprofitable at expiry. If ``net_premium`` is given, also reports the
    break-even win rate (expiry at/through strike +/- premium). Ignores premium
    when measuring the strike-based rates, so prefer ``breakeven_win_rate_pct``
    when comparing to the model's probability of profit. Returns None if fewer
    than ``min_samples`` windows are available.
    """
    bars = []
    for q in quotes:
        d = _parse_bar_date(q.get("date"))
        close = _to_float(q.get("close"))
        if d is None or close is None:
            continue
        low = _to_float(q.get("low"))
        high = _to_float(q.get("high"))
        bars.append(
            {
                "date": d,
                "low": low if low is not None else close,
                "high": high if high is not None else close,
                "close": close,
            }
        )
    bars.sort(key=lambda b: b["date"])
    if len(bars) < min_samples + 2:
        return None

    be = strike
    if net_premium is not None:
        be = strike - net_premium if option_type == "put" else strike + net_premium

    total = touched = itm = be_win = 0
    dtes = []
    for e in range(1, len(bars)):
        expiry_date = bars[e]["date"]
        entry_date = expiry_date - timedelta(days=dte)
        s = None
        for idx in range(e):
            if bars[idx]["date"] >= entry_date:
                s = idx
                break
        if s is None or s >= e:
            continue
        actual_days = (expiry_date - bars[s]["date"]).days
        if actual_days < 0.5 * dte:
            continue
        window = bars[s : e + 1]
        lo = min(b["low"] for b in window)
        hi = max(b["high"] for b in window)
        exp_close = bars[e]["close"]
        if option_type == "put":
            hit = lo <= strike
            itm_now = exp_close < strike
            be_win_now = exp_close > be
        else:
            hit = hi >= strike
            itm_now = exp_close > strike
            be_win_now = exp_close < be
        total += 1
        dtes.append(actual_days)
        if hit:
            touched += 1
        if itm_now:
            itm += 1
        if be_win_now:
            be_win += 1

    if total < min_samples:
        return None
    out = {
        "samples": total,
        "avg_dte_actual": _safe_round(sum(dtes) / len(dtes)),
        "breach_rate_pct": _safe_round(touched / total * 100, 1),
        "expiry_itm_rate_pct": _safe_round(itm / total * 100, 1),
    }
    if net_premium is not None:
        out["breakeven_win_rate_pct"] = _safe_round(be_win / total * 100, 1)
    return out


def fetch_earnings_gap(symbol, quotes):
    """Realized close-to-close move around recent earnings, from the API summary.

    Uses the quoteSummary ``earnings`` module for past report dates, then
    measures the next-trading-day close vs the pre-report close on the daily
    bars. Returns None if there are too few reports or no price data.
    """
    url = f"{api_base()}/yahoo-finance-summary?ticker={symbol}&modules=earnings"
    data = http_get_json(url, timeout=45)
    chart = ((data.get("earnings") or {}).get("earningsChart") or {})
    quarterly = chart.get("quarterly") or []
    if not quarterly:
        return None
    report_dates = []
    for item in quarterly:
        dt = _parse_iso_dt(item.get("reportedDate"))
        if dt is not None:
            report_dates.append(dt.date())

    bars = []
    for q in quotes:
        d = _parse_bar_date(q.get("date"))
        c = _to_float(q.get("close"))
        if d is not None and c is not None:
            bars.append((d, c))
    bars.sort(key=lambda x: x[0])
    if len(bars) < 10 or not report_dates:
        return None

    gaps = []
    for rd in report_dates:
        pre_idx = None
        for i, (d, _) in enumerate(bars):
            if d <= rd:
                pre_idx = i
            else:
                break
        if pre_idx is None or pre_idx + 1 >= len(bars):
            continue
        pre = bars[pre_idx][1]
        post = bars[pre_idx + 1][1]
        if pre and pre > 0:
            gaps.append((post - pre) / pre * 100)

    if len(gaps) < 2:
        return None
    abs_gaps = [abs(g) for g in gaps]
    surprises = [(_to_float(item.get("surprisePct")) or 0.0) for item in quarterly]
    surprises = [s for s in surprises if s]
    return {
        "count": len(gaps),
        "avg_abs_move_pct": _safe_round(sum(abs_gaps) / len(abs_gaps), 2),
        "max_abs_move_pct": _safe_round(max(abs_gaps), 2),
        "avg_signed_move_pct": _safe_round(sum(gaps) / len(gaps), 2),
        "avg_surprise_pct": _safe_round(sum(surprises) / len(surprises), 2) if surprises else None,
    }


def list_expirations_and_strikes(symbol, option_type):
    chain = fetch_chain(symbol, option_type)
    spot = _to_float((chain.get("quote") or {}).get("regularMarketPrice"))
    expiration_dates = chain.get("expirationDates") or []
    strikes = chain.get("strikes") or []
    now = datetime.now(timezone.utc)
    future = []
    for raw in expiration_dates:
        dt = _parse_iso_dt(raw)
        if dt is None:
            continue
        dte = max(0, (dt.date() - now.date()).days)
        if dte >= 0:
            future.append({"date": dt.strftime("%Y-%m-%d"), "dte": dte})
    future.sort(key=lambda x: (x["dte"], x["date"]))
    next_ten = future[:10]

    suggestion = {"atm": _safe_round(spot), "strikes": []}
    if spot and strikes:
        ordered = sorted(strikes)
        atm = min(ordered, key=lambda s: abs(s - spot))
        idx = ordered.index(atm)
        lo = max(0, idx - 5)
        hi = min(len(ordered), idx + 6)
        suggestion["strikes"] = [float(s) for s in ordered[lo:hi]]

    return {
        "symbol": symbol,
        "option_type": option_type,
        "spot": _safe_round(spot),
        "expirations": next_ten,
        "suggested_strikes": suggestion,
    }


def analyze_contract(symbol, option_type, expiration_date, strike):
    from options_wheel.analysis import (
        IV_HISTORY_STORE,
        _dte_from_expiration,
        _extract_next_earnings_dt,
        compute_iv_hv_percentile,
        extract_atm_iv,
        fetch_historical_indicators,
    )
    from options_wheel.metrics import (
        collateral_per_share,
        credit_risk_ratio,
        expected_itm_payoff,
        forecast_volatility,
        net_credit,
        option_delta,
        probability_of_profit,
        sigma_distance,
        simple_yields,
        variance_risk_premium,
        years_to_expiration,
    )

    now_dt = datetime.now(timezone.utc)
    expiration_dt = _parse_iso_dt(expiration_date)
    if expiration_dt is None:
        raise RuntimeError(f"Cannot parse expiration date: {expiration_date}")
    expiration_dt = expiration_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    quote = fetch_quote_full(symbol)
    chain = fetch_chain(symbol, option_type, expiration_date=expiration_date)
    spot = _to_float(quote.get("regularMarketPrice"))
    if not spot:
        spot = _to_float((chain.get("quote") or {}).get("regularMarketPrice"))
    if not spot:
        raise RuntimeError(f"No spot price for {symbol}")

    strike = float(strike)
    chain_list = chain.get("options") or []
    contracts = []
    if chain_list:
        key = "calls" if option_type == "call" else "puts"
        items = chain_list[0].get(key) or []
        for item in items:
            if _to_float(item.get("strike")) == strike:
                contracts.append((expiration_dt, item))
    if not contracts:
        avail = sorted({_to_float(i.get("strike")) for c in chain_list for i in (c.get("calls") or []) + (c.get("puts") or []) if _to_float(i.get("strike"))})
        raise RuntimeError(
            f"Strike {strike} not found for {symbol} {option_type} on {expiration_date}. "
            f"Available strikes: {avail[:30]}"
        )

    _, option = contracts[0]
    bid = _to_float(option.get("bid"))
    ask = _to_float(option.get("ask"))
    implied_vol = _to_float(option.get("impliedVolatility"))
    last_price = _to_float(option.get("lastPrice"))
    open_interest = int(_to_float(option.get("openInterest")) or 0)
    volume = int(_to_float(option.get("volume")) or 0)
    api_delta = _to_float(option.get("delta"))

    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        premium = last_price
        net_premium = last_price
        spread_abs = None
        spread_pct = None
    else:
        premium, net_premium, spread_pct = net_credit(
            bid, ask, commission_per_contract=0.65, slippage_pct_of_spread=30.0, round_trip=False
        )
        spread_abs = ask - bid

    dividend_yield = _to_float(quote.get("trailingAnnualDividendYield")) or 0.0
    trailing_div_rate = _to_float(quote.get("trailingAnnualDividendRate")) or 0.0
    risk_free = 0.045

    dte = _dte_from_expiration(expiration_dt, now_dt)
    t_years = years_to_expiration(expiration_dt, now_dt)

    indicators = fetch_historical_indicators(symbol)

    hv_current = indicators.get("hv_current") if indicators else None
    hv_long = indicators.get("hv_long") if indicators else None
    hv_high = indicators.get("hv_high") if indicators else None
    hv_low = indicators.get("hv_low") if indicators else None
    drift_raw = indicators.get("realized_drift") if indicators else None
    drift = max(-0.25, min(0.25, drift_raw)) if drift_raw is not None else 0.0

    if api_delta is None:
        delta = option_delta(spot, strike, implied_vol, t_years, risk_free, dividend_yield, option_type=option_type)
    else:
        delta = api_delta

    forecast_vol = forecast_volatility(implied_vol, hv_current, hv_long, hv_weight=0.5, iv_haircut=0.85)
    vrp_ratio = variance_risk_premium(implied_vol, forecast_vol)
    expected_loss = expected_itm_payoff(spot, strike, forecast_vol, t_years, drift, option_type=option_type)
    pop = probability_of_profit(spot, strike, forecast_vol, t_years, drift, net_premium, option_type=option_type)
    sigma_dist = sigma_distance(spot, strike, forecast_vol, t_years, option_type=option_type)
    risk_ratio = credit_risk_ratio(net_premium, expected_loss)
    ev = None
    if net_premium is not None and expected_loss is not None:
        ev = net_premium - expected_loss
    collateral = collateral_per_share(strike, spot, net_premium, option_type=option_type)
    monthly_yield, annualized_yield = simple_yields(net_premium, collateral, dte)

    if option_type == "call":
        otm_pct = ((strike - spot) / spot) * 100
    else:
        otm_pct = ((spot - strike) / spot) * 100

    # Events ---------------------------------------------------------------
    next_earnings_dt = _extract_next_earnings_dt(quote, now_dt)
    earnings_before_expiry = (
        next_earnings_dt is not None
        and next_earnings_dt.date() <= expiration_dt.date()
        and next_earnings_dt >= now_dt
    )
    ex_div_dt = _parse_iso_dt(quote.get("dividendDate"))
    ex_div_risk = "NONE"
    if ex_div_dt is not None:
        ex_div_dt = ex_div_dt.astimezone(timezone.utc)
        if now_dt.date() <= ex_div_dt.date() <= expiration_dt.date():
            dividend_amount = trailing_div_rate / 4.0 if trailing_div_rate > 0 else 0.0
            if option_type == "call":
                extrinsic = (premium or 0.0) - max(spot - strike, 0.0)
                ex_div_risk = "HIGH" if dividend_amount > 0 and extrinsic < dividend_amount else "MEDIUM"
            else:
                ex_div_risk = "DIVIDEND_DROP"

    # IV rank / IV-HV percentile -------------------------------------------
    iv_rank = iv_percentile = iv_obs = None
    iv_hv_percentile = None
    atm_iv = None
    try:
        payload = {"options": [chain_list[0]]} if chain_list else {"options": []}
        contracts_all = []
        for c in (payload.get("options") or []):
            for item in (c.get("puts") or []) + (c.get("calls") or []):
                contracts_all.append((expiration_dt, item))
        atm_iv = extract_atm_iv(contracts_all, spot, now_dt=now_dt, dte_fn=_dte_from_expiration)
        key = f"{symbol}|{option_type}"
        iv_rank, iv_percentile, iv_obs = IV_HISTORY_STORE.rank(key, atm_iv)
    except Exception:
        pass
    if implied_vol is not None and hv_high is not None and hv_low is not None:
        iv_hv_percentile = compute_iv_hv_percentile(implied_vol, hv_low, hv_high)

    # IV rank fallback: the store is empty until enough scans accumulate, so use
    # the realised-HV-based percentile as a proxy when rank is unavailable.
    iv_rank_fallback = False
    if iv_rank is None and iv_hv_percentile is not None:
        iv_rank = iv_hv_percentile
        iv_percentile = iv_hv_percentile
        iv_rank_fallback = True

    # Realized backtest + earnings gap (best-effort; never fail the report).
    bars = None
    try:
        bars = fetch_history(symbol)
    except Exception:
        bars = None
    backtest = None
    if bars:
        try:
            backtest = backtest_realized(bars, strike, option_type, dte, net_premium=net_premium)
        except Exception:
            backtest = None
    earnings_gap = None
    if bars:
        try:
            earnings_gap = fetch_earnings_gap(symbol, bars)
        except Exception:
            earnings_gap = None

    # Macro events (FOMC, CPI, jobs report, PPI) inside the contract window.
    macro_events = None
    try:
        macro_events = fetch_macro_events(expiration_dt)
    except Exception:
        macro_events = {"count": 0, "events": [], "errors": {"macro": "unexpected error"}}

    report = {
        "symbol": symbol,
        "name": quote.get("shortName", ""),
        "option_type": option_type,
        "spot": _safe_round(spot),
        "strike": strike,
        "expiration": expiration_dt.strftime("%Y-%m-%d"),
        "dte": dte,
        "quote": {
            "bid": _safe_round(bid),
            "ask": _safe_round(ask),
            "last_price": _safe_round(last_price),
            "premium_mid": _safe_round(premium),
            "net_premium": _safe_round(net_premium),
            "spread_pct": _safe_round(spread_pct),
            "spread_abs": _safe_round(spread_abs),
            "open_interest": open_interest,
            "volume": volume,
            "implied_volatility": _safe_round(implied_vol, 4),
            "api_delta": _safe_round(api_delta, 3),
            "delta_bs": _safe_round(delta, 3),
            "moneyness_otm_pct": _safe_round(otm_pct),
        },
        "consensus": {
            "beta": _safe_round(quote.get("beta"), 3),
            "fifty_two_week_high": _safe_round(quote.get("fiftyTwoWeekHigh")),
            "fifty_two_week_low": _safe_round(quote.get("fiftyTwoWeekLow")),
            "shares_outstanding": quote.get("sharesOutstanding"),
            "runway_from_52w_high_pct": _safe_round(
                ((_to_float(quote.get("fiftyTwoWeekHigh")) - spot) / spot) * 100
                if spot and _to_float(quote.get("fiftyTwoWeekHigh")) else None
            ),
        },
        "events": {
            "next_earnings": next_earnings_dt.strftime("%Y-%m-%d") if next_earnings_dt else None,
            "earnings_before_expiry": earnings_before_expiry,
            "ex_dividend_date": ex_div_dt.strftime("%Y-%m-%d") if ex_div_dt else None,
            "ex_div_risk": ex_div_risk,
            "dividend_yield": _safe_round(dividend_yield, 4),
            "earnings_gap": earnings_gap,
            "macro_events": macro_events,
        },
        "technicals": {
            key: _safe_round(indicators.get(key)) if indicators else None
            for key in ("ema50", "rsi", "adx", "rvi", "macd", "signal", "price")
        },
        "volatility": {
            "hv_current": _safe_round(hv_current, 4),
            "hv_long": _safe_round(hv_long, 4),
            "hv_high": _safe_round(hv_high, 4),
            "hv_low": _safe_round(hv_low, 4),
            "forecast_vol": _safe_round(forecast_vol, 4),
            "vrp_ratio": _safe_round(vrp_ratio, 3),
            "atm_iv": _safe_round(atm_iv, 4),
            "iv_rank": _safe_round(iv_rank, 3),
            "iv_percentile": _safe_round(iv_percentile, 3),
            "iv_rank_fallback": iv_rank_fallback,
            "iv_hv_percentile": _safe_round(iv_hv_percentile, 3),
            "realized_drift": _safe_round(drift_raw, 4),
        },
        "backtest": backtest,
        "returns_risk": {
            "collateral_per_share": _safe_round(collateral),
            "monthly_yield_pct": _safe_round(monthly_yield),
            "annualized_yield_pct": _safe_round(annualized_yield),
            "probability_of_profit_pct": _safe_round(pop * 100 if pop is not None else None, 1),
            "expected_loss_per_share": _safe_round(expected_loss),
            "ev_per_share": _safe_round(ev),
            "sigma_distance": _safe_round(sigma_dist, 3),
            "credit_risk_ratio": _safe_round(risk_ratio, 3),
        },
    }
    return report


def main():
    parser = argparse.ArgumentParser(description="OptionsWheel candidate checker")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--type", choices=["put", "call"], default="put")
    parser.add_argument("--list", action="store_true", help="List next expirations and suggested strikes")
    parser.add_argument("--news", action="store_true", help="Research news, SEC filings and analyst consensus")
    parser.add_argument("--expiration", default=None, help="Expiration date YYYY-MM-DD")
    parser.add_argument("--strike", type=float, default=None)
    args = parser.parse_args()

    src = find_options_wheel_src()
    if src is None:
        sys.stderr.write("ERROR: cannot locate OptionsWheel/src (options_wheel module).\n")
        sys.exit(2)
    if src not in sys.path:
        sys.path.insert(0, src)

    if args.news:
        result = research_events(args.ticker)
    elif args.list:
        result = list_expirations_and_strikes(args.ticker, args.type)
    else:
        if not args.expiration or args.strike is None:
            sys.stderr.write("ERROR: --expiration and --strike are required unless --list is used.\n")
            sys.exit(2)
        result = analyze_contract(args.ticker, args.type, args.expiration, args.strike)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
