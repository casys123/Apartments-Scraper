# app.py
# Multifamily Lead Finder — v2 (Apartments.com + RentCafe → CSV/XLSX/Google Sheets)
# Key fixes in v2:
# - Much more robust link collection from listing pages (adds JSON-LD ItemList parsing)
# - Better anti-bot handling + human/captcha detection
# - Optional manual paste of property URLs if listing pages are blocked
# - Rotating User-Agents + optional Referer
# - Clear diagnostics when 0 links found
#
# Features kept from v1:
# - Extract: Property Name, Address, Management Company, Phone, Email, URLs
# - Enrichment: follow management site to find public email/phone
# - Messaging: per-row Call Script + Email Subject/Body
# - Export: CSV/XLSX, Google Sheets
#
# Usage:
#   pip install streamlit requests beautifulsoup4 lxml pandas xlsxwriter urllib3 gspread google-auth
#   streamlit run app.py

import re
import os
import io
import json
import time
import html
import random
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import streamlit as st

# Optional: Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_GSHEETS = True
except Exception:
    HAS_GSHEETS = False

# ----------------------------
# Streamlit Page Setup
# ----------------------------
st.set_page_config(page_title="Multifamily Lead Finder v2", page_icon="🏢", layout="wide")
st.title("🏢 Multifamily Lead Finder — v2")

st.write(
    "Scrape **Apartments.com** and **RentCafe** for multifamily leads (Property, Address, Management, Phone/Email).\n"
    "Includes enrichment from management sites, message templates, and export to CSV/XLSX/Google Sheets.\n\n"
    "If listing pages are blocked, use **Manual URLs** mode to paste property links you gathered in your browser."
)

# ----------------------------
# Helpers
# ----------------------------
UA_ROTATE = [
    # A few modern desktop UAs
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
]

DEFAULT_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Cache-Control": "no-cache",
}

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(r"(?:(?:\+?1[\s\-\.]?)?(?:\(?\d{3}\)?[\s\-\.]?)\d{3}[\s\-\.]?\d{4})", re.MULTILINE | re.DOTALL)

CAPTCHA_PATTERNS = [
    re.compile(r"are you human|captcha|verif(y|ication)|unusual traffic", re.I),
    re.compile(r"Just a moment\.", re.I), # common CDN interstitial
]

def make_session(referer: str = "") -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers.update(DEFAULT_HEADERS)
    s.headers["User-Agent"] = random.choice(UA_ROTATE)
    if referer:
        s.headers["Referer"] = referer
    return s

def polite_sleep(min_s: float, max_s: float):
    time.sleep(random.uniform(min_s, max_s))

def looks_blocked(text: str) -> bool:
    t = text[:5000]  # check first chunk
    for pat in CAPTCHA_PATTERNS:
        if pat.search(t):
            return True
    return False

def safe_get(session: requests.Session, url: str, timeout: int = 25):
    try:
        session.headers["User-Agent"] = random.choice(UA_ROTATE)
        resp = session.get(url, timeout=timeout)
        if resp is None:
            return None
        if resp.status_code in (403, 429):
            polite_sleep(1.0, 2.0)
            session.headers["User-Agent"] = random.choice(UA_ROTATE)
            resp = session.get(url, timeout=timeout)
        return resp
    except requests.RequestException:
        return None

def clean_text(x: str) -> str:
    if not x:
        return ""
    return " ".join(html.unescape(x).split())

def first_nonempty(*args):
    for a in args:
        if a and str(a).strip():
            return clean_text(str(a))
    return ""

# ----------------------------
# JSON-LD utilities (helps when listing pages are JS-heavy)
# ----------------------------

def parse_json_ld_nodes(soup: BeautifulSoup) -> List[dict]:
    nodes = []
    for tag in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(tag.get_text(strip=True))
        except Exception:
            continue
        if isinstance(data, dict):
            nodes.append(data)
        elif isinstance(data, list):
            nodes.extend([d for d in data if isinstance(d, dict)])
    # flatten @graph
    flat = []
    for d in nodes:
        if isinstance(d, dict) and "@graph" in d and isinstance(d["@graph"], list):
            flat.extend([g for g in d["@graph"] if isinstance(g, dict)])
        else:
            flat.append(d)
    return flat


def extract_itemlist_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    links = []
    for node in parse_json_ld_nodes(soup):
        if node.get("@type") in ("ItemList", ["ItemList"]):
            elems = node.get("itemListElement") or []
            for e in elems:
                try:
                    # formats vary: e may be dict with item.url or url
                    if isinstance(e, dict):
                        item = e.get("item") if isinstance(e.get("item"), dict) else e
                        url = item.get("url") or item.get("@id") or e.get("url")
                        if url:
                            full = urljoin(base_url, url)
                            links.append(full.split("?")[0].rstrip("/"))
                except Exception:
                    pass
    # de-dup
    out = []
    seen = set()
    for u in links:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

# ----------------------------
# Site-specific: Apartments.com
# ----------------------------

def apts_build_listing_urls(city: str, state: str, pages: int) -> List[str]:
    base = f"https://www.apartments.com/{city.strip().lower().replace(' ', '-')}-{state.strip().lower()}/"
    urls = []
    for p in range(1, pages + 1):
        if p == 1:
            urls.append(base)
        else:
            urls.append(f"{base}?page={p}")
            urls.append(urljoin(base, f"{p}/"))
    # de-dup
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def apts_collect_property_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    links = set()
    # 1) Try common anchors on modern pages
    selectors = [
        "a.property-link",
        'a[data-test-id*="property-card-link"]',
        'a[data-tile-track*="PropertyCard"]',
        'a[href*="/property/"]',
        'a[href*="/apartments/"]',
    ]
    for sel in selectors:
        for a in soup.select(sel):
            href = a.get("href", "")
            if not href:
                continue
            full = urljoin(base_url, href)
            if "apartments.com" in urlparse(full).netloc:
                links.add(full.split("?")[0].rstrip("/"))

    # 2) Fallback: parse JSON-LD ItemList (works even when cards are JS-rendered)
    for u in extract_itemlist_links(soup, base_url):
        if "apartments.com" in urlparse(u).netloc:
            links.add(u)

    return list(links)


def apts_is_property_detail(soup: BeautifulSoup) -> bool:
    # Heuristics: "Managed by" label, floor-plan widget, or ApartmentComplex JSON-LD
    if soup.find(string=re.compile(r"Managed by", re.I)):
        return True
    if soup.select_one('[data-testid*="floor-plan"]'):
        return True
    for node in parse_json_ld_nodes(soup):
        t = node.get("@type")
        if isinstance(t, str) and ("Apartment" in t or "Place" in t):
            return True
        if isinstance(t, list) and any("Apartment" in x or "Place" in x for x in t):
            return True
    return False


def apts_extract_details(session: requests.Session, url: str, follow_mgmt: bool, delay_min: float, delay_max: float) -> Optional[Dict]:
    r = safe_get(session, url, 25)
    if not (r and r.ok):
        return None
    if looks_blocked(r.text):
        st.warning("Apartments.com property page appears blocked by anti-bot. Try Manual URLs mode or increase delays.")
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    if not apts_is_property_detail(soup):
        return None

    name = ""; address = ""; mgmt = ""; phone = ""; email = ""; mgmt_url = ""

    # Name
    h1 = soup.find(["h1", "h2"], string=True)
    if h1:
        name = clean_text(h1.get_text())

    # Address
    addr_tag = soup.find(attrs={"data-testid": re.compile(r"property-address|address", re.I)}) or \
               soup.find("address") or \
               soup.find("div", class_=re.compile(r"property-address|address", re.I))
    if addr_tag:
        address = clean_text(addr_tag.get_text())

    # Phone
    tel = soup.select_one('a[href^="tel:"]')
    if tel:
        phone = clean_text(tel.get_text() or tel.get("href", "").replace("tel:", ""))
    if not phone:
        m = PHONE_RE.search(soup.get_text(" ", strip=True))
        if m:
            phone = clean_text(m.group(0))

    # Management + mgmt_url
    label = soup.find(string=re.compile(r"Managed by", re.I))
    if label:
        block = label.parent if hasattr(label, "parent") else None
        if block:
            link = block.find("a", href=True) or (block.find_next("a", href=True) if block else None)
            if link:
                mgmt = first_nonempty(mgmt, link.get_text())
                href = link["href"]
                mgmt_url = href if href.startswith("http") else urljoin(url, href)
            else:
                mgmt = first_nonempty(mgmt, block.get_text())

    # Follow management site
    if follow_mgmt and mgmt_url:
        polite_sleep(delay_min, delay_max)
        r2 = safe_get(session, mgmt_url, 25)
        if r2 and r2.ok and not looks_blocked(r2.text):
            s2 = BeautifulSoup(r2.text, "html.parser")
            if not email:
                m = EMAIL_RE.search(s2.get_text(" ", strip=True))
                if m:
                    email = m.group(0)
            if not phone:
                t2 = s2.select_one('a[href^="tel:"]')
                if t2:
                    phone = clean_text(t2.get_text() or t2.get("href", "").replace("tel:", ""))
            if not email:
                mlinks = [a.get("href") for a in s2.select('a[href^="mailto:"]')]
                mlinks = [x.replace("mailto:", "") for x in mlinks if x]
                if mlinks:
                    email = clean_text(mlinks[0])

    return {
        "Property Name": name,
        "Address": address,
        "Management Company": mgmt,
        "Phone": phone,
        "Email": email,
        "Source URL": url,
        "Mgmt URL": mgmt_url,
        "Source": "Apartments.com",
    }

# ----------------------------
# Site-specific: RentCafe
# ----------------------------

def rentcafe_build_listing_urls(city: str, state: str, pages: int) -> List[str]:
    base = f"https://www.rentcafe.com/apartments-for-rent/us/{state.strip().lower()}/{city.strip().lower().replace(' ', '-')}/"
    return [base if p == 1 else f"{base}?page={p}" for p in range(1, pages + 1)]


def rentcafe_collect_property_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    links = set()
    selectors = [
        "a.card-title, a.property-title, a.btn-details, .property-name a, .js-CommunityName a",
        'a[href*="/apartments/"]',
    ]
    for sel in selectors:
        for a in soup.select(sel):
            href = a.get("href", "")
            if not href:
                continue
            full = urljoin(base_url, href)
            if "rentcafe.com" in urlparse(full).netloc:
                links.add(full.split("?")[0].rstrip("/"))

    # JSON-LD ItemList fallback
    for u in extract_itemlist_links(soup, base_url):
        if "rentcafe.com" in urlparse(u).netloc:
            links.add(u)

    return list(links)


def rentcafe_is_property_detail(soup: BeautifulSoup) -> bool:
    if soup.find(string=re.compile(r"Managed by|Management", re.I)):
        return True
    if soup.select_one(".community-details, .community-header, #communityName"):
        return True
    for node in parse_json_ld_nodes(soup):
        t = node.get("@type")
        if isinstance(t, str) and ("Apartment" in t or "Place" in t):
            return True
        if isinstance(t, list) and any("Apartment" in x or "Place" in x for x in t):
            return True
    return False


def rentcafe_extract_details(session: requests.Session, url: str, follow_mgmt: bool, delay_min: float, delay_max: float) -> Optional[Dict]:
    r = safe_get(session, url, 25)
    if not (r and r.ok):
        return None
    if looks_blocked(r.text):
        st.warning("RentCafe property page appears blocked by anti-bot. Try Manual URLs mode or increase delays.")
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    if not rentcafe_is_property_detail(soup):
        return None

    name = ""; address = ""; mgmt = ""; phone = ""; email = ""; mgmt_url = ""

    name_tag = soup.select_one("#communityName, h1, .community-header h1")
    if name_tag:
        name = clean_text(name_tag.get_text())

    addr_tag = soup.select_one(".community-address, .address, address")
    if addr_tag:
        address = clean_text(addr_tag.get_text())

    tel = soup.select_one('a[href^="tel:"]')
    if tel:
        phone = clean_text(tel.get_text() or tel.get("href", "").replace("tel:", ""))
    if not phone:
        m = PHONE_RE.search(soup.get_text(" ", strip=True))
        if m:
            phone = clean_text(m.group(0))

    mgmt_label = soup.find(string=re.compile(r"Managed by|Management", re.I))
    if mgmt_label:
        block = mgmt_label.parent if hasattr(mgmt_label, "parent") else None
        if block:
            link = block.find("a", href=True) or (block.find_next("a", href=True) if block else None)
            if link:
                mgmt = first_nonempty(mgmt, link.get_text())
                href = link["href"]
                mgmt_url = href if href.startswith("http") else urljoin(url, href)
            else:
                mgmt = first_nonempty(mgmt, block.get_text())

    if follow_mgmt and mgmt_url:
        polite_sleep(delay_min, delay_max)
        r2 = safe_get(session, mgmt_url, 25)
        if r2 and r2.ok and not looks_blocked(r2.text):
            s2 = BeautifulSoup(r2.text, "html.parser")
            if not email:
                m = EMAIL_RE.search(s2.get_text(" ", strip=True))
                if m:
                    email = m.group(0)
            if not phone:
                t2 = s2.select_one('a[href^="tel:"]')
                if t2:
                    phone = clean_text(t2.get_text() or t2.get("href", "").replace("tel:", ""))
            if not email:
                mlinks = [a.get("href") for a in s2.select('a[href^="mailto:"]')]
                mlinks = [x.replace("mailto:", "") for x in mlinks if x]
                if mlinks:
                    email = clean_text(mlinks[0])

    return {
        "Property Name": name,
        "Address": address,
        "Management Company": mgmt,
        "Phone": phone,
        "Email": email,
        "Source URL": url,
        "Mgmt URL": mgmt_url,
        "Source": "RentCafe",
    }

# ----------------------------
# Generic enrichment (Entrata/Yardi/etc.)
# ----------------------------

def generic_enrich_site(session: requests.Session, url: str) -> Dict[str, str]:
    info = {"email": "", "phone": ""}
    r = safe_get(session, url, 25)
    if not (r and r.ok) or looks_blocked(r.text):
        return info
    s = BeautifulSoup(r.text, "html.parser")
    m = EMAIL_RE.search(s.get_text(" ", strip=True))
    if m:
        info["email"] = clean_text(m.group(0))
    else:
        mailtos = [a.get("href") for a in s.select('a[href^="mailto:"]')]
        mailtos = [x.replace("mailto:", "") for x in mailtos if x]
        if mailtos:
            info["email"] = clean_text(mailtos[0])
    if not info["phone"]:
        tel = s.select_one('a[href^="tel:"]')
        if tel:
            info["phone"] = clean_text(tel.get_text() or tel.get("href", "").replace("tel:", ""))
        else:
            m2 = PHONE_RE.search(s.get_text(" ", strip=True))
            if m2:
                info["phone"] = clean_text(m2.group(0))
    return info

# ----------------------------
# Messaging helpers
# ----------------------------

def build_call_script(property_name: str, address: str, mgmt: str) -> str:
    opener = f"Hi, this is Luis with Miami Master Flooring. Is the property manager available for {property_name or 'your community'}?"
    value = (
        "We specialize in **fast, cost-effective flooring replacements** for multifamily turns — "
        "SPC waterproof vinyl, LVT glue-down, and carpet tile. We handle quick turnarounds and "
        "keep units rent-ready."
    )
    proof = "We work across Miami-Dade & Broward and can provide references and insurance on request."
    ask = "Could we schedule a quick walkthrough or let me send pricing options for your upcoming turns?"
    close = "What’s the best email to send our pricing sheet and availability?"
    parts = [opener, value, proof, ask, close]
    if address:
        parts.insert(1, f"(I’m calling about the community at {address}.)")
    if mgmt:
        parts.insert(0, f"Hi {mgmt} team" if " " not in mgmt else f"Hi {mgmt},")
    return "\n\n".join(parts)


def build_email_template(property_name: str, address: str, mgmt: str) -> Dict[str, str]:
    subj = f"{property_name or 'Your Community'} — Fast, budget-friendly flooring turns (SPC/LVT/Carpet Tile)"
    body_lines = []
    greet = f"Hi {mgmt}," if mgmt else "Hi there,"
    body_lines.append(greet)
    body_lines.append("")
    body_lines.append(
        "We help multifamily communities in Miami-Dade & Broward with **fast, cost-effective flooring replacements** "
        "(SPC waterproof vinyl, LVT glue-down, carpet tile). Our crews handle turnarounds quickly to keep units rent-ready."
    )
    if property_name or address:
        body_lines.append(f"Ref: **{property_name or 'your community'}** — {address}")
    body_lines.append("")
    body_lines.append("**Why us**")
    body_lines.append("• Quick scheduling + reliable crews")
    body_lines.append("• Competitive pricing and high-durability materials")
    body_lines.append("• Licensed & insured")
    body_lines.append("")
    body_lines.append("I’d love to **send pricing options** or walk a unit/building this week.")
    body_lines.append("What’s the best email/phone for the community manager?")
    body_lines.append("")
    body_lines.append("Thanks,")
    body_lines.append("Luis Gonzalez\nMiami Master Flooring\ninfo@miamimasterflooring.com | (305) 555-0123")
    return {"subject": subj, "body": "\n".join(body_lines)}

# ----------------------------
# Google Sheets (Service Account)
# ----------------------------

def get_gs_client_from_secrets():
    if not HAS_GSHEETS:
        return None, "gspread/google-auth not installed"
    try:
        svc_info = st.secrets.get("gcp_service_account", None)
        if not svc_info:
            return None, "Missing st.secrets['gcp_service_account']"
        if isinstance(svc_info, str):
            svc_info = json.loads(svc_info)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(svc_info, scopes=scopes)
        client = gspread.authorize(creds)
        return client, None
    except Exception as e:
        return None, str(e)


def append_to_sheet(df: pd.DataFrame, spreadsheet_id: str, worksheet_name: str) -> str:
    client, err = get_gs_client_from_secrets()
    if err or client is None:
        return f"Google Sheets auth error: {err}"
    try:
        sh = client.open_by_key(spreadsheet_id)
    except Exception:
        return "Could not open spreadsheet. Check the Spreadsheet ID and share access with the service account email."

    try:
        try:
            ws = sh.worksheet(worksheet_name)
        except Exception:
            ws = sh.add_worksheet(title=worksheet_name, rows="1000", cols="26")
        existing = ws.get_all_values()
        if not existing:
            ws.append_row(list(df.columns))
        rows = df.astype(str).values.tolist()
        for r in rows:
            ws.append_row(r)
        return f"Appended {len(rows)} rows to '{worksheet_name}'."
    except Exception as e:
        return f"Append failed: {e}"

# ----------------------------
# UI Controls
# ----------------------------
mode = st.radio("Choose Mode", ["City & State", "Full Search URL", "Manual Property URLs"], horizontal=True)

col_a, col_b, col_c = st.columns([2, 1, 1])
city = state = search_url = ""
manual_urls_text = ""

if mode == "City & State":
    city = col_a.text_input("City", value="Miami")
    state = col_b.text_input("State (2-letter)", value="FL", max_chars=2)
elif mode == "Full Search URL":
    search_url = st.text_input("Search URL (Apartments.com or RentCafe)", value="https://www.apartments.com/miami-fl/")
else:
    manual_urls_text = st.text_area("Paste property detail URLs (one per line)", height=160, placeholder="https://www.apartments.com/property/xyz...\nhttps://www.rentcafe.com/apartments/...\n...")

pages = st.number_input("Max listing pages to crawl (per source)", min_value=1, max_value=50, value=3, step=1)
max_props = st.number_input("Max properties to process (safety cap)", min_value=1, max_value=3000, value=400, step=25)

st.markdown("**Sources to scan** (ignored in Manual URLs mode)")
src1, src2 = st.columns(2)
use_apartments = src1.checkbox("Apartments.com", value=True)
use_rentcafe   = src2.checkbox("RentCafe", value=True)

follow_mgmt = st.checkbox("Follow 'Managed by' / management site to hunt public email/phone", value=True)

delay_min, delay_max = st.slider("Per-request random delay (seconds)", 0.2, 3.0, (0.8, 1.8), step=0.1)

referer_hint = st.text_input("Optional Referer header (can slightly reduce blocks)", value="https://www.google.com/")

go = st.button("🚀 Start Scan")

# Session storage
if "results" not in st.session_state:
    st.session_state["results"] = pd.DataFrame(
        columns=[
            "Property Name","Address","Management Company","Phone","Email",
            "Source URL","Mgmt URL","Source","Call Script","Email Subject","Email Body"
        ]
    )

# ----------------------------
# Crawl helpers per source
# ----------------------------

def scan_apartments(session: requests.Session, city: str, state: str, pages: int, max_props: int,
                    follow_mgmt: bool, delay_min: float, delay_max: float, base_url_override: str = "") -> List[Dict]:
    results = []
    if base_url_override:
        base = base_url_override if base_url_override.endswith("/") else base_url_override + "/"
        listing_urls = []
        for p in range(1, pages + 1):
            listing_urls.append(base if p == 1 else f"{base}?page={p}")
            if p > 1:
                listing_urls.append(urljoin(base, f"{p}/"))
        seen = set(); listing_urls = [u for u in listing_urls if not (u in seen or seen.add(u))]
    else:
        listing_urls = apts_build_listing_urls(city, state, pages)

    st.info(f"Apartments.com: scanning up to {len(listing_urls)} listing pages…")
    prop_links = []
    progress = st.progress(0)
    for i, lu in enumerate(listing_urls, start=1):
        polite_sleep(delay_min, delay_max)
        session.headers["Referer"] = referer_hint or ""
        r = safe_get(session, lu)
        if not (r and r.ok):
            continue
        if looks_blocked(r.text):
            st.warning("Apartments.com listing page looks blocked by anti-bot. Try smaller pages, increase delay, or Manual URLs mode.")
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        links = apts_collect_property_links(soup, lu)
        prop_links.extend(links)
        progress.progress(min(i/len(listing_urls), 1.0))

    prop_links = list(dict.fromkeys(prop_links))
    if not prop_links:
        st.warning("Apartments.com: no property links found (likely blocked or HTML changed). Try Manual URLs mode or a different city.")
        return results

    st.write(f"🔎 Apartments.com candidates: **{len(prop_links)}**")
    detail_prog = st.progress(0)
    for idx, url in enumerate(prop_links[:max_props], start=1):
        st.caption(f"Apartments.com [{idx}/{min(len(prop_links), max_props)}]: {url}")
        polite_sleep(delay_min, delay_max)
        row = apts_extract_details(session, url, follow_mgmt, delay_min, delay_max)
        if row and (row["Property Name"] or row["Address"]):
            results.append(row)
        detail_prog.progress(min(idx/max_props, 1.0))
    return results


def scan_rentcafe(session: requests.Session, city: str, state: str, pages: int, max_props: int,
                  follow_mgmt: bool, delay_min: float, delay_max: float, base_url_override: str = "") -> List[Dict]:
    results = []
    if base_url_override:
        listing_urls = [base_url_override if p == 1 else f"{base_url_override}?page={p}" for p in range(1, pages + 1)]
    else:
        listing_urls = rentcafe_build_listing_urls(city, state, pages)

    st.info(f"RentCafe: scanning up to {len(listing_urls)} listing pages…")
    prop_links = []
    progress = st.progress(0)
    for i, lu in enumerate(listing_urls, start=1):
        polite_sleep(delay_min, delay_max)
        session.headers["Referer"] = referer_hint or ""
        r = safe_get(session, lu)
        if not (r and r.ok):
            continue
        if looks_blocked(r.text):
            st.warning("RentCafe listing page looks blocked by anti-bot. Try smaller pages, increase delay, or Manual URLs mode.")
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        prop_links.extend(rentcafe_collect_property_links(soup, lu))
        progress.progress(min(i/len(listing_urls), 1.0))

    prop_links = list(dict.fromkeys(prop_links))
    if not prop_links:
        st.warning("RentCafe: no property links found (likely blocked or HTML changed). Try Manual URLs mode or a different city.")
        return results

    st.write(f"🔎 RentCafe candidates: **{len(prop_links)}**")
    detail_prog = st.progress(0)
    for idx, url in enumerate(prop_links[:max_props], start=1):
        st.caption(f"RentCafe [{idx}/{min(len(prop_links), max_props)}]: {url}")
        polite_sleep(delay_min, delay_max)
        row = rentcafe_extract_details(session, url, follow_mgmt, delay_min, delay_max)
        if row and (row["Property Name"] or row["Address"]):
            results.append(row)
        detail_prog.progress(min(idx/max_props, 1.0))
    return results

# ----------------------------
# Main Run
# ----------------------------
if go:
    if mode == "City & State" and (not city or not state):
        st.error("Please provide both City and State.")
        st.stop()
    if mode == "Full Search URL" and not search_url.strip():
        st.error("Please paste a valid search URL.")
        st.stop()

    session = make_session(referer_hint or "")
    all_rows: List[Dict] = []

    if mode == "Manual Property URLs":
        urls = [u.strip() for u in manual_urls_text.splitlines() if u.strip()]
        if not urls:
            st.error("Paste at least one property URL.")
            st.stop()
        st.info(f"Processing {len(urls)} pasted property URLs…")
        detail_prog = st.progress(0)
        for i, url in enumerate(urls, start=1):
            polite_sleep(delay_min, delay_max)
            if "apartments.com" in url:
                row = apts_extract_details(session, url, follow_mgmt, delay_min, delay_max)
            elif "rentcafe.com" in url:
                row = rentcafe_extract_details(session, url, follow_mgmt, delay_min, delay_max)
            else:
                row = None
            if row and (row["Property Name"] or row["Address"]):
                all_rows.append(row)
            detail_prog.progress(min(i/len(urls), 1.0))
    else:
        # apartments.com
        if use_apartments:
            if mode == "Full Search URL" and "apartments.com" in search_url.lower():
                rows = scan_apartments(session, city, state, int(pages), int(max_props), follow_mgmt, delay_min, delay_max, base_url_override=search_url.strip())
            else:
                rows = scan_apartments(session, city, state, int(pages), int(max_props), follow_mgmt, delay_min, delay_max)
            all_rows.extend(rows)
        # rentcafe
        if use_rentcafe:
            if mode == "Full Search URL" and "rentcafe.com" in search_url.lower():
                rows = scan_rentcafe(session, city, state, int(pages), int(max_props), follow_mgmt, delay_min, delay_max, base_url_override=search_url.strip())
            else:
                rows = scan_rentcafe(session, city, state, int(pages), int(max_props), follow_mgmt, delay_min, delay_max)
            all_rows.extend(rows)

    if not all_rows:
        st.error("No properties parsed. Try fewer pages, increase delays, switch source/mode, or paste URLs manually.")
        st.stop()

    df = pd.DataFrame(all_rows).fillna("")
    df = df.drop_duplicates(subset=["Source URL"]).reset_index(drop=True)
    if df.duplicated(subset=["Property Name", "Address"]).any():
        df = df.drop_duplicates(subset=["Property Name", "Address"]).reset_index(drop=True)

    # Messaging columns
    call_scripts, email_subjects, email_bodies = [], [], []
    for _, r in df.iterrows():
        script = build_call_script(r.get("Property Name", ""), r.get("Address", ""), r.get("Management Company", ""))
        email = build_email_template(r.get("Property Name", ""), r.get("Address", ""), r.get("Management Company", ""))
        call_scripts.append(script)
        email_subjects.append(email["subject"])
        email_bodies.append(email["body"])
    df["Call Script"] = call_scripts
    df["Email Subject"] = email_subjects
    df["Email Body"] = email_bodies

    st.success(f"✅ Done! Parsed **{len(df)}** properties.")
    st.dataframe(df, use_container_width=True, height=480)

    st.session_state["results"] = df

    # Downloads
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    xls_buf = io.BytesIO()
    with pd.ExcelWriter(xls_buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Leads")
    xls_buf.seek(0)

    colA, colB = st.columns(2)
    colA.download_button("⬇️ Download CSV", data=csv_bytes, file_name="multifamily_leads.csv", mime="text/csv")
    colB.download_button("⬇️ Download Excel (XLSX)", data=xls_buf, file_name="multifamily_leads.xlsx",
                         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ----------------------------
# Utilities: Merge / Dedupe / Google Sheets
# ----------------------------
st.markdown("### 📦 Utilities")
u1, u2, u3 = st.columns(3)
with u1:
    uploaded = st.file_uploader("Upload CSV/XLSX to merge (optional)", type=["csv", "xlsx"])
with u2:
    dedupe_cols = st.multiselect(
        "Dedupe by columns",
        ["Property Name","Address","Management Company","Phone","Email","Source URL"],
        default=["Source URL"]
    )
with u3:
    run_merge = st.button("🔁 Merge & Dedupe")

if run_merge and uploaded:
    if uploaded.name.lower().endswith(".csv"):
        new_df = pd.read_csv(uploaded)
    else:
        new_df = pd.read_excel(uploaded)
    base_df = st.session_state.get("results", pd.DataFrame())
    merged = pd.concat([base_df, new_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=dedupe_cols).reset_index(drop=True)
    st.session_state["results"] = merged
    st.success(f"Merged! Combined rows: **{len(merged)}**")
    st.dataframe(merged, use_container_width=True, height=420)

st.markdown("### 📤 Google Sheets Export")
if not HAS_GSHEETS:
    st.info("To enable Sheets export: `pip install gspread google-auth` and add your service account JSON to `st.secrets['gcp_service_account']`.")
col1, col2, col3 = st.columns([2,2,1])
spreadsheet_id = col1.text_input("Spreadsheet ID (the long ID in the Sheet URL)", value="")
worksheet_name = col2.text_input("Worksheet name", value="Leads")
push_btn = col3.button("☁️ Append to Google Sheets")

if push_btn:
    df = st.session_state.get("results", pd.DataFrame())
    if df.empty:
        st.error("No results to append.")
    elif not spreadsheet_id.strip():
        st.error("Please provide a Spreadsheet ID.")
    else:
        msg = append_to_sheet(df, spreadsheet_id.strip(), worksheet_name.strip() or "Leads")
        if msg.startswith("Appended"):
            st.success(msg)
        else:
            st.error(msg)

st.caption("⚠️ Respect each website’s Terms of Service and robots.txt. Keep crawl volumes/velocity reasonable.")
