# app.py
# Multifamily Lead Finder — Apartments.com + RentCafe (+ Entrata/Yardi follow) → CSV/XLSX/Google Sheets
# - Input: City/State or a search URL (Apartments.com or RentCafe)
# - Output: Property Name, Address, Management Company, Phone, Email, URLs
# - Enrichment: follow "Managed by" or footer links (Entrata/Yardi/RentCafe sites) to find public email/phone
# - Messaging: generates call scripts and email templates per property
# - Export: CSV, XLSX, and Google Sheets (service account JSON in st.secrets["gcp_service_account"])
#
# Requirements:
#   pip install streamlit requests beautifulsoup4 lxml pandas xlsxwriter urllib3 gspread google-auth
#
# Usage:
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
st.set_page_config(page_title="Multifamily Lead Finder", page_icon="🏢", layout="wide")
st.title("🏢 Multifamily Lead Finder — Apartments.com + RentCafe → CSV/XLSX/Google Sheets")

st.write(
    "Find **multifamily properties** and **management contacts** for outreach.\n\n"
    "Enter a **City & State** (e.g., `Miami, FL`) or paste a **search URL** from Apartments.com or RentCafe."
)

# ----------------------------
# Helpers
# ----------------------------
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(r"(?:(?:\+?1[\s\-\.]?)?(?:\(?\d{3}\)?[\s\-\.]?)\d{3}[\s\-\.]?\d{4})", re.MULTILINE | re.DOTALL)

def make_session() -> requests.Session:
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
    s.headers.update(UA)
    return s

def polite_sleep(min_s: float, max_s: float):
    time.sleep(random.uniform(min_s, max_s))

def safe_get(session: requests.Session, url: str, timeout: int = 20):
    try:
        resp = session.get(url, timeout=timeout)
        if resp.status_code in (403, 429):
            # gentle retry after small nap
            polite_sleep(1.0, 2.0)
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
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def apts_collect_property_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    links = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(base_url, href)
        netloc = urlparse(full).netloc
        if "apartments.com" in netloc:
            if "/property/" in full or "/apartments/" in full or full.count("/") > 4:
                links.add(full.split("?")[0].rstrip("/"))
    return list(links)

def apts_is_property_detail(soup: BeautifulSoup) -> bool:
    # heuristic: many detail pages have "Managed by" somewhere
    if soup.find(string=re.compile(r"Managed by", re.I)):
        return True
    # or the presence of floorplans container
    if soup.select_one('[data-testid*="floor-plan"]'):
        return True
    return False

def apts_extract_details(session: requests.Session, url: str, follow_mgmt: bool, delay_min: float, delay_max: float) -> Optional[Dict]:
    r = safe_get(session, url, 25)
    if not (r and r.ok):
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    if not apts_is_property_detail(soup):
        return None

    name = ""
    address = ""
    mgmt = ""
    phone = ""
    email = ""
    mgmt_url = ""

    # name
    h1 = soup.find(["h1", "h2"], string=True)
    if h1:
        name = clean_text(h1.get_text())

    # address
    addr_tag = soup.find(attrs={"data-testid": re.compile(r"property-address|address", re.I)}) or \
               soup.find("address") or \
               soup.find("div", class_=re.compile(r"property-address|address", re.I))
    if addr_tag:
        address = clean_text(addr_tag.get_text())

    # phone
    tel = soup.select_one('a[href^="tel:"]')
    if tel:
        phone = clean_text(tel.get_text() or tel.get("href", "").replace("tel:", ""))
    if not phone:
        m = PHONE_RE.search(soup.get_text(" ", strip=True))
        if m:
            phone = clean_text(m.group(0))

    # mgmt + mgmt_url
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

    # follow mgmt site for email/phone
    if follow_mgmt and mgmt_url:
        polite_sleep(delay_min, delay_max)
        r2 = safe_get(session, mgmt_url, 25)
        if r2 and r2.ok:
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
                # check mailto links
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
    # Common RentCafe geo pattern: https://www.rentcafe.com/apartments-for-rent/us/fl/miami/
    base = f"https://www.rentcafe.com/apartments-for-rent/us/{state.strip().lower()}/{city.strip().lower().replace(' ', '-')}/"
    urls = []
    for p in range(1, pages + 1):
        if p == 1:
            urls.append(base)
        else:
            urls.append(f"{base}?page={p}")
    return urls

def rentcafe_collect_property_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    links = set()
    # cards link to detail pages
    for a in soup.select("a[href].property-title, a[href].card-title, a[href].btn-details"):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(base_url, href)
        if "rentcafe.com" in urlparse(full).netloc:
            links.add(full.split("?")[0].rstrip("/"))
    # fallback: any anchor to rentcafe property detail
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(base_url, href)
        if "rentcafe.com" in urlparse(full).netloc and "/apartments/" in full:
            links.add(full.split("?")[0].rstrip("/"))
    return list(links)

def rentcafe_is_property_detail(soup: BeautifulSoup) -> bool:
    # presence of community-details or managed by text
    if soup.find(string=re.compile(r"Managed by|Management", re.I)):
        return True
    if soup.select_one(".community-details, .community-header, #communityName"):
        return True
    return False

def rentcafe_extract_details(session: requests.Session, url: str, follow_mgmt: bool, delay_min: float, delay_max: float) -> Optional[Dict]:
    r = safe_get(session, url, 25)
    if not (r and r.ok):
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    if not rentcafe_is_property_detail(soup):
        return None

    name = ""
    address = ""
    mgmt = ""
    phone = ""
    email = ""
    mgmt_url = ""

    # name
    name_tag = soup.select_one("#communityName, h1, .community-header h1")
    if name_tag:
        name = clean_text(name_tag.get_text())

    # address
    addr_tag = soup.select_one(".community-address, .address, address")
    if addr_tag:
        address = clean_text(addr_tag.get_text())

    # phone
    tel = soup.select_one('a[href^="tel:"]')
    if tel:
        phone = clean_text(tel.get_text() or tel.get("href", "").replace("tel:", ""))
    if not phone:
        m = PHONE_RE.search(soup.get_text(" ", strip=True))
        if m:
            phone = clean_text(m.group(0))

    # management
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

    # follow mgmt site
    if follow_mgmt and mgmt_url:
        polite_sleep(delay_min, delay_max)
        r2 = safe_get(session, mgmt_url, 25)
        if r2 and r2.ok:
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
# Enrichment / Generic site scan (Entrata/Yardi/etc.)
# ----------------------------
def generic_enrich_site(session: requests.Session, url: str) -> Dict[str, str]:
    info = {"email": "", "phone": ""}
    r = safe_get(session, url, 25)
    if not (r and r.ok):
        return info
    s = BeautifulSoup(r.text, "html.parser")
    # email
    m = EMAIL_RE.search(s.get_text(" ", strip=True))
    if m:
        info["email"] = clean_text(m.group(0))
    else:
        mailtos = [a.get("href") for a in s.select('a[href^="mailto:"]')]
        mailtos = [x.replace("mailto:", "") for x in mailtos if x]
        if mailtos:
            info["email"] = clean_text(mailtos[0])
    # phone
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
# Messaging (Call Script + Email Template)
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

        # Ensure header
        existing = ws.get_all_values()
        if not existing:
            ws.append_row(list(df.columns))
        # Append rows
        rows = df.astype(str).values.tolist()
        for r in rows:
            ws.append_row(r)
        return f"Appended {len(rows)} rows to '{worksheet_name}'."
    except Exception as e:
        return f"Append failed: {e}"

# ----------------------------
# UI Controls
# ----------------------------
mode = st.radio("Choose Input Mode", ["City & State", "Full Search URL"], horizontal=True)

col_a, col_b, col_c = st.columns([2, 1, 1])
city = state = search_url = ""
if mode == "City & State":
    city = col_a.text_input("City", value="Miami")
    state = col_b.text_input("State (2-letter)", value="FL", max_chars=2)
else:
    search_url = st.text_input(
        "Search URL (Apartments.com or RentCafe)",
        value="https://www.apartments.com/miami-fl/"
    )

pages = st.number_input("Max listing pages to crawl (per source)", min_value=1, max_value=50, value=3, step=1)
max_props = st.number_input("Max properties to process (safety cap)", min_value=1, max_value=3000, value=400, step=25)

st.markdown("**Sources to scan**")
src1, src2 = st.columns(2)
use_apartments = src1.checkbox("Apartments.com", value=True)
use_rentcafe   = src2.checkbox("RentCafe", value=True)

follow_mgmt = st.checkbox("Follow 'Managed by' / management site to hunt public email/phone", value=True)
delay_min, delay_max = st.slider("Per-request random delay (seconds)", 0.2, 3.0, (0.6, 1.5), step=0.1)

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
# Crawl/Extract per source
# ----------------------------
def scan_apartments(session: requests.Session, city: str, state: str, pages: int, max_props: int,
                    follow_mgmt: bool, delay_min: float, delay_max: float, base_url_override: str = "") -> List[Dict]:
    results = []
    # derive listing urls
    if base_url_override:
        base = base_url_override if base_url_override.endswith("/") else base_url_override + "/"
        listing_urls = []
        for p in range(1, pages + 1):
            if p == 1:
                listing_urls.append(base)
            else:
                listing_urls.append(f"{base}?page={p}")
                listing_urls.append(urljoin(base, f"{p}/"))
        # dedupe
        seen = set()
        listing_urls = [u for u in listing_urls if not (u in seen or seen.add(u))]
    else:
        listing_urls = apts_build_listing_urls(city, state, pages)

    st.info(f"Apartments.com: scanning up to {len(listing_urls)} listing pages…")
    prop_links = []
    progress = st.progress(0)
    for i, lu in enumerate(listing_urls, start=1):
        polite_sleep(delay_min, delay_max)
        r = safe_get(session, lu)
        if not (r and r.ok):
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        prop_links.extend(apts_collect_property_links(soup, lu))
        progress.progress(min(i/len(listing_urls), 1.0))

    prop_links = list(dict.fromkeys(prop_links))
    if not prop_links:
        st.warning("Apartments.com: no property links found.")
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
        listing_urls = []
        base = base_url_override
        for p in range(1, pages + 1):
            if p == 1:
                listing_urls.append(base)
            else:
                listing_urls.append(f"{base}?page={p}")
    else:
        listing_urls = rentcafe_build_listing_urls(city, state, pages)

    st.info(f"RentCafe: scanning up to {len(listing_urls)} listing pages…")
    prop_links = []
    progress = st.progress(0)
    for i, lu in enumerate(listing_urls, start=1):
        polite_sleep(delay_min, delay_max)
        r = safe_get(session, lu)
        if not (r and r.ok):
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        prop_links.extend(rentcafe_collect_property_links(soup, lu))
        progress.progress(min(i/len(listing_urls), 1.0))

    prop_links = list(dict.fromkeys(prop_links))
    if not prop_links:
        st.warning("RentCafe: no property links found.")
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

    session = make_session()
    all_rows = []

    # apartments.com
    if use_apartments:
        if mode == "Full Search URL" and "apartments.com" in search_url.lower():
            rows = scan_apartments(session, city, state, pages, max_props, follow_mgmt, delay_min, delay_max, base_url_override=search_url.strip())
        else:
            rows = scan_apartments(session, city, state, pages, max_props, follow_mgmt, delay_min, delay_max)
        all_rows.extend(rows)

    # rentcafe
    if use_rentcafe:
        if mode == "Full Search URL" and "rentcafe.com" in search_url.lower():
            rows = scan_rentcafe(session, city, state, pages, max_props, follow_mgmt, delay_min, delay_max, base_url_override=search_url.strip())
        else:
            rows = scan_rentcafe(session, city, state, pages, max_props, follow_mgmt, delay_min, delay_max)
        all_rows.extend(rows)

    if not all_rows:
        st.error("No properties parsed. Try fewer pages, adjust delays, or switch source/mode.")
        st.stop()

    df = pd.DataFrame(all_rows)
    # Dedupe
    df = df.fillna("")
    df = df.drop_duplicates(subset=["Source URL"]).reset_index(drop=True)
    if df.duplicated(subset=["Property Name", "Address"]).any():
        df = df.drop_duplicates(subset=["Property Name", "Address"]).reset_index(drop=True)

    # Generate messaging
    call_scripts = []
    email_subjects = []
    email_bodies = []
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

    # Save to session
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
