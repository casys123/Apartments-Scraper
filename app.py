# app.py
# Multifamily Lead Finder — Apartments.com  ➜ Property + Manager data to CSV/XLSX
# - Input: City/State **or** a full Apartments.com search URL
# - Output: Property Name, Address, Management Company, Phone, Email (when available)
# - Extras: follows "Managed by" link (if present) and tries to find a public email/phone on the mgmt site
# - Built for Streamlit Cloud or local use (Python 3.9+)

import re
import os
import io
import json
import time
import math
import html
import random
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import streamlit as st

# ----------------------------
# Streamlit Page Setup
# ----------------------------
st.set_page_config(page_title="Multifamily Lead Finder", page_icon="🏢", layout="wide")
st.title("🏢 Multifamily Lead Finder — Apartments.com → CSV/XLSX")

st.write(
    "Enter a **City & State** (e.g., `Miami, FL`) or paste an **Apartments.com search URL** "
    "(e.g., `https://www.apartments.com/miami-fl/`). The app will crawl listings, visit property pages, "
    "and extract **Property Name, Address, Management Company, Phone, and Email (when available)**."
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
PHONE_RE = re.compile(
    r"(?:(?:\+?1[\s\-\.]?)?(?:\(?\d{3}\)?[\s\-\.]?)\d{3}[\s\-\.]?\d{4})"
)

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
        # basic soft-block handling
        if resp.status_code == 403:
            # try again with small delay
            polite_sleep(1.0, 2.0)
            resp = session.get(url, timeout=timeout)
        return resp
    except requests.RequestException:
        return None

def clean_text(x: str) -> str:
    if not x:
        return ""
    return " ".join(html.unescape(x).split())

def parse_json_ld(soup: BeautifulSoup):
    """Return best-effort JSON-LD data (ApartmentComplex / Place / Organization)."""
    candidates = []
    for tag in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(tag.get_text(strip=True))
        except Exception:
            continue
        # data may be a dict or a list
        if isinstance(data, dict):
            candidates.append(data)
        elif isinstance(data, list):
            candidates.extend([d for d in data if isinstance(d, dict)])
    # flatten @graph nodes too
    flattened = []
    for d in candidates:
        if "@graph" in d and isinstance(d["@graph"], list):
            flattened.extend([g for g in d["@graph"] if isinstance(g, dict)])
        else:
            flattened.append(d)
    return flattened

def first_nonempty(*args):
    for a in args:
        if a and str(a).strip():
            return clean_text(str(a))
    return ""

def extract_address_from_ld(addr_obj):
    if not isinstance(addr_obj, dict):
        return ""
    parts = []
    for key in ["streetAddress", "addressLocality", "addressRegion", "postalCode"]:
        if addr_obj.get(key):
            parts.append(str(addr_obj[key]))
    return clean_text(", ".join(parts))

def likely_property_detail(soup: BeautifulSoup) -> bool:
    """Heuristic: property detail pages usually contain 'Managed by' text or JSON-LD of ApartmentComplex."""
    ld = parse_json_ld(soup)
    for d in ld:
        t = d.get("@type")
        if isinstance(t, list):
            if any("Apartment" in x or "Place" in x for x in t):
                return True
        elif isinstance(t, str) and ("Apartment" in t or "Place" in t):
            return True
    # fallback: look for 'Managed by' label
    if soup.find(string=re.compile(r"Managed by", re.I)):
        return True
    return False

def collect_property_links_from_listing(soup: BeautifulSoup, base_url: str) -> list:
    """Collect candidate property detail links from a listing page; filter later by likely_property_detail."""
    links = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(base_url, href)
        # only apartments.com pages; avoid query-only dupes
        if "apartments.com" in urlparse(full).netloc:
            if "/property/" in full or "/apartments/" in full or full.count("/") > 4:
                links.add(full.split("?")[0].rstrip("/"))
    return list(links)

def build_listing_urls(city: str, state: str, pages: int) -> list:
    """Build listing URLs for city/state with page variations that Apartments.com supports."""
    base = f"https://www.apartments.com/{city.strip().lower().replace(' ', '-')}-{state.strip().lower()}/"
    urls = []
    for p in range(1, pages + 1):
        # Some geos use ?page=, some support /pN/
        if p == 1:
            urls.append(base)
        else:
            urls.append(f"{base}?page={p}")
            urls.append(urljoin(base, f"{p}/"))
    # de-dup while preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def extract_property_details(session: requests.Session, url: str, follow_mgmt: bool, timeout: int = 25):
    """Return dict with fields: Property Name, Address, Management Company, Phone, Email, Source URL, Mgmt URL."""
    resp = safe_get(session, url, timeout=timeout)
    if not (resp and resp.ok):
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    if not likely_property_detail(soup):
        return None

    name = ""
    address = ""
    mgmt = ""
    phone = ""
    email = ""

    # 1) Try JSON-LD first
    for d in parse_json_ld(soup):
        t = d.get("@type")
        # allow list/str match
        t_list = []
        if isinstance(t, list):
            t_list = [str(x) for x in t]
        elif isinstance(t, str):
            t_list = [t]
        if any("Apartment" in x or "Place" in x or "Organization" in x for x in t_list):
            cand_name = d.get("name")
            cand_phone = d.get("telephone") or d.get("contactPoint", {}).get("telephone") if isinstance(d.get("contactPoint"), dict) else None
            cand_addr = d.get("address")
            if isinstance(cand_addr, dict):
                cand_addr_str = extract_address_from_ld(cand_addr)
            else:
                cand_addr_str = cand_addr if isinstance(cand_addr, str) else ""

            # mgmt sometimes appears as brand/provider/employee/department
            cand_mgmt = (
                (d.get("brand") or {}).get("name") if isinstance(d.get("brand"), dict) else d.get("brand")
            )
            cand_mgmt = cand_mgmt or ((d.get("provider") or {}).get("name") if isinstance(d.get("provider"), dict) else d.get("provider"))

            name = first_nonempty(name, cand_name)
            address = first_nonempty(address, cand_addr_str)
            phone = first_nonempty(phone, cand_phone)
            mgmt = first_nonempty(mgmt, cand_mgmt)

    # 2) Fallbacks from visible DOM
    # Name
    if not name:
        h1 = soup.find(["h1", "h2"], string=True)
        if h1:
            name = clean_text(h1.get_text())

    # Address block
    if not address:
        addr_tag = soup.find(attrs={"data-testid": re.compile(r"property-address|address", re.I)}) or \
                   soup.find("address") or \
                   soup.find("div", class_=re.compile(r"property-address|address", re.I))
        if addr_tag:
            address = clean_text(addr_tag.get_text())

    # Phone: look for tel: link, or regex on page
    if not phone:
        tel = soup.select_one('a[href^="tel:"]')
        if tel:
            phone = clean_text(tel.get_text() or tel.get("href", "").replace("tel:", ""))
    if not phone:
        m = PHONE_RE.search(soup.get_text(" ", strip=True))
        if m:
            phone = clean_text(m.group(0))

    # Management company: look for "Managed by"
    mgmt_url = ""
    label = soup.find(string=re.compile(r"Managed by", re.I))
    if label:
        # look for a nearby anchor
        block = label.parent if hasattr(label, "parent") else None
        if block:
            a = block.find("a", href=True) or (block.find_next("a", href=True) if block else None)
            if a and a.get("href"):
                mgmt = first_nonempty(mgmt, a.get_text())
                mgmt_url = a["href"] if a["href"].startswith("http") else urljoin(url, a["href"])
            else:
                # text after label
                mgmt = first_nonempty(mgmt, block.get_text())

    # Optional: follow mgmt URL to try find email/phone
    if follow_mgmt and mgmt_url:
        polite_sleep(0.7, 1.4)
        r2 = safe_get(session, mgmt_url, timeout=timeout)
        if r2 and r2.ok:
            s2 = BeautifulSoup(r2.text, "html.parser")
            if not email:
                m = EMAIL_RE.search(s2.get_text(" ", strip=True))
                if m:
                    email = m.group(0)
            if not phone:
                m2 = s2.select_one('a[href^="tel:"]')
                if m2:
                    phone = clean_text(m2.get_text() or m2.get("href", "").replace("tel:", ""))
            # sometimes footer lists a generic inbox/company email
            if not email:
                # scan mailto: links
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
    }

# ----------------------------
# UI Controls
# ----------------------------
mode = st.radio("Choose Input Mode", ["City & State", "Full Apartments.com URL"], horizontal=True)

city = state = search_url = ""
if mode == "City & State":
    col1, col2 = st.columns([2, 1])
    city = col1.text_input("City", value="Miami")
    state = col2.text_input("State (2-letter)", value="FL", max_chars=2)
else:
    search_url = st.text_input("Apartments.com search URL", value="https://www.apartments.com/miami-fl/")

pages = st.number_input("Max listing pages to crawl", min_value=1, max_value=50, value=3, step=1)
max_props = st.number_input("Max properties to process (safety cap)", min_value=1, max_value=2000, value=200, step=10)
follow_mgmt = st.checkbox("Also follow 'Managed by' link to hunt for public email/phone", value=True)
delay_min, delay_max = st.slider("Per-request random delay range (seconds)", 0.2, 3.0, (0.6, 1.5), step=0.1)

go = st.button("🚀 Start Scan")

# Session storage
if "results" not in st.session_state:
    st.session_state["results"] = pd.DataFrame(
        columns=["Property Name", "Address", "Management Company", "Phone", "Email", "Source URL", "Mgmt URL"]
    )

# ----------------------------
# Main Scan
# ----------------------------
if go:
    session = make_session()
    results = []

    if mode == "City & State":
        if not city or not state:
            st.error("Please provide both City and State.")
            st.stop()
        listing_urls = build_listing_urls(city, state, pages)
    else:
        if not search_url.strip():
            st.error("Please paste a valid Apartments.com search URL.")
            st.stop()
        # attempt to derive paged URLs; we'll try both ?page=N and /N/
        base = search_url.strip()
        if not base.endswith("/"):
            base = base + "/"
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

    st.info(f"Crawling up to **{len(listing_urls)}** listing pages…")
    progress = st.progress(0)
    prop_links = []

    # 1) Collect candidate property detail links
    for i, lu in enumerate(listing_urls, start=1):
        polite_sleep(delay_min, delay_max)
        r = safe_get(session, lu)
        if not (r and r.ok):
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        links = collect_property_links_from_listing(soup, lu)
        prop_links.extend(links)
        progress.progress(min(i / max(1, len(listing_urls)), 1.0))

    # de-dup and cap
    prop_links = list(dict.fromkeys(prop_links))  # preserve order
    if not prop_links:
        st.warning("No candidate property links found. Try fewer pages or a different city/URL.")
        st.stop()

    st.write(f"🔎 Found **{len(prop_links)}** candidate property links. Filtering & extracting details…")

    # 2) Visit each property page and extract details
    data_rows = []
    max_to_process = min(len(prop_links), int(max_props))
    detail_prog = st.progress(0)
    status = st.empty()
    processed = 0

    for idx, url in enumerate(prop_links[:max_to_process], start=1):
        status.info(f"Processing {idx}/{max_to_process}: {url}")
        polite_sleep(delay_min, delay_max)
        row = extract_property_details(session, url, follow_mgmt=follow_mgmt)
        if row:
            # basic guard: must have property name
            if row["Property Name"] or row["Address"]:
                data_rows.append(row)
        processed += 1
        detail_prog.progress(min(processed / max_to_process, 1.0))

    if not data_rows:
        st.error("No valid property detail pages parsed. Try increasing pages, adjusting delays, or switching input mode.")
        st.stop()

    df = pd.DataFrame(data_rows)

    # Normalize and dedupe
    df["Property Name"] = df["Property Name"].fillna("").str.strip()
    df["Address"] = df["Address"].fillna("").str.strip()
    df["Management Company"] = df["Management Company"].fillna("").str.strip()
    df["Phone"] = df["Phone"].fillna("").str.strip()
    df["Email"] = df["Email"].fillna("").str.strip()

    # Deduplicate by Source URL or Property+Address combo
    df = df.drop_duplicates(subset=["Source URL"]).reset_index(drop=True)
    if df.duplicated(subset=["Property Name", "Address"]).any():
        df = df.drop_duplicates(subset=["Property Name", "Address"]).reset_index(drop=True)

    st.success(f"✅ Done! Parsed **{len(df)}** properties.")
    st.dataframe(df, use_container_width=True, height=420)

    # Save to session and offer downloads
    st.session_state["results"] = df

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
# Utilities: Merge, Append, Clean
# ----------------------------
st.markdown("### 📦 Utilities")
u1, u2, u3 = st.columns(3)
with u1:
    uploaded = st.file_uploader("Upload CSV/XLSX to merge (optional)", type=["csv", "xlsx"])
with u2:
    dedupe_cols = st.multiselect("Dedupe by columns", ["Property Name", "Address", "Management Company", "Phone", "Email", "Source URL"], default=["Source URL"])
with u3:
    run_merge = st.button("🔁 Merge & Dedupe with Current Results")

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
    st.dataframe(merged, use_container_width=True, height=380)
