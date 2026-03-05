#!/usr/bin/env python3



import asyncio
import json
import csv
import re
import logging
import argparse
from datetime import date
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PwTimeout

# ─── Configuration ───────────────────────────────────────────────────────────

BRANDS = [
    
    "saab", "seat", "skoda", "smart", "vauxhall", "volkswagen", "volvo"
]
# "alfa-romeo", "aston-martin", "audi", "bentley", "bmw", "citroen",
#     "cupra", "ds-automobiles", "ferrari", "fiat", "ford", "holden",
#     "jaguar", "lancia", "land-rover", "mercedes-benz", "mini", "opel",
#     "peugeot", "polestar", "porsche", "renault", "rolls-royce", "rover",
BRAND_BASE_URL = "https://www.trademe.co.nz/a/motors/cars"

CSV_FIELDS = [
    "VIN", "Plate", "Year", "Maker", "Model", "Submodel", "CC",
    "Fuel", "Transmission", "FirstReg", "BodyStyle", "ListingDate", "ListingUrl"
]

MAX_PAGES = 50
PAGE_LOAD_DELAY = 2.0
DETAIL_DELAY = 1.5
CONCURRENT_DETAIL_PAGES = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("trademe")


# ─── Helper Functions ────────────────────────────────────────────────────────

def _model_from_url(url: str) -> str:
    """Extract model from Trade Me URL: /motors/cars/{brand}/{model}/listing/{id}"""
    m = re.search(r'/motors/cars/[^/]+/([^/]+)/listing/', url)
    if m:
        seg = m.group(1)
        if not seg.isdigit():
            return seg.replace("-", " ").title()
    return ""


def _submodel_from_title(title: str, year: str, maker: str, model: str) -> str:
    """Strip year/maker/model prefix from listing title to get the variant/submodel."""
    if not title:
        return ""
    s = title.strip()
    original = s
    if year:
        s = re.sub(rf"^\s*{re.escape(str(year))}\s+", "", s, flags=re.I).strip()
    if maker:
        maker_pat = re.escape(maker).replace(r"\-", r"[\s\-]+")
        s = re.sub(rf"^\s*{maker_pat}\s+", "", s, flags=re.I).strip()
    if model:
        s = re.sub(rf"^\s*{re.escape(model)}\s+", "", s, flags=re.I).strip()
    # If nothing was stripped, no variant found
    if s.lower() == original.lower():
        return ""
    return s


def _format_cc(val) -> str:
    """Ensure CC value has 'cc' suffix, e.g. 1996 -> '1996cc'."""
    if not val:
        return ""
    s = re.sub(r'[cC][cC]$', '', str(val).strip()).strip()
    return (s + "cc") if re.match(r'^\d+$', s) else str(val).strip()


def _normalize_first_reg(val) -> str:
    """Normalize FirstReg to 'NZ New', 'Imported', or empty — never a date."""
    if not val:
        return ""
    s = str(val).strip()
    if re.search(r'nz\s*new', s, re.I):
        return "NZ New"
    if re.search(r'import', s, re.I):
        return "Imported"
    # Reject anything that looks like a date or timestamp
    if re.search(r'\d{4}|\d{1,2}[/\-]\d', s):
        return ""
    return s


def _normalize_listing_date(val: str) -> str:
    """Reject machine timestamps; keep human-readable relative strings."""
    if not val:
        return ""
    s = str(val)
    if "/Date(" in s or re.match(r'^\d{4}-\d{2}-\d{2}', s) or re.match(r'^\d{10,}$', s):
        return ""
    return s.strip()


# ─── Network Interception ────────────────────────────────────────────────────

class APICapture:
    """Captures internal API responses from Trade Me's frontend."""

    def __init__(self):
        self.captured_responses = []

    async def handle_response(self, response):
        """Intercept responses that look like search/listing API calls."""
        url = response.url
        if any(kw in url for kw in [
            "/api/", "/search", "motors", "listing",
            "SearchResults", "MotorsSearch", "graphql"
        ]):
            try:
                if "json" in (response.headers.get("content-type", "") or ""):
                    body = await response.json()
                    self.captured_responses.append({
                        "url": url,
                        "data": body
                    })
            except Exception:
                pass


# ─── Search Page Scraping ────────────────────────────────────────────────────

async def get_search_listings(page, brand: str) -> list[dict]:
    """
    Fetch all listings for a brand using Trade Me's internal search API.
    Page 1: full browser navigation (establishes session/cookies).
    Page 2+: direct API fetch via browser fetch() for correct pagination.
    """
    all_listings = []
    seen_ids = set()

    # Trade Me's internal search API URL pattern (discovered from network capture)
    def api_url(pg):
        return (
            f"https://api.trademe.co.nz/v1/search/general.json"
            f"?sort_order=ExpiryDesc&rows=50"
            f"&return_metadata=true&return_variants=true"
            f"&canonical_path=%2Fmotors%2Fcars%2F{brand}"
            f"&page={pg}"
        )

    # ── Page 1: browser navigation to establish session ──
    pg1_url = f"{BRAND_BASE_URL}/{brand}?sort_order=ExpiryDesc&rows=50&page=1"
    log.info(f"  Page 1: {pg1_url}")
    api_capture = APICapture()
    page.on("response", api_capture.handle_response)
    try:
        await page.goto(pg1_url, wait_until="networkidle", timeout=45000)
    except PwTimeout:
        log.warning("  Timeout on page 1, retrying with domcontentloaded...")
        try:
            await page.goto(pg1_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
        except Exception:
            log.error("  Failed to load page 1")
            page.remove_listener("response", api_capture.handle_response)
            return all_listings

    found_pg1 = []
    next_data = await page.evaluate("""
        () => {
            const el = document.getElementById('__NEXT_DATA__');
            if (el) { try { return JSON.parse(el.textContent); } catch(e) {} }
            return null;
        }
    """)
    if next_data:
        found_pg1 = extract_listings_from_next_data(next_data, brand)
        if found_pg1:
            log.info(f"    [__NEXT_DATA__] Found {len(found_pg1)} listings")

    if not found_pg1:
        for cap in api_capture.captured_responses:
            items = _find_listing_array(cap["data"])
            if items:
                for item in items:
                    r = _parse_api_listing(item, brand)
                    if r:
                        found_pg1.append(r)
        if found_pg1:
            log.info(f"    [API Capture] Found {len(found_pg1)} listings")

    if not found_pg1:
        found_pg1 = await extract_listings_from_dom(page, brand)
        if found_pg1:
            log.info(f"    [DOM] Found {len(found_pg1)} listings")

    page.remove_listener("response", api_capture.handle_response)

    for listing in found_pg1:
        lid = listing.get("ListingId") or listing.get("ListingUrl", "")
        if lid and lid not in seen_ids:
            seen_ids.add(lid)
            all_listings.append(listing)

    if not all_listings:
        log.info("  No listings found on page 1")
        return all_listings

    log.info(f"  Page 1 total: {len(all_listings)} listings")

    # ── Pages 2+: direct API fetch (bypasses SSR, gets correct page data) ──
    for pg in range(2, MAX_PAGES + 1):
        url = api_url(pg)
        log.info(f"  Page {pg} (API): rows=50&page={pg}")
        try:
            response = await page.evaluate(f"""
                async () => {{
                    try {{
                        const r = await fetch({repr(url)}, {{
                            headers: {{"Accept": "application/json"}}
                        }});
                        if (!r.ok) return null;
                        return await r.json();
                    }} catch(e) {{ return null; }}
                }}
            """)
        except Exception as e:
            log.warning(f"  API fetch error page {pg}: {e}")
            break

        if not response:
            log.info(f"  Empty response on page {pg}, stopping")
            break

        items = _find_listing_array(response)
        if not items:
            log.info(f"  No listings in response page {pg}, stopping")
            break

        new_count = 0
        for item in items:
            record = _parse_api_listing(item, brand)
            if record:
                lid = record.get("ListingId") or record.get("ListingUrl", "")
                if lid and lid not in seen_ids:
                    seen_ids.add(lid)
                    all_listings.append(record)
                    new_count += 1

        log.info(f"    +{new_count} new (total: {len(all_listings)})")

        if new_count == 0 or len(items) < 50:
            log.info(f"  Pagination complete at page {pg}")
            break

        await asyncio.sleep(PAGE_LOAD_DELAY)

    return all_listings


def extract_listings_from_next_data(next_data: dict, brand: str) -> list[dict]:
    """Parse search results from __NEXT_DATA__."""
    results = []

    # Try common Next.js data paths
    paths_to_try = [
        lambda d: d["props"]["pageProps"]["searchResults"]["results"],
        lambda d: d["props"]["pageProps"]["data"]["searchResults"],
        lambda d: d["props"]["pageProps"]["listings"],
        lambda d: d["props"]["pageProps"]["data"]["listings"],
        lambda d: d["props"]["pageProps"]["initialData"]["searchResults"]["results"],
    ]

    items = None
    for path_fn in paths_to_try:
        try:
            items = path_fn(next_data)
            if items:
                break
        except (KeyError, TypeError):
            continue

    if not items:
        return results

    for item in items:
        record = _parse_next_data_listing(item, brand)
        if record:
            results.append(record)

    return results


def _parse_next_data_listing(item: dict, brand: str) -> dict | None:
    """Parse a single listing from __NEXT_DATA__ search results."""
    listing_url = item.get("url", "") or item.get("listingUrl", "")
    if not listing_url:
        listing_id = item.get("listingId", "") or item.get("id", "")
        if listing_id:
            listing_url = f"/a/motors/cars/{brand}/{listing_id}"
    if listing_url and not listing_url.startswith("http"):
        listing_url = "https://www.trademe.co.nz" + listing_url

    raw_title = item.get("title", "")
    model = item.get("model", "") or _model_from_url(listing_url)

    # FirstReg = origin label ("NZ New" / "Imported"), not a date
    origin = item.get("origin", "") or item.get("registration", "")
    if not origin:
        is_nz = item.get("isNZNew") or item.get("nzNew")
        if is_nz is True or str(is_nz).lower() in ("true", "1"):
            origin = "NZ New"
        elif is_nz is False or str(is_nz).lower() in ("false", "0"):
            origin = "Imported"

    # ListingDate = relative text ("Listed within the last 30 days" etc.), not timestamp
    listing_date = _normalize_listing_date(
        item.get("ageGroup") or item.get("dateText") or item.get("listingAgeText") or
        item.get("listedText") or ""
    )

    return {
        "ListingId": str(item.get("listingId", item.get("id", ""))),
        "ListingUrl": listing_url,
        "_RawTitle": raw_title,
        "Year": str(item.get("year", "")),
        "Maker": brand.replace("-", " ").title(),
        "Model": model,
        "Submodel": item.get("variant", item.get("submodel", "")),
        "CC": str(item.get("engineSize", item.get("cc", ""))),
        "Fuel": item.get("fuelType", item.get("fuel", "")),
        "Transmission": item.get("transmission", ""),
        "FirstReg": _normalize_first_reg(origin),
        "BodyStyle": item.get("bodyStyle", item.get("body", "")),
        "ListingDate": listing_date,
        "VIN": item.get("vin", ""),
        "Plate": item.get("numberPlate", item.get("plate", "")),
    }


def _find_listing_array(data, depth=0) -> list | None:
    """Recursively search a dict for an array that looks like listings."""
    if depth > 5:
        return None
    if isinstance(data, list) and len(data) > 0:
        first = data[0]
        if isinstance(first, dict) and any(
            k in first for k in ["listingId", "ListingId", "title", "url", "year"]
        ):
            return data
    if isinstance(data, dict):
        for key, val in data.items():
            result = _find_listing_array(val, depth + 1)
            if result:
                return result
    return None


def _parse_api_listing(item: dict, brand: str) -> dict | None:
    """Parse a listing from a captured API response."""
    # Handle both camelCase and PascalCase keys
    listing_id = item.get("listingId") or item.get("ListingId") or item.get("id") or ""
    if not listing_id:
        return None

    url = item.get("url") or item.get("Url") or ""
    if not url and listing_id:
        url = f"https://www.trademe.co.nz/a/motors/cars/{brand}/{listing_id}"
    elif url and not url.startswith("http"):
        url = "https://www.trademe.co.nz" + url

    raw_title = item.get("title") or item.get("Title") or ""
    # Use dedicated model field; fall back to URL; last resort: raw title
    model = (item.get("model") or item.get("Model") or
             _model_from_url(url) or raw_title)

    # FirstReg = origin label, not a date
    origin = item.get("origin") or item.get("Origin") or item.get("registration") or ""
    if not origin:
        is_nz = item.get("isNZNew") or item.get("nzNew") or item.get("IsNZNew")
        if is_nz is True or str(is_nz).lower() in ("true", "1"):
            origin = "NZ New"
        elif is_nz is False or str(is_nz).lower() in ("false", "0"):
            origin = "Imported"

    # ListingDate = relative display text, not a timestamp
    listing_date = _normalize_listing_date(
        item.get("ageGroup") or item.get("AgeGroup") or item.get("dateText") or
        item.get("listingAgeText") or item.get("listedText") or ""
    )

    return {
        "ListingId": str(listing_id),
        "ListingUrl": url,
        "_RawTitle": raw_title,
        "Year": str(item.get("year") or item.get("Year") or ""),
        "Maker": brand.replace("-", " ").title(),
        "Model": model,
        "Submodel": item.get("variant") or item.get("Variant") or item.get("badge") or "",
        "CC": str(item.get("engineSize") or item.get("EngineSize") or ""),
        "Fuel": item.get("fuelType") or item.get("FuelType") or "",
        "Transmission": item.get("transmission") or item.get("Transmission") or "",
        "FirstReg": _normalize_first_reg(origin),
        "BodyStyle": item.get("bodyStyle") or item.get("BodyStyle") or "",
        "ListingDate": listing_date,
        "VIN": item.get("vin") or "",
        "Plate": item.get("numberPlate") or item.get("plate") or "",
    }


async def extract_listings_from_dom(page, brand: str) -> list[dict]:
    """Fallback: scrape listing cards from the DOM."""
    listings = []

    # Try multiple selector patterns
    selectors = [
        'a[href*="/motors/cars/"][href*="/listing/"]',
        'a[href*="/a/motors/cars/"]',
        '[data-testid*="listing"] a',
        '.tm-motors-search-card a',
        '.listing-card a',
    ]

    seen_urls = set()
    for selector in selectors:
        try:
            elements = await page.query_selector_all(selector)
            for el in elements:
                href = await el.get_attribute("href")
                if href and href not in seen_urls and re.search(r'/\d+$', href):
                    seen_urls.add(href)
                    if not href.startswith("http"):
                        href = "https://www.trademe.co.nz" + href
                    lid = re.search(r'/(\d+)$', href)

                    # Try to get card text for basic info
                    text = ""
                    try:
                        text = await el.inner_text()
                    except Exception:
                        pass

                    year_match = re.search(r'\b(19|20)\d{2}\b', text)

                    url_model = _model_from_url(href)
                    listings.append({
                        "ListingId": lid.group(1) if lid else "",
                        "ListingUrl": href,
                        "_RawTitle": text,
                        "Year": year_match.group(0) if year_match else "",
                        "Maker": brand.replace("-", " ").title(),
                        "Model": url_model,
                        "Submodel": "",
                        "CC": "",
                        "Fuel": "",
                        "Transmission": "",
                        "FirstReg": "",
                        "BodyStyle": "",
                        "ListingDate": "",
                        "VIN": "",
                        "Plate": "",
                    })
        except Exception:
            continue

        if listings:
            break

    return listings


# ─── Detail Page Scraping ────────────────────────────────────────────────────

async def enrich_listing(page, listing: dict) -> dict:
    """Visit a listing detail page and extract all available fields."""
    url = listing.get("ListingUrl", "")
    if not url:
        return listing

    try:
        api_capture = APICapture()
        page.on("response", api_capture.handle_response)

        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)

        # ── Strategy 1: __NEXT_DATA__ ──
        next_data = await page.evaluate("""
            () => {
                const el = document.getElementById('__NEXT_DATA__');
                if (el) {
                    try { return JSON.parse(el.textContent); }
                    catch(e) { return null; }
                }
                return null;
            }
        """)

        if next_data:
            listing = _enrich_from_next_data_detail(next_data, listing)

        # ── Strategy 2: Captured API calls ──
        for cap in api_capture.captured_responses:
            listing = _enrich_from_api_detail(cap["data"], listing)

        # ── Strategy 3: JSON-LD structured data ──
        json_ld = await page.evaluate("""
            () => {
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                return Array.from(scripts).map(s => {
                    try { return JSON.parse(s.textContent); }
                    catch(e) { return null; }
                }).filter(Boolean);
            }
        """)
        for ld in (json_ld or []):
            listing = _enrich_from_json_ld(ld, listing)

        # ── Strategy 4: Regex on page text ──
        listing = await _enrich_from_page_text(page, listing)

        # ── Strategy 5: Structured key-value pairs from detail tables ──
        listing = await _enrich_from_detail_table(page, listing)

        # ── Post-processing ──
        # Derive submodel from title if still empty
        if not listing.get("Submodel") and listing.get("_RawTitle"):
            listing["Submodel"] = _submodel_from_title(
                listing["_RawTitle"],
                listing.get("Year", ""),
                listing.get("Maker", ""),
                listing.get("Model", ""),
            )

        # Normalize FirstReg (ensure no date slipped through)
        listing["FirstReg"] = _normalize_first_reg(listing.get("FirstReg", ""))

        # Clear any machine timestamp from ListingDate
        listing["ListingDate"] = _normalize_listing_date(listing.get("ListingDate", ""))

        page.remove_listener("response", api_capture.handle_response)

        log.info(f"    ✓ {listing.get('Year','')} {listing.get('Maker','')} {listing.get('Model','')[:30]}")

    except PwTimeout:
        log.warning(f"    ✗ Timeout: {url}")
    except Exception as e:
        log.warning(f"    ✗ Error: {url} — {e}")

    await asyncio.sleep(DETAIL_DELAY)
    return listing


def _enrich_from_next_data_detail(next_data: dict, listing: dict) -> dict:
    """Extract detail fields from __NEXT_DATA__ on a listing page."""
    try:
        props = next_data.get("props", {}).get("pageProps", {})

        # Find the listing detail object
        detail = None
        for key in ["listingDetail", "listing", "data", "motorListing", "vehicle"]:
            detail = props.get(key)
            if detail and isinstance(detail, dict):
                break
        if not detail:
            detail = props

        # Check for nested vehicle/motor object
        motor = detail.get("motor", {}) or detail.get("vehicle", {}) or detail.get("motorAttributes", {}) or {}

        # Build attribute dict from attributes array
        attrs = {}
        for attr in (detail.get("attributes", []) or motor.get("attributes", []) or []):
            name = str(attr.get("name", "") or attr.get("label", "")).lower().replace(" ", "_")
            value = str(attr.get("value", "") or attr.get("displayValue", "") or attr.get("display", ""))
            if name and value:
                attrs[name] = value

        # Map fields (only fill if currently empty)
        def pick(current, *candidates):
            if current:
                return current
            for c in candidates:
                if c:
                    return str(c)
            return current

        listing["VIN"] = pick(listing["VIN"],
            attrs.get("vin"), motor.get("vin"), detail.get("vin"),
            attrs.get("chassis_number"), motor.get("chassisNumber"))

        listing["Plate"] = pick(listing["Plate"],
            attrs.get("plate"), attrs.get("number_plate"), attrs.get("registration_plate"),
            motor.get("numberPlate"), motor.get("plate"), detail.get("numberPlate"))

        listing["Year"] = pick(listing["Year"],
            motor.get("year"), attrs.get("year"), detail.get("year"))

        listing["Model"] = pick(listing.get("Model"),
            motor.get("model"), attrs.get("model"), detail.get("model")) or listing.get("Model", "")

        listing["Submodel"] = pick(listing["Submodel"],
            motor.get("submodel"), motor.get("variant"), attrs.get("submodel"),
            attrs.get("variant"), attrs.get("badge"), detail.get("variant"))

        listing["CC"] = pick(listing["CC"],
            motor.get("engineSize"), attrs.get("engine_size"), attrs.get("cc"),
            attrs.get("engine_capacity"), detail.get("engineSize"))

        listing["Fuel"] = pick(listing["Fuel"],
            motor.get("fuelType"), attrs.get("fuel_type"), attrs.get("fuel"),
            detail.get("fuelType"))

        listing["Transmission"] = pick(listing["Transmission"],
            motor.get("transmission"), attrs.get("transmission"), detail.get("transmission"))

        # FirstReg = "NZ New" / "Imported" (origin label, not a date)
        if not listing["FirstReg"]:
            origin_val = (motor.get("origin") or attrs.get("origin") or
                          detail.get("origin") or attrs.get("registration") or
                          motor.get("registration") or "")
            if not origin_val:
                is_nz = motor.get("isNZNew") or detail.get("isNZNew")
                if is_nz is True or str(is_nz).lower() in ("true", "1"):
                    origin_val = "NZ New"
                elif is_nz is False or str(is_nz).lower() in ("false", "0"):
                    origin_val = "Imported"
            listing["FirstReg"] = _normalize_first_reg(origin_val)

        listing["BodyStyle"] = pick(listing["BodyStyle"],
            motor.get("bodyStyle"), attrs.get("body_style"), attrs.get("body"),
            attrs.get("body_type"), detail.get("bodyStyle"))

        # ListingDate = relative text only; reject timestamps
        if not listing["ListingDate"]:
            raw_date = (detail.get("ageGroup") or detail.get("dateText") or
                        detail.get("listedText") or detail.get("listingAgeText") or "")
            listing["ListingDate"] = _normalize_listing_date(raw_date)

    except Exception as e:
        log.debug(f"_enrich_from_next_data_detail error: {e}")

    return listing


def _enrich_from_api_detail(data: dict, listing: dict) -> dict:
    """Enrich from captured API detail response."""
    if not isinstance(data, dict):
        return listing

    def pick(current, *candidates):
        if current:
            return current
        for c in candidates:
            if c:
                return str(c)
        return current

    # Flatten nested structures
    flat = {}
    def flatten(d, prefix=""):
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, (str, int, float)):
                    flat[k.lower()] = str(v)
                elif isinstance(v, dict):
                    flatten(v, prefix + k + ".")
        elif isinstance(d, list):
            for item in d:
                flatten(item, prefix)
    flatten(data)

    listing["VIN"] = pick(listing["VIN"], flat.get("vin"), flat.get("chassisnumber"))
    listing["Plate"] = pick(listing["Plate"], flat.get("numberplate"), flat.get("plate"))
    listing["Fuel"] = pick(listing["Fuel"], flat.get("fueltype"), flat.get("fuel"))
    listing["CC"] = pick(listing["CC"], flat.get("enginesize"), flat.get("cc"), flat.get("enginecapacity"))
    listing["Submodel"] = pick(listing["Submodel"], flat.get("submodel"), flat.get("variant"), flat.get("badge"))
    # FirstReg = origin label only
    if not listing["FirstReg"]:
        origin_val = flat.get("origin") or flat.get("registration") or ""
        if not origin_val:
            is_nz = flat.get("isnznew") or flat.get("nznew")
            if is_nz in ("true", "1", True):
                origin_val = "NZ New"
            elif is_nz in ("false", "0", False):
                origin_val = "Imported"
        listing["FirstReg"] = _normalize_first_reg(origin_val)
    # ListingDate = relative text only
    if not listing["ListingDate"]:
        listing["ListingDate"] = _normalize_listing_date(
            flat.get("agegroup") or flat.get("datetext") or flat.get("listedtext") or ""
        )
    listing["BodyStyle"] = pick(listing["BodyStyle"], flat.get("bodystyle"), flat.get("bodytype"), flat.get("body"))

    return listing


def _enrich_from_json_ld(ld: dict, listing: dict) -> dict:
    """Enrich from JSON-LD structured data."""
    if not isinstance(ld, dict):
        return listing
    ld_type = ld.get("@type", "")
    if ld_type not in ["Car", "Vehicle", "Product", "Offer"]:
        return listing

    def pick(current, val):
        return current if current else (str(val) if val else current)

    listing["VIN"] = pick(listing["VIN"], ld.get("vehicleIdentificationNumber"))
    listing["Model"] = pick(listing.get("Model", ""), ld.get("model"))
    listing["Fuel"] = pick(listing["Fuel"], ld.get("fuelType"))
    listing["BodyStyle"] = pick(listing["BodyStyle"], ld.get("bodyType"))
    listing["Transmission"] = pick(listing["Transmission"], ld.get("vehicleTransmission"))
    listing["CC"] = pick(listing["CC"], ld.get("vehicleEngine", {}).get("engineDisplacement") if isinstance(ld.get("vehicleEngine"), dict) else "")

    brand = ld.get("brand")
    if isinstance(brand, dict):
        listing["Maker"] = pick(listing["Maker"], brand.get("name"))

    return listing


async def _enrich_from_page_text(page, listing: dict) -> dict:
    """Regex extraction from visible page text."""
    try:
        text = await page.inner_text("body")
    except Exception:
        return listing

    patterns = {
        "VIN": [r"(?:VIN|Chassis(?:\s*No\.?)?|Vehicle\s*Identification\s*Number)[:\s]+([A-HJ-NPR-Z0-9a-z]{11,17})", r"\b([A-HJ-NPR-Z0-9]{17})\b"],
        "Plate": [r"(?:Number plate|Plate|Rego)[:\s]+([A-Z0-9]{1,7})"],
        "CC": [r"(?:Engine size|Engine|CC|Capacity)[:\s]+(\d[\d,]*)\s*(?:cc)?"],
        "Fuel": [r"(?:Fuel type|Fuel)[:\s]+(Petrol|Diesel|Electric|Hybrid|Plug.in Hybrid|LPG|CNG|BEV|PHEV|HEV)"],
        "Transmission": [r"(?:Transmission|Gearbox)[:\s]+(Automatic|Manual|CVT|DCT|DSG|Auto|Tiptronic|Steptronic)"],
        "FirstReg": [
            r"(?:Origin|Registration)[:\s]+(NZ\s*[Nn]ew|Imported)",
            r"\b(NZ\s*[Nn]ew)\b",
            r"\b(Imported)\b",
        ],
        "BodyStyle": [r"(?:Body style|Body)[:\s]+(Sedan|Hatchback|SUV|Wagon|Coupe|Convertible|Ute|Van|Station Wagon|Liftback|Roadster|Fastback|Cabriolet|Pickup|People Mover|Cab Chassis)"],
        "Submodel": [r"(?:Variant|Submodel|Badge)[:\s]+([\w\s\-\.]+?)(?:\n|$)"],
    }

    for field, regexes in patterns.items():
        if not listing.get(field):
            for regex in regexes:
                m = re.search(regex, text, re.IGNORECASE)
                if m:
                    listing[field] = m.group(1).strip()
                    break

    if not listing.get("ListingDate"):
        m = re.search(
            r"(Listed\s+(?:within\s+the\s+last\s+\d+\s+days?|more\s+than\s+a\s+month\s+ago"
            r"|yesterday|today|\d+\s+days?\s+ago|this\s+week|this\s+month))",
            text, re.IGNORECASE
        )
        if m:
            listing["ListingDate"] = m.group(1)

    return listing


async def _enrich_from_detail_table(page, listing: dict) -> dict:
    """Extract from key-value detail tables on the page."""
    try:
        # Common patterns: <dt>Key</dt><dd>Value</dd> or <th>Key</th><td>Value</td>
        kv_pairs = await page.evaluate("""
            () => {
                const result = {};

                // dt/dd pairs
                document.querySelectorAll('dl').forEach(dl => {
                    const dts = dl.querySelectorAll('dt');
                    const dds = dl.querySelectorAll('dd');
                    dts.forEach((dt, i) => {
                        if (dds[i]) {
                            result[dt.textContent.trim().toLowerCase()] = dds[i].textContent.trim();
                        }
                    });
                });

                // th/td pairs
                document.querySelectorAll('tr').forEach(tr => {
                    const th = tr.querySelector('th');
                    const td = tr.querySelector('td');
                    if (th && td) {
                        result[th.textContent.trim().toLowerCase()] = td.textContent.trim();
                    }
                });

                // Also look for label/value patterns in divs
                document.querySelectorAll('[class*="detail"], [class*="attribute"], [class*="spec"]').forEach(el => {
                    const label = el.querySelector('[class*="label"], [class*="key"], [class*="name"]');
                    const value = el.querySelector('[class*="value"], [class*="data"]');
                    if (label && value) {
                        result[label.textContent.trim().toLowerCase()] = value.textContent.trim();
                    }
                });

                return result;
            }
        """)

        if not kv_pairs:
            return listing

        field_map = {
            "VIN": ["vin", "chassis number", "chassis", "chassis no", "chassis no.", "wmi", "vehicle identification number", "vin number"],
            "Plate": ["number plate", "plate", "registration plate", "rego"],
            "Year": ["year"],
            "Model": ["model"],
            "Submodel": ["variant", "submodel", "badge", "trim"],
            "CC": ["engine size", "engine", "cc", "capacity", "engine capacity"],
            "Fuel": ["fuel type", "fuel"],
            "Transmission": ["transmission", "gearbox"],
            "FirstReg": ["origin", "registration", "nz new", "imported", "first registered", "first reg"],
            "BodyStyle": ["body style", "body", "body type"],
        }

        for field, keys in field_map.items():
            if not listing.get(field):
                for key in keys:
                    if key in kv_pairs and kv_pairs[key]:
                        listing[field] = kv_pairs[key]
                        break

    except Exception as e:
        log.debug(f"Detail table extraction error: {e}")

    return listing


# ─── Main Orchestrator ───────────────────────────────────────────────────────

async def scrape_all(brands: list[str], output_dir: Path, headless: bool = True):
    """Main scraper orchestrator."""

    output_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    output_file = output_dir / f"trademe_cars_{today}.csv"

    log.info("=" * 60)
    log.info(f"  Trade Me Motors Scraper (Playwright)")
    log.info(f"  Date: {today}")
    log.info(f"  Brands: {len(brands)}")
    log.info(f"  Output: {output_file}")
    log.info("=" * 60)

    all_records = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
        )

        for brand in brands:
            log.info(f"\n🔍 Brand: {brand}")

            # Create a fresh context per brand (clean cookies, etc.)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )

            try:
                # Step 1: Search
                search_page = await context.new_page()
                listings = await get_search_listings(search_page, brand)
                await search_page.close()
                log.info(f"  Found {len(listings)} listings")

                if not listings:
                    await context.close()
                    continue

                # Step 2: Enrich each listing with detail page data
                detail_page = await context.new_page()
                for i, listing in enumerate(listings):
                    listings[i] = await enrich_listing(detail_page, listing)
                    if (i + 1) % 10 == 0:
                        log.info(f"  Progress: {i+1}/{len(listings)}")
                await detail_page.close()

                # Format and collect
                for listing in listings:
                    record = {field: listing.get(field, "") for field in CSV_FIELDS}
                    record["CC"] = _format_cc(record.get("CC", ""))
                    all_records.append(record)

                log.info(f"  ✅ {brand}: {len(listings)} records")

            except Exception as e:
                log.error(f"  ❌ {brand}: {e}")

            finally:
                await context.close()

            await asyncio.sleep(2)

        await browser.close()

    # Write today's dated CSV
    if all_records:
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(all_records)

        # ── Merge into cumulative all-time CSV ──
        all_data_file = output_dir / "trademe_cars_all.csv"
        existing: dict[str, dict] = {}

        # Load existing records keyed by ListingUrl
        if all_data_file.exists():
            with open(all_data_file, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    key = row.get("ListingUrl", "")
                    if key:
                        existing[key] = row

        prev_count = len(existing)

        # Merge: new records overwrite old ones with the same URL
        for record in all_records:
            key = record.get("ListingUrl", "")
            if key:
                existing[key] = record

        merged = list(existing.values())

        with open(all_data_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(merged)

        added = len(existing) - prev_count
        log.info("")
        log.info("=" * 60)
        log.info(f"  ✅ Today: {len(all_records)} records")
        log.info(f"  ➕ New listings added: {added}")
        log.info(f"  📊 All-time total: {len(merged)} records")
        log.info(f"  📁 {output_file}")
        log.info(f"  📁 {all_data_file}")
        log.info("=" * 60)
    else:
        log.warning("⚠ No records collected!")

    return output_file


# ─── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trade Me Motors Playwright Scraper")
    parser.add_argument("--brands", nargs="+", default=BRANDS, help="Brands to scrape")
    parser.add_argument("--output", default="output", help="Output directory")
    parser.add_argument("--headful", action="store_true", help="Show browser window")
    args = parser.parse_args()

    asyncio.run(scrape_all(
        brands=args.brands,
        output_dir=Path(args.output),
        headless=not args.headful,
    ))
