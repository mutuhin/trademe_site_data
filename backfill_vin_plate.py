#!/usr/bin/env python3
"""
Back-fill VIN and Plate (and other missing fields) for records in trademe_cars_all.csv
without re-running the full brand search.

Usage:
    python backfill_vin_plate.py [--concurrency 20] [--output output/]
"""

import asyncio
import csv
import json
import logging
import re
import argparse
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PwTimeout

# ─── Configuration ───────────────────────────────────────────────────────────

CONCURRENT_PAGES = 20       # Higher than main scraper since no search pages
PAGE_WAIT_MS = 3500         # ms to wait after domcontentloaded (API responses arrive 2.2-2.6s after start)
DETAIL_DELAY = 0.5          # seconds between page batches
SAVE_EVERY = 200            # save CSV + checkpoint every N records processed
CHECKPOINT_FILE = Path("output/.backfill_checkpoint.json")

CSV_FIELDS = [
    "VIN", "Plate", "Year", "Maker", "Model", "Submodel", "CC",
    "Fuel", "Transmission", "FirstReg", "BodyStyle", "ListingDate", "ListingUrl"
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("backfill")


# ─── Helpers (copied from main scraper) ──────────────────────────────────────

def _normalize_first_reg(val) -> str:
    if not val:
        return ""
    s = str(val).strip()
    if re.search(r'nz\s*new', s, re.I):
        return "NZ New"
    if re.search(r'import', s, re.I):
        return "Imported"
    if re.search(r'\d{4}|\d{1,2}[/\-]\d', s):
        return ""
    return s


def _normalize_listing_date(val: str) -> str:
    if not val:
        return ""
    s = str(val)
    if "/Date(" in s or re.match(r'^\d{4}-\d{2}-\d{2}', s) or re.match(r'^\d{10,}$', s):
        return ""
    return s.strip()


def _format_cc(val) -> str:
    if not val:
        return ""
    s = re.sub(r'[cC][cC]$', '', str(val).strip()).strip()
    return (s + "cc") if re.match(r'^\d+$', s) else str(val).strip()


class APICapture:
    def __init__(self):
        self.captured_responses = []

    async def handle_response(self, response):
        url = response.url
        if any(kw in url for kw in ["/api/", "/search", "motors", "listing", "SearchResults", "MotorsSearch", "graphql"]):
            try:
                if "json" in (response.headers.get("content-type", "") or ""):
                    body = await response.json()
                    self.captured_responses.append({"url": url, "data": body})
            except Exception:
                pass


def _enrich_from_api_response(data: dict, listing: dict) -> dict:
    if not isinstance(data, dict):
        return listing

    def pick(current, *candidates):
        if current:
            return current
        for c in candidates:
            if c:
                return str(c)
        return current

    flat = {}
    def _flatten(d):
        if isinstance(d, dict):
            for k, v in d.items():
                key = k.lower().replace(" ", "").replace("_", "").replace("-", "")
                if isinstance(v, (str, int, float)) and v:
                    flat[key] = str(v)
                elif isinstance(v, dict):
                    _flatten(v)
                elif isinstance(v, list):
                    for item in v:
                        _flatten(item)
    _flatten(data)

    listing["VIN"] = pick(listing["VIN"],
        flat.get("vin"), flat.get("chassisnumber"), flat.get("vehicleidentificationnumber"))
    listing["Plate"] = pick(listing["Plate"],
        flat.get("numberplate"), flat.get("plate"), flat.get("registrationplate"), flat.get("rego"))
    listing["Fuel"] = pick(listing["Fuel"], flat.get("fueltype"), flat.get("fuel"))
    listing["CC"] = pick(listing["CC"], flat.get("enginesize"), flat.get("enginecapacity"), flat.get("cc"))
    listing["Submodel"] = pick(listing["Submodel"], flat.get("submodel"), flat.get("variant"), flat.get("badge"))
    listing["BodyStyle"] = pick(listing["BodyStyle"], flat.get("bodystyle"), flat.get("bodytype"), flat.get("body"))
    if not listing["FirstReg"]:
        origin = flat.get("origin") or flat.get("registration") or ""
        if not origin:
            is_nz = flat.get("isnznew") or flat.get("nznew")
            if is_nz in ("true", "1"):
                origin = "NZ New"
            elif is_nz in ("false", "0"):
                origin = "Imported"
        listing["FirstReg"] = _normalize_first_reg(origin)
    return listing


def _enrich_from_next_data_detail(next_data: dict, listing: dict) -> dict:
    try:
        props = next_data.get("props", {}).get("pageProps", {})
        detail = None
        for key in ["listingDetail", "listing", "data", "motorListing", "vehicle"]:
            detail = props.get(key)
            if detail and isinstance(detail, dict):
                break
        if not detail:
            detail = props

        motor = detail.get("motor", {}) or detail.get("vehicle", {}) or detail.get("motorAttributes", {}) or {}

        attrs = {}
        for attr in (detail.get("attributes", []) or motor.get("attributes", []) or []):
            raw_name = str(attr.get("name", "") or attr.get("label", "") or attr.get("displayName", ""))
            value = str(attr.get("value", "") or attr.get("displayValue", "") or attr.get("display", ""))
            if raw_name and value:
                attrs[raw_name.lower().replace(" ", "_")] = value
                attrs[raw_name.lower().replace(" ", "")] = value
                attrs[raw_name.lower()] = value

        def pick(current, *candidates):
            if current:
                return current
            for c in candidates:
                if c:
                    return str(c)
            return current

        listing["VIN"] = pick(listing["VIN"],
            attrs.get("vin"), attrs.get("chassis_number"), attrs.get("chassisnumber"),
            attrs.get("vehicle_identification_number"),
            motor.get("vin"), motor.get("chassisNumber"), detail.get("vin"))
        listing["Plate"] = pick(listing["Plate"],
            attrs.get("number_plate"), attrs.get("numberplate"), attrs.get("number plate"),
            attrs.get("plate"), attrs.get("registration_plate"), attrs.get("registrationplate"),
            attrs.get("rego"),
            motor.get("numberPlate"), motor.get("plate"), detail.get("numberPlate"))
        listing["Fuel"] = pick(listing["Fuel"],
            motor.get("fuelType"), attrs.get("fuel_type"), attrs.get("fuel"), detail.get("fuelType"))
        listing["CC"] = pick(listing["CC"],
            motor.get("engineSize"), attrs.get("engine_size"), attrs.get("cc"),
            attrs.get("engine_capacity"), detail.get("engineSize"))
        listing["Transmission"] = pick(listing["Transmission"],
            motor.get("transmission"), attrs.get("transmission"), detail.get("transmission"))
        listing["Submodel"] = pick(listing["Submodel"],
            motor.get("submodel"), motor.get("variant"), attrs.get("submodel"),
            attrs.get("variant"), attrs.get("badge"), detail.get("variant"))
        listing["BodyStyle"] = pick(listing["BodyStyle"],
            motor.get("bodyStyle"), attrs.get("body_style"), attrs.get("body"),
            attrs.get("body_type"), detail.get("bodyStyle"))
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
        if not listing.get("ListingDate"):
            raw_date = (detail.get("ageGroup") or detail.get("dateText") or
                        detail.get("listedText") or detail.get("listingAgeText") or "")
            listing["ListingDate"] = _normalize_listing_date(raw_date)
    except Exception as e:
        log.debug(f"next_data_detail error: {e}")
    return listing


def _enrich_from_json_ld(ld: dict, listing: dict) -> dict:
    if not isinstance(ld, dict):
        return listing
    if ld.get("@type", "") not in ["Car", "Vehicle", "Product", "Offer"]:
        return listing
    def pick(current, val):
        return current if current else (str(val) if val else current)
    listing["VIN"] = pick(listing["VIN"], ld.get("vehicleIdentificationNumber"))
    listing["Fuel"] = pick(listing["Fuel"], ld.get("fuelType"))
    listing["BodyStyle"] = pick(listing["BodyStyle"], ld.get("bodyType"))
    listing["Transmission"] = pick(listing["Transmission"], ld.get("vehicleTransmission"))
    return listing


def _enrich_from_html_text(html: str, listing: dict) -> dict:
    text = re.sub(r'<[^>]+>', ' ', html)
    patterns = {
        "VIN": [
            r"(?:VIN|Chassis(?:\s*No\.?)?|Vehicle\s*Identification\s*Number)[:\s]+([A-HJ-NPR-Z0-9]{17})",
            r"\b([A-HJ-NPR-Z0-9]{17})\b",
        ],
        "Plate": [
            r"(?:Number\s+plate|Plate|Rego|Registration)[:\s]+([A-Z]{1,3}[0-9]{1,4}[A-Z]{0,3})",
            r"(?:Number\s+plate|Plate|Rego)[:\s\n]+([A-Z0-9]{2,7})",
        ],
        "CC": [r"(?:Engine size|Engine|CC|Capacity)[:\s]+(\d[\d,]*)\s*(?:cc)?"],
        "Fuel": [r"(?:Fuel type|Fuel)[:\s]+(Petrol|Diesel|Electric|Hybrid|Plug.in Hybrid|LPG|CNG|BEV|PHEV|HEV)"],
        "Transmission": [r"(?:Transmission|Gearbox)[:\s]+(Automatic|Manual|CVT|DCT|DSG|Auto|Tiptronic|Steptronic)"],
        "FirstReg": [
            r"(?:Origin|Registration)[:\s]+(NZ\s*[Nn]ew|Imported)",
            r"\b(NZ\s*[Nn]ew)\b",
            r"\b(Imported)\b",
        ],
        "BodyStyle": [r"(?:Body style|Body)[:\s]+(Sedan|Hatchback|SUV|Wagon|Coupe|Convertible|Ute|Van|Station Wagon|Liftback|Roadster|Fastback|Cabriolet|Pickup|People Mover|Cab Chassis)"],
    }
    for field, regexes in patterns.items():
        if not listing.get(field):
            for regex in regexes:
                m = re.search(regex, text, re.IGNORECASE)
                if m:
                    listing[field] = m.group(1).strip()
                    break
    return listing


def _enrich_from_html_kv(html: str, listing: dict) -> dict:
    kv = {}
    for dt, dd in re.findall(r'<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', html, re.DOTALL | re.IGNORECASE):
        key = re.sub(r'<[^>]+>', '', dt).strip().lower()
        val = re.sub(r'<[^>]+>', '', dd).strip()
        if key and val:
            kv[key] = val
    for th, td in re.findall(r'<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>', html, re.DOTALL | re.IGNORECASE):
        key = re.sub(r'<[^>]+>', '', th).strip().lower()
        val = re.sub(r'<[^>]+>', '', td).strip()
        if key and val:
            kv[key] = val
    field_map = {
        "VIN": ["vin", "chassis number", "chassis", "chassis no", "chassis no.", "vehicle identification number", "chassisnumber"],
        "Plate": ["number plate", "numberplate", "plate", "registration plate", "rego", "number_plate"],
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
                if key in kv and kv[key]:
                    listing[field] = kv[key]
                    break
    return listing


# ─── Checkpoint ───────────────────────────────────────────────────────────────

def load_checkpoint(cp_file: Path) -> set:
    if not cp_file.exists():
        return set()
    try:
        with open(cp_file) as f:
            data = json.load(f)
        done = set(data.get("completed_urls", []))
        log.info(f"Checkpoint: {len(done)} URLs already processed")
        return done
    except Exception as e:
        log.warning(f"Could not load checkpoint: {e}")
        return set()


def save_checkpoint(cp_file: Path, completed_urls: set):
    try:
        cp_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cp_file, "w") as f:
            json.dump({"completed_urls": list(completed_urls)}, f)
    except Exception as e:
        log.warning(f"Could not save checkpoint: {e}")


# ─── Core Enrichment ──────────────────────────────────────────────────────────

async def enrich_one(context, listing: dict, semaphore: asyncio.Semaphore) -> dict:
    url = listing.get("ListingUrl", "")
    if not url:
        return listing
    async with semaphore:
        page = await context.new_page()
        api_capture = APICapture()
        page.on("response", api_capture.handle_response)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(PAGE_WAIT_MS)

            # Strategy 1: Directly fetch the motors API (most reliable — carries VIN/Plate)
            listing_id = url.rstrip("/").split("/")[-1]
            if listing_id.isdigit():
                motors_data = await page.evaluate(f"""
                    async () => {{
                        try {{
                            const r = await fetch(
                                'https://api.trademe.co.nz/v1/motors/{listing_id}.json',
                                {{headers: {{"Accept": "application/json"}}}}
                            );
                            if (!r.ok) return null;
                            return await r.json();
                        }} catch(e) {{ return null; }}
                    }}
                """)
                if motors_data:
                    listing = _enrich_from_api_response(motors_data, listing)

            # Strategy 2: __NEXT_DATA__ embedded JSON
            next_data = await page.evaluate("""
                () => {
                    const el = document.getElementById('__NEXT_DATA__');
                    if (el) { try { return JSON.parse(el.textContent); } catch(e) {} }
                    return null;
                }
            """)
            if next_data:
                listing = _enrich_from_next_data_detail(next_data, listing)

            # Strategy 3: Intercepted API responses (bonus — any other API calls)
            for cap in api_capture.captured_responses:
                listing = _enrich_from_api_response(cap["data"], listing)

            json_ld = await page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                    return Array.from(scripts).map(s => {
                        try { return JSON.parse(s.textContent); } catch(e) { return null; }
                    }).filter(Boolean);
                }
            """)
            for ld in (json_ld or []):
                listing = _enrich_from_json_ld(ld, listing)

            html = await page.content()
            listing = _enrich_from_html_text(html, listing)
            listing = _enrich_from_html_kv(html, listing)

            listing["FirstReg"] = _normalize_first_reg(listing.get("FirstReg", ""))
            listing["CC"] = _format_cc(listing.get("CC", ""))
            listing["ListingDate"] = _normalize_listing_date(listing.get("ListingDate", ""))

        except PwTimeout:
            log.warning(f"  Timeout: {url}")
        except Exception as e:
            log.warning(f"  Error: {url} — {e}")
        finally:
            page.remove_listener("response", api_capture.handle_response)
            await page.close()

    await asyncio.sleep(DETAIL_DELAY)
    return listing


def save_csv(all_records: list, output_file: Path):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_records)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def run(output_dir: Path, concurrency: int, headless: bool):
    all_csv = output_dir / "trademe_cars_all.csv"
    if not all_csv.exists():
        log.error(f"Not found: {all_csv}")
        return

    # Load all records into a dict keyed by ListingUrl
    records_by_url: dict[str, dict] = {}
    with open(all_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            url = row.get("ListingUrl", "").strip()
            if url:
                records_by_url[url] = dict(row)

    total = len(records_by_url)
    log.info(f"Loaded {total} total records from {all_csv}")

    # Filter to those missing VIN or Plate
    to_backfill = [
        r for r in records_by_url.values()
        if not r.get("VIN", "").strip() or not r.get("Plate", "").strip()
    ]
    log.info(f"Records needing backfill: {len(to_backfill)}")

    # Load checkpoint (skip already processed URLs)
    cp_file = output_dir / ".backfill_checkpoint.json"
    completed_urls = load_checkpoint(cp_file)
    pending = [r for r in to_backfill if r.get("ListingUrl", "") not in completed_urls]
    log.info(f"Pending (not yet processed): {len(pending)}")

    if not pending:
        log.info("All records already backfilled. Done.")
        return

    semaphore = asyncio.Semaphore(concurrency)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
        )

        processed = 0
        vin_found = 0
        plate_found = 0

        # Process in batches of SAVE_EVERY for checkpoint saves
        batch_size = SAVE_EVERY
        for batch_start in range(0, len(pending), batch_size):
            batch = pending[batch_start: batch_start + batch_size]

            tasks = [enrich_one(context, dict(r), semaphore) for r in batch]
            results = await asyncio.gather(*tasks)

            for enriched in results:
                url = enriched.get("ListingUrl", "")
                old = records_by_url.get(url, {})

                # Track improvement
                had_vin = bool(old.get("VIN", "").strip())
                had_plate = bool(old.get("Plate", "").strip())
                now_vin = bool(enriched.get("VIN", "").strip())
                now_plate = bool(enriched.get("Plate", "").strip())
                if not had_vin and now_vin:
                    vin_found += 1
                if not had_plate and now_plate:
                    plate_found += 1

                # Merge: prefer new non-empty value, keep old if new is empty
                merged = dict(old)
                for field in CSV_FIELDS:
                    new_val = enriched.get(field, "").strip() if isinstance(enriched.get(field), str) else str(enriched.get(field, ""))
                    if new_val and not merged.get(field, "").strip():
                        merged[field] = new_val

                records_by_url[url] = merged
                completed_urls.add(url)
                processed += 1

            # Save progress
            all_updated = list(records_by_url.values())
            save_csv(all_updated, all_csv)
            save_checkpoint(cp_file, completed_urls)

            pct = processed / len(pending) * 100
            log.info(
                f"Progress: {processed}/{len(pending)} ({pct:.0f}%) | "
                f"+VIN: {vin_found} | +Plate: {plate_found}"
            )

        await context.close()
        await browser.close()

    log.info("=" * 60)
    log.info(f"Backfill complete: {processed} records processed")
    log.info(f"VIN newly found: {vin_found}")
    log.info(f"Plate newly found: {plate_found}")

    # Clean up checkpoint on success
    if cp_file.exists():
        cp_file.unlink()
        log.info("Checkpoint removed (clean run)")


def main():
    parser = argparse.ArgumentParser(description="Back-fill VIN/Plate for trademe_cars_all.csv")
    parser.add_argument("--output", default="output", help="Output directory (default: output)")
    parser.add_argument("--concurrency", type=int, default=CONCURRENT_PAGES,
                        help=f"Concurrent Playwright pages (default: {CONCURRENT_PAGES})")
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    args = parser.parse_args()

    output_dir = Path(args.output)
    asyncio.run(run(output_dir, args.concurrency, headless=not args.no_headless))


if __name__ == "__main__":
    main()
