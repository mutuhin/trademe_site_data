#!/usr/bin/env python3
"""
Trade Me Motors Car Scraper
===========================
Scrapes car listings from Trade Me NZ for specified brands.

Approach:
  1. Uses Trade Me's public API (api.trademe.co.nz) for search results (no auth needed)
  2. Uses Playwright to visit each listing page for detailed fields (VIN, Plate, etc.)

Usage:
  pip install playwright aiohttp aiofiles pandas
  playwright install chromium
  python trademe_scraper.py

Daily scheduling: use --cron flag or set up via crontab / Task Scheduler.
"""

import asyncio
import aiohttp
import json
import csv
import os
import re
import logging
import argparse
from datetime import datetime, date
from pathlib import Path
from typing import Optional

# Playwright import (async)
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ─── Configuration ───────────────────────────────────────────────────────────

BRANDS = [
    "alfa-romeo", "aston-martin", "audi", "bentley", "bmw", "citroen",
    "cupra", "ds-automobiles", "ferrari", "fiat", "ford", "holden",
    "jaguar", "lancia", "land-rover", "mercedes-benz", "mini", "opel",
    "peugeot", "polestar", "porsche", "renault", "rolls-royce", "rover",
    "saab", "seat", "skoda", "smart", "vauxhall", "volkswagen", "volvo"
]

# Trade Me public API endpoint (no auth required, 60 req/hr unauthenticated)
API_BASE = "https://api.trademe.co.nz/v1"
SEARCH_ENDPOINT = f"{API_BASE}/Search/Motors/Used.json"

# Web search URL (fallback / for Playwright)
WEB_SEARCH_URL = "https://www.trademe.co.nz/a/motors/cars/search"

# Output
OUTPUT_DIR = Path("output")
CSV_FIELDS = [
    "VIN", "Plate", "Year", "Maker", "Model", "Submodel", "CC",
    "Fuel", "Transmission", "FirstReg", "BodyStyle", "ListingDate", "ListingUrl"
]

# Rate limiting
API_DELAY = 1.5          # seconds between API calls (respect 60/hr limit)
PAGE_DELAY = 2.0         # seconds between Playwright page loads
MAX_CONCURRENT_PAGES = 3 # concurrent Playwright detail page loads
MAX_API_PAGES = 50       # max pagination pages per brand (50 results/page = 2500 listings)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("trademe_scraper")


# ─── API Search ──────────────────────────────────────────────────────────────

async def search_brand_api(session: aiohttp.ClientSession, brand: str) -> list[dict]:
    """
    Search Trade Me API for a brand. Returns list of basic listing dicts.
    The API returns up to 50 results per page.
    """
    all_listings = []
    page = 1

    while page <= MAX_API_PAGES:
        params = {
            "search_string": brand,
            "rows": 50,
            "page": page,
            "sort_order": "Default",
        }

        try:
            async with session.get(SEARCH_ENDPOINT, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 429:
                    logger.warning(f"Rate limited on API for {brand}, page {page}. Waiting 60s...")
                    await asyncio.sleep(60)
                    continue
                if resp.status != 200:
                    logger.error(f"API error {resp.status} for {brand} page {page}")
                    break

                data = await resp.json()

        except Exception as e:
            logger.error(f"API request failed for {brand} page {page}: {e}")
            break

        listings = data.get("List", [])
        if not listings:
            break

        total_count = data.get("TotalCount", 0)
        logger.info(f"  {brand} page {page}: got {len(listings)} listings (total: {total_count})")

        for item in listings:
            listing_id = item.get("ListingId", "")
            listing_url = f"https://www.trademe.co.nz/a/motors/cars/{brand}/{listing_id}"

            # Extract what we can from API response
            record = {
                "ListingId": listing_id,
                "ListingUrl": listing_url,
                "Year": item.get("Year", ""),
                "Maker": brand.replace("-", " ").title(),
                "Model": item.get("Title", "").replace(item.get("Year", ""), "").strip() if item.get("Year") else item.get("Title", ""),
                "ListingDate": item.get("StartDate", item.get("AsAt", "")),
                "BodyStyle": item.get("BodyStyle", ""),
                "Transmission": item.get("Transmission", ""),
                "EngineSize": item.get("EngineSize", ""),
                # Fields we need from detail page:
                "VIN": "",
                "Plate": "",
                "Submodel": "",
                "CC": item.get("EngineSize", ""),
                "Fuel": "",
                "FirstReg": "",
            }
            all_listings.append(record)

        # Check if we've got all results
        if len(all_listings) >= total_count:
            break

        page += 1
        await asyncio.sleep(API_DELAY)

    return all_listings


async def search_brand_web_fallback(session: aiohttp.ClientSession, brand: str) -> list[dict]:
    """
    Fallback: scrape search results from the web page if API fails.
    Uses the web search URL and parses JSON from __NEXT_DATA__ or listing cards.
    """
    all_listings = []
    page = 1

    while page <= MAX_API_PAGES:
        url = f"{WEB_SEARCH_URL}?search_string={brand}&page={page}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    break
                html = await resp.text()
        except Exception as e:
            logger.error(f"Web fallback failed for {brand} page {page}: {e}")
            break

        # Try to extract __NEXT_DATA__ JSON (Next.js SSR pages embed this)
        next_data_match = re.search(
            r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
            html, re.DOTALL
        )
        if next_data_match:
            try:
                next_data = json.loads(next_data_match.group(1))
                search_results = (
                    next_data.get("props", {})
                    .get("pageProps", {})
                    .get("searchResults", {})
                    .get("results", [])
                )
                if not search_results:
                    break

                for item in search_results:
                    listing_url = "https://www.trademe.co.nz" + item.get("url", "")
                    record = {
                        "ListingId": item.get("listingId", ""),
                        "ListingUrl": listing_url,
                        "Year": item.get("year", ""),
                        "Maker": brand.replace("-", " ").title(),
                        "Model": item.get("title", ""),
                        "ListingDate": item.get("listingDate", item.get("startDate", "")),
                        "BodyStyle": item.get("bodyStyle", ""),
                        "Transmission": item.get("transmission", ""),
                        "CC": item.get("engineSize", ""),
                        "VIN": "",
                        "Plate": "",
                        "Submodel": "",
                        "Fuel": "",
                        "FirstReg": "",
                    }
                    all_listings.append(record)
            except json.JSONDecodeError:
                pass
        else:
            # Fallback: parse listing links from HTML
            listing_links = re.findall(
                r'href="(/a/motors/cars/[^"]+/listing/(\d+))"', html
            )
            if not listing_links:
                # Also try alternative URL patterns
                listing_links = re.findall(
                    r'href="(/a/motors/cars/[^"]+?/(\d+))"', html
                )
            if not listing_links:
                break

            for href, lid in listing_links:
                listing_url = f"https://www.trademe.co.nz{href}"
                record = {
                    "ListingId": lid,
                    "ListingUrl": listing_url,
                    "Year": "",
                    "Maker": brand.replace("-", " ").title(),
                    "Model": "",
                    "ListingDate": "",
                    "BodyStyle": "",
                    "Transmission": "",
                    "CC": "",
                    "VIN": "",
                    "Plate": "",
                    "Submodel": "",
                    "Fuel": "",
                    "FirstReg": "",
                }
                all_listings.append(record)

        page += 1
        await asyncio.sleep(API_DELAY)

    return all_listings


# ─── Playwright Detail Scraper ───────────────────────────────────────────────

async def scrape_listing_detail(page, listing: dict, semaphore: asyncio.Semaphore) -> dict:
    """
    Visit a single listing page with Playwright and extract detailed fields.
    """
    async with semaphore:
        url = listing["ListingUrl"]
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(1500)

            # Strategy 1: Try to extract from __NEXT_DATA__ JSON embedded in page
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
                listing = extract_from_next_data(next_data, listing)
            else:
                # Strategy 2: Parse from page content
                listing = await extract_from_page(page, listing)

            logger.info(f"    ✓ Scraped detail: {listing.get('Year', '')} {listing.get('Maker', '')} {listing.get('Model', '')}")

        except PlaywrightTimeout:
            logger.warning(f"    ✗ Timeout loading {url}")
        except Exception as e:
            logger.warning(f"    ✗ Error scraping {url}: {e}")

        await asyncio.sleep(PAGE_DELAY)
        return listing


def extract_from_next_data(next_data: dict, listing: dict) -> dict:
    """Extract fields from the __NEXT_DATA__ JSON blob."""
    try:
        props = next_data.get("props", {}).get("pageProps", {})

        # The listing detail is usually nested under a key like "listing" or "listingDetail"
        detail = (
            props.get("listingDetail", {}) or
            props.get("listing", {}) or
            props.get("data", {}).get("listing", {}) or
            props
        )

        # Also check for vehicle-specific attributes
        attributes = detail.get("attributes", [])
        attr_dict = {}
        for attr in attributes:
            name = attr.get("name", "").lower().replace(" ", "_")
            value = attr.get("value", "") or attr.get("displayValue", "")
            attr_dict[name] = value

        # Also check for a "motor" or "vehicle" sub-object
        motor = detail.get("motor", {}) or detail.get("vehicle", {}) or {}

        # Map fields
        listing["VIN"] = (
            attr_dict.get("vin", "") or
            motor.get("vin", "") or
            detail.get("vin", "")
        )
        listing["Plate"] = (
            attr_dict.get("plate", "") or
            attr_dict.get("registration_plate", "") or
            attr_dict.get("number_plate", "") or
            motor.get("numberPlate", "") or
            motor.get("plate", "") or
            detail.get("numberPlate", "")
        )
        listing["Year"] = listing["Year"] or (
            str(motor.get("year", "")) or
            attr_dict.get("year", "") or
            str(detail.get("year", ""))
        )
        listing["Model"] = (
            motor.get("model", "") or
            attr_dict.get("model", "") or
            detail.get("model", "") or
            listing["Model"]
        )
        listing["Submodel"] = (
            motor.get("submodel", "") or
            attr_dict.get("submodel", "") or
            attr_dict.get("variant", "") or
            motor.get("variant", "") or
            detail.get("variant", "")
        )
        listing["CC"] = listing["CC"] or (
            str(motor.get("engineSize", "")) or
            attr_dict.get("engine_size", "") or
            attr_dict.get("cc", "")
        )
        listing["Fuel"] = (
            motor.get("fuelType", "") or
            attr_dict.get("fuel_type", "") or
            attr_dict.get("fuel", "") or
            detail.get("fuelType", "")
        )
        listing["Transmission"] = listing["Transmission"] or (
            motor.get("transmission", "") or
            attr_dict.get("transmission", "")
        )
        listing["FirstReg"] = (
            motor.get("firstRegistered", "") or
            attr_dict.get("first_registered", "") or
            attr_dict.get("registration_date", "") or
            detail.get("firstRegistered", "")
        )
        listing["BodyStyle"] = listing["BodyStyle"] or (
            motor.get("bodyStyle", "") or
            attr_dict.get("body_style", "") or
            attr_dict.get("body", "")
        )
        listing["ListingDate"] = listing["ListingDate"] or (
            detail.get("startDate", "") or
            detail.get("listingDate", "")
        )

    except Exception as e:
        logger.debug(f"Error extracting from __NEXT_DATA__: {e}")

    return listing


async def extract_from_page(page, listing: dict) -> dict:
    """
    Fallback: extract fields from the rendered HTML using selectors.
    Trade Me listing pages typically show vehicle details in a key-value table.
    """
    try:
        # Get all text content for regex extraction
        body_text = await page.inner_text("body")

        # Common patterns in Trade Me listing detail pages
        patterns = {
            "VIN": [
                r"VIN[:\s]+([A-HJ-NPR-Z0-9]{17})",
                r"Chassis[:\s]+([A-HJ-NPR-Z0-9]{17})",
            ],
            "Plate": [
                r"(?:Plate|Registration|Rego|Number plate)[:\s]+([A-Z0-9]{1,7})",
                r"(?:Plate|Registration)[:\s]+([\w]+)",
            ],
            "Year": [
                r"Year[:\s]+(\d{4})",
            ],
            "Model": [
                r"Model[:\s]+([\w\s\-]+?)(?:\n|$)",
            ],
            "Submodel": [
                r"(?:Submodel|Variant|Badge)[:\s]+([\w\s\-\.]+?)(?:\n|$)",
            ],
            "CC": [
                r"(?:Engine size|Engine|CC|Capacity)[:\s]+(\d[\d,]*)\s*(?:cc)?",
            ],
            "Fuel": [
                r"(?:Fuel type|Fuel)[:\s]+(Petrol|Diesel|Electric|Hybrid|LPG|CNG|Plug-in Hybrid|PHEV|BEV|HEV)",
            ],
            "Transmission": [
                r"(?:Transmission|Gearbox)[:\s]+(Automatic|Manual|CVT|DCT|DSG|Auto|Tiptronic|Steptronic)",
            ],
            "FirstReg": [
                r"(?:First registered|First reg|Registration date)[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
                r"(?:First registered|First reg)[:\s]+(\w+\s+\d{4})",
            ],
            "BodyStyle": [
                r"(?:Body style|Body|Style)[:\s]+(Sedan|Hatchback|SUV|Wagon|Coupe|Convertible|Ute|Van|Cab Chassis|Station Wagon|Liftback|Roadster|Fastback|Cabriolet|Pickup|People Mover|Bus)",
            ],
        }

        for field, regex_list in patterns.items():
            if not listing.get(field):
                for regex in regex_list:
                    match = re.search(regex, body_text, re.IGNORECASE)
                    if match:
                        listing[field] = match.group(1).strip()
                        break

        # Try extracting from structured data (JSON-LD)
        json_ld_scripts = await page.query_selector_all('script[type="application/ld+json"]')
        for script in json_ld_scripts:
            try:
                content = await script.inner_text()
                ld_data = json.loads(content)
                if isinstance(ld_data, dict):
                    if ld_data.get("@type") in ["Car", "Vehicle", "Product"]:
                        listing["VIN"] = listing["VIN"] or ld_data.get("vehicleIdentificationNumber", "")
                        listing["Model"] = listing["Model"] or ld_data.get("model", "")
                        listing["Maker"] = listing["Maker"] or ld_data.get("brand", {}).get("name", "")
                        listing["Fuel"] = listing["Fuel"] or ld_data.get("fuelType", "")
                        listing["BodyStyle"] = listing["BodyStyle"] or ld_data.get("bodyType", "")
            except (json.JSONDecodeError, Exception):
                continue

        # Try to get listing date from page metadata or visible elements
        if not listing.get("ListingDate"):
            date_match = re.search(r"Listed[:\s]+(\d{1,2}\s+\w+\s+\d{4})", body_text)
            if date_match:
                listing["ListingDate"] = date_match.group(1)

    except Exception as e:
        logger.debug(f"Error extracting from page HTML: {e}")

    return listing


# ─── Playwright Search Scraper (Alternative to API) ─────────────────────────

async def search_brand_playwright(browser, brand: str) -> list[dict]:
    """
    Alternative: use Playwright to scrape search results directly.
    Useful if the API is down or rate-limited.
    """
    all_listings = []
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    page = await context.new_page()

    try:
        pg = 1
        while pg <= MAX_API_PAGES:
            url = f"{WEB_SEARCH_URL}?search_string={brand}&page={pg}"
            logger.info(f"  [Playwright] Loading search page: {url}")

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # Try __NEXT_DATA__ first
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

            found_this_page = 0

            if next_data:
                try:
                    results = (
                        next_data.get("props", {})
                        .get("pageProps", {})
                        .get("searchResults", {})
                        .get("results", [])
                    )
                    if not results:
                        # Try alternative paths
                        results = (
                            next_data.get("props", {})
                            .get("pageProps", {})
                            .get("data", {})
                            .get("searchResults", [])
                        )
                    for item in results:
                        lurl = item.get("url", "")
                        if not lurl.startswith("http"):
                            lurl = "https://www.trademe.co.nz" + lurl
                        record = {
                            "ListingId": item.get("listingId", ""),
                            "ListingUrl": lurl,
                            "Year": str(item.get("year", "")),
                            "Maker": brand.replace("-", " ").title(),
                            "Model": item.get("title", ""),
                            "ListingDate": item.get("startDate", ""),
                            "BodyStyle": item.get("bodyStyle", ""),
                            "Transmission": item.get("transmission", ""),
                            "CC": str(item.get("engineSize", "")),
                            "VIN": "",
                            "Plate": "",
                            "Submodel": "",
                            "Fuel": item.get("fuelType", ""),
                            "FirstReg": "",
                        }
                        all_listings.append(record)
                        found_this_page += 1
                except Exception as e:
                    logger.debug(f"Error parsing __NEXT_DATA__: {e}")

            if found_this_page == 0:
                # Fallback: extract listing cards from DOM
                listing_cards = await page.query_selector_all('a[href*="/motors/cars/"]')
                seen_urls = set()
                for card in listing_cards:
                    href = await card.get_attribute("href")
                    if href and "/listing/" in href and href not in seen_urls:
                        seen_urls.add(href)
                        if not href.startswith("http"):
                            href = "https://www.trademe.co.nz" + href
                        lid = re.search(r'/(\d+)$', href)
                        record = {
                            "ListingId": lid.group(1) if lid else "",
                            "ListingUrl": href,
                            "Year": "",
                            "Maker": brand.replace("-", " ").title(),
                            "Model": "",
                            "ListingDate": "",
                            "BodyStyle": "",
                            "Transmission": "",
                            "CC": "",
                            "VIN": "",
                            "Plate": "",
                            "Submodel": "",
                            "Fuel": "",
                            "FirstReg": "",
                        }
                        all_listings.append(record)
                        found_this_page += 1

            if found_this_page == 0:
                logger.info(f"  No more results for {brand} at page {pg}")
                break

            pg += 1
            await asyncio.sleep(PAGE_DELAY)

    except Exception as e:
        logger.error(f"Playwright search error for {brand}: {e}")
    finally:
        await context.close()

    return all_listings


# ─── Main Orchestrator ───────────────────────────────────────────────────────

async def scrape_brand(browser, session: aiohttp.ClientSession, brand: str, use_playwright_search: bool = False) -> list[dict]:
    """Scrape all listings for a single brand."""

    logger.info(f"🔍 Searching brand: {brand}")

    # Step 1: Get search results (listing URLs)
    if use_playwright_search:
        listings = await search_brand_playwright(browser, brand)
    else:
        listings = await search_brand_api(session, brand)
        if not listings:
            logger.info(f"  API returned nothing for {brand}, trying Playwright fallback...")
            listings = await search_brand_playwright(browser, brand)

    logger.info(f"  Found {len(listings)} listings for {brand}")

    if not listings:
        return []

    # Step 2: Visit each listing for detailed fields
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    # Create a pool of pages
    pages = []
    for _ in range(MAX_CONCURRENT_PAGES):
        pages.append(await context.new_page())

    enriched = []
    for i, listing in enumerate(listings):
        pg = pages[i % MAX_CONCURRENT_PAGES]
        result = await scrape_listing_detail(pg, listing, semaphore)
        enriched.append(result)

        if (i + 1) % 10 == 0:
            logger.info(f"  Progress: {i+1}/{len(listings)} listings enriched for {brand}")

    await context.close()
    return enriched


def format_record(listing: dict) -> dict:
    """Format a listing dict to the output CSV fields."""
    return {
        "VIN": listing.get("VIN", ""),
        "Plate": listing.get("Plate", ""),
        "Year": listing.get("Year", ""),
        "Maker": listing.get("Maker", ""),
        "Model": listing.get("Model", ""),
        "Submodel": listing.get("Submodel", ""),
        "CC": listing.get("CC", ""),
        "Fuel": listing.get("Fuel", ""),
        "Transmission": listing.get("Transmission", ""),
        "FirstReg": listing.get("FirstReg", ""),
        "BodyStyle": listing.get("BodyStyle", ""),
        "ListingDate": listing.get("ListingDate", ""),
        "ListingUrl": listing.get("ListingUrl", ""),
    }


async def main(brands: list[str], output_dir: Path, use_playwright_search: bool = False):
    """Main entry point."""

    output_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    output_file = output_dir / f"trademe_cars_{today}.csv"

    logger.info(f"═══════════════════════════════════════════════════")
    logger.info(f"  Trade Me Motors Scraper - {today}")
    logger.info(f"  Brands: {len(brands)}")
    logger.info(f"  Output: {output_file}")
    logger.info(f"═══════════════════════════════════════════════════")

    all_records = []

    async with aiohttp.ClientSession() as session:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )

            for brand in brands:
                try:
                    listings = await scrape_brand(browser, session, brand, use_playwright_search)
                    records = [format_record(l) for l in listings]
                    all_records.extend(records)
                    logger.info(f"✅ {brand}: {len(records)} records collected")
                except Exception as e:
                    logger.error(f"❌ {brand}: Error - {e}")

                # Brief pause between brands
                await asyncio.sleep(2)

            await browser.close()

    # Write CSV
    if all_records:
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(all_records)

        logger.info(f"")
        logger.info(f"═══════════════════════════════════════════════════")
        logger.info(f"  ✅ DONE: {len(all_records)} total records saved")
        logger.info(f"  📁 File: {output_file}")
        logger.info(f"═══════════════════════════════════════════════════")
    else:
        logger.warning("No records collected!")

    # Also save a "latest" symlink / copy for easy access
    latest_file = output_dir / "trademe_cars_latest.csv"
    if output_file.exists():
        import shutil
        shutil.copy2(output_file, latest_file)

    return all_records


# ─── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trade Me Motors Car Scraper")
    parser.add_argument(
        "--brands", nargs="+", default=BRANDS,
        help="List of brands to scrape (default: all 31 brands)"
    )
    parser.add_argument(
        "--output", type=str, default="output",
        help="Output directory (default: ./output)"
    )
    parser.add_argument(
        "--playwright-search", action="store_true",
        help="Use Playwright for search instead of API (slower but more resilient)"
    )
    parser.add_argument(
        "--brands-only", nargs="+",
        help="Override: scrape only these specific brands"
    )

    args = parser.parse_args()

    brands_to_scrape = args.brands_only if args.brands_only else args.brands

    asyncio.run(main(
        brands=brands_to_scrape,
        output_dir=Path(args.output),
        use_playwright_search=args.playwright_search,
    ))
