#!/usr/bin/env python3
import asyncio, csv, sys
sys.path.insert(0, '/Users/mujahidulhaqtuhin/trade_me_cars')
from backfill_vin_plate import enrich_one
from playwright.async_api import async_playwright

async def test():
    records = []
    with open('output/trademe_cars_all.csv') as f:
        for r in csv.DictReader(f):
            if not r.get('VIN','').strip() or not r.get('Plate','').strip():
                records.append(dict(r))
            if len(records) >= 6:
                break

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        semaphore = asyncio.Semaphore(5)

        tasks = [enrich_one(context, r, semaphore) for r in records]
        results = await asyncio.gather(*tasks)

        for r in results:
            vin = r.get('VIN','')
            plate = r.get('Plate','')
            print(f"VIN={vin!r:22} Plate={plate!r:12} {r['Maker']} | {r['ListingUrl'][-40:]}")

        await context.close()
        await browser.close()

asyncio.run(test())
