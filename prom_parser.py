import requests
import json
import csv
import time
import math
import logging
import concurrent.futures
import sqlite3
from bs4 import BeautifulSoup
from db_setup import upsert_products, setup_database

# SETUP DETAILED LOGGER (Saves to a file AND prints to your screen)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("prom_scraper_engine.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def fetch_and_extract_page(url, page_num, category_alias):
    """
    Downloads a single page. If the server drops the connection or returns an error,
    it attempts an in-line retry with a brief pause before reporting a failure.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    max_inline_retries = 3
    for attempt in range(1, max_inline_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=12)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                for script in soup.find_all('script'):
                    script_text = str(script.string)
                    if 'window.ApolloCacheState' in script_text:
                        start_marker = "window.ApolloCacheState = "
                        start_idx = script_text.find(start_marker) + len(start_marker)
                        json_candidate = script_text[start_idx:].strip()
                        
                        decoder = json.JSONDecoder()
                        cache_data, _ = decoder.raw_decode(json_candidate)
                        fast_cache = cache_data.get('_FAST_CACHE', {})
                        search_key = next((k for k in fast_cache.keys() if 'SearchListingQuery' in k or 'CategoryListingQuery' in k or 'Catalog' in k), None)
                        
                        if search_key:
                            total_items = fast_cache[search_key]['result']['listing']['page']['total']
                            products = fast_cache[search_key]['result']['listing']['page']['products']
                            
                            clean_products = [] 
                            for item in products:
                                product_info = item.get('product', {})
                                
                                if product_info:
                                    # 1. Core Identifiers
                                    product_id = product_info.get('id')
                                    url_product = f"https://prom.ua/ua/p{product_id}-{product_info.get('urlText', '')}.html"
                                    
                                    # 2. Company Info
                                    company = product_info.get('company', {})
                                    stats = company.get('opinionStats', {})
                                    
                                    # 3. Brand / Manufacturer extraction
                                    manufacturer = product_info.get('manufacturerInfo') or {}
                                    brand_name = manufacturer.get('name', 'Unknown')
                                    
                                    # 4. Stock Status extraction
                                    presence = product_info.get('catalogPresence') or {}
                                    stock_status = presence.get('value', 'Unknown')
                                    
                                    # 5. SEO & Advertising Metrics
                                    algo_score = item.get('score', '0')
                                    advert_data = item.get('advert') or {}
                                    is_ad = bool(advert_data)
                                    ad_weight = advert_data.get('advert_weight_adv', '0')
                                    ad_commission = advert_data.get('commission_type', 'organic')

                                    # 6. Append everything to the master dictionary
                                    clean_products.append({
                                        "id": product_id,
                                        "category_alias": category_alias,
                                        "name": product_info.get('name'),
                                        "brand": brand_name,
                                        "price_original": product_info.get('price'),
                                        "price_discounted": product_info.get('discountedPrice', ''),
                                        "stock_status": stock_status,
                                        "sku": product_info.get('sku', ''),
                                        "url_product": url_product,
                                        "company_name": company.get('name', 'Unknown'),
                                        "company_region": company.get('regionName', 'Unknown'),
                                        "company_num_review": f"{stats.get('opinionTotal', '0')}",
                                        "company_per_pos_review": f"{stats.get('opinionPositivePercent', '0')}%",
                                        "algo_score": algo_score,
                                        "is_ad": is_ad,
                                        "ad_weight": ad_weight,
                                        "ad_commission_type": ad_commission
                                    })
                                    
                            if clean_products:
                                return clean_products, total_items
                            
            elif response.status_code in [429, 403]:
                logging.warning(f"⚠️ Page {page_num} hit Status {response.status_code} (Attempt {attempt}/{max_inline_retries}). Pausing...")
                time.sleep(3 * attempt)
            else:
                logging.warning(f"⚠️ Page {page_num} returned abnormal status {response.status_code} on attempt {attempt}")
                
        except Exception as e:
            logging.warning(f"⚠️ Connection glitch on Page {page_num} (Attempt {attempt}/{max_inline_retries}): {e}")
            time.sleep(1)
            
    return [], 0


def analyze_global_catalog(category_tree):
    """
    Analyzes the ENTIRE platform tree from the 'root' up.
    Prints a clean department-level summary for user verification, 
    and returns EVERY single lowest-level leaf category on the site.
    """
    children_map = {}
    for alias, data in category_tree.items():
        parent = data.get("parent_alias", "root")
        if parent not in children_map:
            children_map[parent] = []
        children_map[parent].append(alias)

    all_global_leaf_nodes = []

    def collect_leaves(current_alias):
        leaves = []
        if current_alias in children_map:
            for child_alias in children_map[current_alias]:
                leaves.extend(collect_leaves(child_alias))
        else:
            leaves.append(current_alias)
        return leaves

    print("\n🌳 --- SYSTEM BOUNDS VERIFICATION: GLOBAL CATALOG TREE --- 🌳")
    print(f"Total entries discovered in mapping cache: {len(category_tree)}")
    print("----------------------------------------------------------------")

    top_departments = children_map.get("root", [])
    for dept_alias in top_departments:
        dept_name = category_tree.get(dept_alias, {}).get("name", dept_alias)
        dept_leaves = collect_leaves(dept_alias)
        all_global_leaf_nodes.extend(dept_leaves)
        print(f" 📂 Department: {dept_name:<30} | Alias: {dept_alias:<25} | Hiding {len(dept_leaves):>4} Scraper Targets")

    print("----------------------------------------------------------------")
    print(f"🏆 GLOBAL TARGET MATRIX COMPLETE: Ready to extract {len(all_global_leaf_nodes)} total categories.")
    print("================================================================\n")
    
    return all_global_leaf_nodes


def main():
    setup_database("prom_master.db")

    try:
        with open("prom_category_tree.json", "r", encoding="utf-8") as f:
            category_tree = json.load(f)
    except FileNotFoundError:
        logging.error("❌ Critical Pipeline Halt: 'prom_category_tree.json' missing from workspace. Run your mapper script first.")
        return

    target_subcategories = analyze_global_catalog(category_tree)
    if not target_subcategories:
        logging.warning("⚠️ Scraper target run matrix evaluated to empty. Aborting run pipeline.")
        return
        
    # --- NEW RESUMPTION LOGIC ---
    # Check the database to see which categories we already successfully scraped
    conn = sqlite3.connect("prom_master.db")
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT category_alias FROM products_surface")
    completed_aliases = {row[0] for row in cursor.fetchall()}
    conn.close()
    
    logging.info(f"📂 RESUMPTION CHECK: Found {len(completed_aliases)} categories already partially or fully in the database.")
    logging.info("⏳ Countdown initiated. Review global department matrix above...")
    time.sleep(5)
    
    for target_alias in target_subcategories:
        # SKIP this category if it's already in the database
        if target_alias in completed_aliases:
            logging.info(f"⏭️ SKIPPING: {target_alias} (Already exists in database)")
            continue

        base_url = f"https://prom.ua/ua/{target_alias}?sort=-score"
        master_database = []
        
        logging.info(f"\n🚀 INITIATING MASS HARVEST: {target_alias}")
        
        page_1_data, total_items = fetch_and_extract_page(f"{base_url}&page=1", 1, target_alias)
        
        if not page_1_data:
            logging.error(f"❌ Target context aborted: Handshake drop on {target_alias}. Skipping node.")
            continue
            
        master_database.extend(page_1_data)
        
        items_per_page = 30
        total_pages = math.ceil(total_items / items_per_page)
        pages_to_scrape = total_pages 
        
        logging.info(f"📊 Bounds Evaluated: {total_items} entries across {pages_to_scrape} pages.")
        
        if pages_to_scrape <= 1:
            logging.info(f"✅ Context extraction single-page allocation satisfied.")
        else:
            processing_track = {page: False for page in range(2, pages_to_scrape + 1)}
            max_sweep_passes = 4
            current_pass = 1
            
            while current_pass <= max_sweep_passes and not all(processing_track.values()):
                pending_pages = [page for page, completed in processing_track.items() if not completed]
                
                # REDUCED WORKERS TO 16 TO PREVENT CLOUDFLARE TIMEOUT BANS
                with concurrent.futures.ThreadPoolExecutor(max_workers=48) as executor:
                    future_to_page = {
                        executor.submit(fetch_and_extract_page, f"{base_url}&page={p}", p, target_alias): p 
                        for p in pending_pages
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_page):
                        page_num = future_to_page[future]
                        try:
                            page_data, _ = future.result()
                            if page_data:
                                master_database.extend(page_data)
                                processing_track[page_num] = True  
                            else:
                                logging.error(f"❌ Segment {page_num} returned empty metrics.")
                        except Exception as exc:
                            logging.error(f"❌ Execution failure tracking index {page_num}: {exc}")
                            
                current_pass += 1
                if not all(processing_track.values()):
                    time.sleep(3)
        
        # SECURE LOCAL PERSISTENCE TO SQLITE DATABASE
        if master_database:
            upsert_products("prom_master.db", master_database)
            
        time.sleep(5)

if __name__ == "__main__":
    main()