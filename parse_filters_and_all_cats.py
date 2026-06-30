import time
import math
import logging
import sqlite3
import concurrent.futures
import threading
import json
import os
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import requests as standard_requests
from curl_cffi import requests as cffi_requests

# ==========================================
# 1. LOGGING & GLOBAL VARIABLES
# ==========================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(message)s', 
    handlers=[
        logging.FileHandler("scraper_full_run.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

DB_NAME = "prom_master_run.db"
TEMP_FILE = "prom_raw_data_fast.jsonl"
FILE_LOCK = threading.Lock() # Protects the JSONL file from being corrupted by 48 workers writing at once
COMPETITOR_LOCK = threading.Lock()
# Clear the temp file on startup
with open(TEMP_FILE, 'w', encoding='utf-8') as f:
    pass

# ==========================================
# 2. DATABASE MANAGEMENT
# ==========================================
def setup_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # We added competitors_json to the end of this table
    cursor.execute('''CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY, 
            category TEXT, 
            name TEXT, 
            url TEXT, 
            brand TEXT,
            price TEXT,
            is_ad BOOLEAN,
            company_name TEXT,
            algo_score TEXT,
            filters_json TEXT,
            competitors_json TEXT
        )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS filter_progress (
            filter_url TEXT PRIMARY KEY
        )''')
    
    conn.commit()
    conn.close()

def get_completed_tasks(table_name, column_name):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(f"SELECT {column_name} FROM {table_name}")
    completed = {row[0] for row in cursor.fetchall()}
    conn.close()
    return completed

def mark_task_completed(table_name, column_name, value):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(f"INSERT OR IGNORE INTO {table_name} ({column_name}) VALUES (?)", (value,))
    conn.commit()
    conn.close()

# ==========================================
# 3. PHASE 1: HIGH-SPEED EXTRACTION
# ==========================================
def save_to_temp_file(products_list):
    """Instantly appends scraped data to a JSON Lines file. Zero waiting."""
    if not products_list: return
    with FILE_LOCK:
        with open(TEMP_FILE, 'a', encoding='utf-8') as f:
            for p in products_list:
                f.write(json.dumps(p, ensure_ascii=False) + '\n')

def get_headers(url):
    """Strict headers to force Ukrainian locale and look like a real browser."""
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8", # FORCES UKRAINIAN
        "Referer": url,
        "x-language": "uk"
    }

def fetch_filters_via_graphql(category_alias):
    url = 'https://prom.ua/graphql'
    base_category_url = f"https://prom.ua/ua/{category_alias}"
    headers = get_headers(base_category_url)
    
    headers.update({
        'content-type': 'application/json',
        'x-apollo-operation-name': 'CategoryFiltersQuery', 
        'x-apollo-operation-type': 'query'
    })

    payload = {
        "operationName": "CategoryFiltersQuery",
        "variables": {"regionId": None, "params": {"binary_filters": []}, "alias": category_alias},
        "query": """query CategoryFiltersQuery($alias: String!, $manufacturer_id: Int, $params: Any, $company_id: Int, $sort: String, $regionId: Int = null, $subdomain: String = null) { listing: categoryListing(alias: $alias, manufacturer_id: $manufacturer_id, params: $params, company_id: $company_id, sort: $sort, region: {id: $regionId, subdomain: $subdomain}) { filters { ...FiltersFragment __typename } __typename } } fragment FiltersFragment on ListingFilters { total attributeFilters { name title type min max measureUnit values { value title __typename } __typename } __typename }"""
    }

    try:
        response = standard_requests.post(url, headers=headers, json=payload, timeout=15)
        filters_data = response.json()['data']['listing']['filters']
    except Exception as e:
        logging.error(f"❌ API Request Failed: {e}")
        return {}

    structured_filters = {}
    for attr in filters_data.get('attributeFilters', []):
        group_title, group_name = attr.get('title'), attr.get('name')
        if not group_name or not group_title or attr.get('type') == 'real': continue
        
        structured_filters[group_title] = []
        for val in attr.get('values', []):
            if val.get('value') and val.get('title'):
                structured_filters[group_title].append({
                    "option_name": val.get('title'),
                    "url": f"{base_category_url}?{urlencode({group_name: val.get('value')})}"
                })
    return structured_filters

def extract_page_data(task_url, category, filter_group, filter_option):
    headers = get_headers(task_url)
    for attempt in range(1, 4):
        try:
            response = standard_requests.get(task_url, headers=headers, timeout=12)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                for script in soup.find_all('script'):
                    script_text = str(script.string or '')
                    if 'window.ApolloCacheState' in script_text:
                        start_idx = script_text.find("window.ApolloCacheState = ") + 26
                        raw_json = script_text[start_idx:].strip()
                        
                        json_data, _ = json.JSONDecoder().raw_decode(raw_json)
                        fast_cache = json_data.get('_FAST_CACHE', {})
                        
                        search_key = next((k for k in fast_cache.keys() if 'ListingQuery' in k or 'Catalog' in k), None)
                        if not search_key: return [], 0
                            
                        total_items = fast_cache[search_key]['result']['listing']['page']['total']
                        
                        clean_products = []
                        for item in fast_cache[search_key]['result']['listing']['page']['products']:
                            product_info = item.get('product', {})
                            
                            if product_info:
                                # 🟢 YOUR EXTRACTION LOGIC INTEGRATED HERE
                                product_id = str(product_info.get('id'))
                                url_product = f"https://prom.ua/ua/p{product_id}-{product_info.get('urlText', '')}.html"
                                
                                company = product_info.get('company', {})
                                manufacturer = product_info.get('manufacturerInfo') or {}
                                advert_data = item.get('advert') or {}
                                
                                clean_products.append({
                                    "id": product_id,
                                    "category": category,
                                    "name": product_info.get('name'), 
                                    "url": url_product,
                                    "brand": manufacturer.get('name', 'Unknown'),
                                    "price": product_info.get('price'),
                                    "is_ad": bool(advert_data),
                                    "company_name": company.get('name', 'Unknown'),
                                    "algo_score": str(item.get('score', '0')),
                                    "filter_group": filter_group,
                                    "filter_option": filter_option
                                })
                        
                        return clean_products, total_items
            time.sleep(2) 
        except Exception:
            time.sleep(2)
            
    return [], 0

# ==========================================
# 4. PHASE 1.5: SQLITE INGESTION
# ==========================================
def ingest_jsonl_to_sqlite():
    """Reads the massive JSONL file, merges all duplicate items, and bulk saves rich data to SQLite."""
    if not os.path.exists(TEMP_FILE) or os.path.getsize(TEMP_FILE) == 0:
        return

    logging.info("==================================================")
    logging.info("🗄️ PHASE 1.5: INGESTING RICH DATA TO SQLITE")
    logging.info("==================================================")
    
    merged_data = {}
    
    with open(TEMP_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                p = json.loads(line)
                pid = p['id']
                
                # Setup the base data dictionary if we haven't seen this ID yet
                if pid not in merged_data:
                    merged_data[pid] = {
                        "category": p['category'], 
                        "name": p['name'], 
                        "url": p['url'], 
                        "brand": p.get('brand', 'Unknown'),
                        "price": p.get('price', ''),
                        "is_ad": p.get('is_ad', False),
                        "company_name": p.get('company_name', 'Unknown'),
                        "algo_score": p.get('algo_score', '0'),
                        "filters": {}
                    }
                
                # Append the filter data
                f_group, f_option = p['filter_group'], p['filter_option']
                if f_group not in merged_data[pid]["filters"]:
                    merged_data[pid]["filters"][f_group] = []
                if f_option not in merged_data[pid]["filters"][f_group]:
                    merged_data[pid]["filters"][f_group].append(f_option)
            except: pass

    logging.info(f"    📊 Merged into {len(merged_data)} unique products. Saving to database...")

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Process in chunks to avoid SQLite parameter limits
    ids = list(merged_data.keys())
    chunk_size = 900
    existing_db_data = {}
    
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        placeholders = ','.join(['?'] * len(chunk))
        cursor.execute(f"SELECT id, filters_json FROM products WHERE id IN ({placeholders})", chunk)
        for row in cursor.fetchall():
            try: existing_db_data[row[0]] = json.loads(row[1]) if row[1] else {}
            except: existing_db_data[row[0]] = {}

    records_to_upsert = []
    for pid, mem_data in merged_data.items():
        db_filters = existing_db_data.get(pid, {})
        for group, options in mem_data["filters"].items():
            if group not in db_filters: db_filters[group] = []
            for opt in options:
                if opt not in db_filters[group]: db_filters[group].append(opt)
                    
        # 🟢 EXACT COLUMN MATCHING
        records_to_upsert.append((
            pid, mem_data['category'], mem_data['name'], mem_data['url'], 
            mem_data['brand'], str(mem_data['price']), mem_data['is_ad'], 
            mem_data['company_name'], mem_data['algo_score'], 
            json.dumps(db_filters, ensure_ascii=False)
        ))
    
    for i in range(0, len(records_to_upsert), chunk_size):
        chunk = records_to_upsert[i:i + chunk_size]
        # 🟢 UPDATED SQL UPSERT
        cursor.executemany('''INSERT INTO products 
            (id, category, name, url, brand, price, is_ad, company_name, algo_score, filters_json) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET 
            filters_json = excluded.filters_json, 
            name = excluded.name, 
            url = excluded.url,
            brand = excluded.brand,
            price = excluded.price,
            is_ad = excluded.is_ad,
            company_name = excluded.company_name,
            algo_score = excluded.algo_score''', chunk)
        
    conn.commit()
    conn.close()
    
    os.remove(TEMP_FILE)
    logging.info("    ✅ Ingestion complete. Temp file deleted.")
# ==========================================
# 5. PHASE 2: COMPETITOR EXTRACTION
# ==========================================
def extract_competitors_for_product(product_id, target_url):
    api_url = "https://prom.ua/graphql"
    api_headers = {
        "accept": "*/*", "content-type": "application/json", "origin": "https://prom.ua",
        "referer": target_url, "x-apollo-operation-name": "GlobalRecommendedBlockQuery",
        "x-apollo-operation-type": "query", "x-language": "uk", "x-requested-with": "XMLHttpRequest"
    }
    
    EXACT_QUERY = """query GlobalRecommendedBlockQuery($product_id: Long!, $visited_ids: [Long!]!, $favorite_ids: [Long!]!, $limit: Int, $offset: Int) { recommendedNew(product_id: $product_id visited_ids: $visited_ids favorite_ids: $favorite_ids limit: $limit offset: $offset) { product { id urlText __typename } __typename } }"""

    payload = {
        "operationName": "GlobalRecommendedBlockQuery",
        "variables": {"product_id": int(product_id), "visited_ids": [int(product_id)], "favorite_ids": [], "limit": 60, "offset": 0},
        "query": EXACT_QUERY
    }

    try:
        response = cffi_requests.post(api_url, json=payload, headers=api_headers, impersonate="chrome120", timeout=15)
        if response.status_code == 200:
            items = response.json().get('data', {}).get('recommendedNew', [])
            
            # 🟢 Create a simple list of just the URLs
            competitor_links = []
            for item in items:
                p = item.get('product')
                if p:
                    competitor_links.append(f"https://prom.ua/ua/p{p['id']}-{p.get('urlText')}.html")
                    
            return competitor_links, 200
        return [], response.status_code
    except Exception:
        return [], 500
def run_competitor_sweep():
    logging.info("==================================================")
    logging.info("🚀 PHASE 2 INITIATED: INLINE COMPETITOR EXTRACTION")
    logging.info("==================================================")
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 🟢 Checkpoint Logic: Only select products that don't have competitors saved yet
    cursor.execute("SELECT id, url FROM products WHERE competitors_json IS NULL")
    pending_products = cursor.fetchall()
    
    cursor.execute("SELECT COUNT(*) FROM products WHERE competitors_json IS NOT NULL")
    completed_count = cursor.fetchone()[0]
    conn.close()

    logging.info(f"📊 Found {len(pending_products)} products missing competitors. {completed_count} already done.")
    
    if not pending_products:
        return

    session_completed = 0
    start_time = time.time()

    def fetch_and_save(pid, url):
        comp_links, status = extract_competitors_for_product(pid, url)
        
        if status == 429:
            return "BLOCKED"
            
        # Convert the list of links to a JSON string (e.g., '["url1", "url2"]')
        json_links = json.dumps(comp_links, ensure_ascii=False)
            
        with COMPETITOR_LOCK:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            # 🟢 UPDATE the existing row instead of making a new table
            cursor.execute("UPDATE products SET competitors_json = ? WHERE id = ?", (json_links, pid))
            conn.commit()
            conn.close()
            
        return "SUCCESS"

    # Launching 16 workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_to_pid = {executor.submit(fetch_and_save, pid, url): pid for pid, url in pending_products}
        
        for future in concurrent.futures.as_completed(future_to_pid):
            result = future.result()
            
            if result == "BLOCKED":
                logging.error("🚨 Cloudflare 429 Block Hit. Pausing worker briefly.")
                time.sleep(2)
                continue
                
            session_completed += 1
            if session_completed % 100 == 0:
                logging.info(f"    🔥 COMPETITOR GRIND: Processed {session_completed}/{len(pending_products)} products...")

    elapsed = round((time.time() - start_time) / 60, 2)
    logging.info(f"✅ Phase 2 finished in {elapsed} minutes.")
# ==========================================
# 6. ORCHESTRATOR
# ==========================================
def main():
    setup_database()
    target_category = "Kaminy"
    
    logging.info("==================================================")
    logging.info("🚀 PHASE 1 INITIATED: MASSIVE MASTER QUEUE")
    logging.info("==================================================")
    phase_1_start = time.time()
    
    logging.info(f"📡 Fetching filter schema for: {target_category} (Enforcing Ukrainian Locale)...")
    filters_dict = fetch_filters_via_graphql(target_category)
    filter_tasks = [{"group": g, "option": o['option_name'], "url": o['url']} for g, opts in filters_dict.items() for o in opts]
    completed_urls = get_completed_tasks("filter_progress", "filter_url")
    
    master_queue = []
    
    logging.info("⚙️ Step A: Pinging Page 1 of all filters to map pagination boundaries...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_to_filter = {
            executor.submit(extract_page_data, f"{task['url']}&page=1", target_category, task['group'], task['option']): task 
            for task in filter_tasks if task['url'] not in completed_urls
        }
        
        for future in concurrent.futures.as_completed(future_to_filter):
            task = future_to_filter[future]
            items, total = future.result()
            
            if items:
                save_to_temp_file(items)
                total_pages = math.ceil(total / 30)
                
                # Add pages 2 onwards to the Master Queue
                for p in range(2, total_pages + 1):
                    master_queue.append({
                        "url": f"{task['url']}&page={p}",
                        "group": task['group'],
                        "option": task['option']
                    })
            else:
                mark_task_completed("filter_progress", "filter_url", task['url'])

    if master_queue:
        logging.info(f"🎯 Step B: Launching Continuous 48-Worker Grind on {len(master_queue)} total pages...")
        completed_pages_count = 0
        
        # 48 Workers hitting the queue without ever stopping
        with concurrent.futures.ThreadPoolExecutor(max_workers=48) as executor:
            future_to_url = {
                executor.submit(extract_page_data, item['url'], target_category, item['group'], item['option']): item 
                for item in master_queue
            }
            
            for future in concurrent.futures.as_completed(future_to_url):
                item = future_to_url[future]
                p_items, _ = future.result()
                
                if p_items:
                    save_to_temp_file(p_items)
                    
                completed_pages_count += 1
                if completed_pages_count % 50 == 0:
                    logging.info(f"    🔥 ACTIVE GRIND: {completed_pages_count}/{len(master_queue)} pages complete...")
                    
    # Mark all base URLs as completed so we don't repeat them on next run
    for task in filter_tasks:
        mark_task_completed("filter_progress", "filter_url", task['url'])
        
    logging.info(f"\n🏁 PHASE 1 SCRAPING COMPLETE in {round((time.time() - phase_1_start) / 60, 2)} minutes.")
    
    # --- PHASE 1.5 ---
    ingest_jsonl_to_sqlite()
    
    # --- PHASE 2 ---
    run_competitor_sweep()
    logging.info("\n🎉 PIPELINE FULLY COMPLETE.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("\n🛑 Interrupted! Attempting to ingest whatever was saved to the temp file...")
        ingest_jsonl_to_sqlite()