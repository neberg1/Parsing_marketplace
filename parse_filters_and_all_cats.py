import time
import math
import logging
import sqlite3
import concurrent.futures
import threading
import json
import os
from urllib.parse import urlencode
import requests

# ==========================================
# 1. CONFIGURATION & TELEGRAM
# ==========================================
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = ""

DB_NAME = "prom_master_run.db"
TEMP_FILE = "prom_raw_data_fast.jsonl"
LEAF_TARGETS_FILE = "prom_leaf_targets.json"
MAX_WORKERS = 48

FILE_LOCK = threading.Lock() 
COMPETITOR_LOCK = threading.Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', 
                    handlers=[logging.FileHandler("scraper_24_7.log", encoding='utf-8'), logging.StreamHandler()])

def send_telegram_alert(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": f"🤖 Prom Scraper:\n{message}"}, timeout=5)
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")

# ==========================================
# 2. DATABASE MANAGEMENT
# ==========================================
def setup_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS category_progress (alias TEXT PRIMARY KEY, status TEXT)')
    cursor.execute('''CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY, category TEXT, name TEXT, url TEXT, brand TEXT,
            price TEXT, is_ad BOOLEAN, company_name TEXT, company_url TEXT,
            algo_score TEXT, filters_json TEXT, competitors_json TEXT, sku TEXT,
            image_large TEXT, measure_unit TEXT, selling_type TEXT, availability_status TEXT,
            company_data_json TEXT, wholesale_json TEXT, product_model_json TEXT, advert_weight TEXT
        )''')
    conn.commit()
    conn.close()

# ==========================================
# 3. PHASE 1: HIGH-SPEED EXTRACTION
# ==========================================
def save_to_temp_file(products_list):
    if not products_list: return
    with FILE_LOCK:
        with open(TEMP_FILE, 'a', encoding='utf-8') as f:
            for p in products_list:
                f.write(json.dumps(p, ensure_ascii=False) + '\n')

def get_headers(url):
    return {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36", "Referer": url, "x-language": "uk"}

def fetch_filters_via_graphql(category_alias):
    base_category_url = f"https://prom.ua/ua/{category_alias}"
    headers = get_headers(base_category_url)
    headers.update({'content-type': 'application/json', 'x-apollo-operation-name': 'CategoryFiltersQuery', 'x-apollo-operation-type': 'query'})

    payload = {
        "operationName": "CategoryFiltersQuery", "variables": {"regionId": None, "params": {"binary_filters": []}, "alias": category_alias},
        "query": """query CategoryFiltersQuery($alias: String!, $manufacturer_id: Int, $params: Any, $company_id: Int, $sort: String, $regionId: Int = null, $subdomain: String = null) { listing: categoryListing(alias: $alias, manufacturer_id: $manufacturer_id, params: $params, company_id: $company_id, sort: $sort, region: {id: $regionId, subdomain: $subdomain}) { filters { ...FiltersFragment __typename } __typename } } fragment FiltersFragment on ListingFilters { total attributeFilters { name title type min max measureUnit values { value title count __typename } __typename } __typename }"""
    }

    try:
        response = requests.post('https://prom.ua/graphql', headers=headers, json=payload, timeout=15)
        filters_data = response.json()['data']['listing']['filters']
    except Exception: return {}

    structured_filters = {}
    for attr in filters_data.get('attributeFilters', []):
        group_title, group_name, filter_type = attr.get('title'), attr.get('name'), attr.get('type')
        if not group_name or not group_title: continue
        structured_filters[group_title] = []
        
        if filter_type == 'real':
            try: min_v, max_v = int(attr.get('min', 0)), int(attr.get('max', 0))
            except: continue
            if max_v <= min_v: continue
            step = math.ceil((max_v - min_v) / 5)
            for i in range(5):
                b_min = min_v + (i * step)
                b_max = b_min + step - 1 if i < 4 else max_v
                opt_title = f"{b_min} - {b_max} {attr.get('measureUnit', '')}".strip()
                structured_filters[group_title].append({"option_name": opt_title, "url": f"{base_category_url}?{group_name}__gte={b_min}&{group_name}__lte={b_max}"})
        else:
            for val in attr.get('values', []):
                if val.get('value') and val.get('title'):
                    structured_filters[group_title].append({"option_name": val.get('title'), "url": f"{base_category_url}?{urlencode({group_name: val.get('value')})}"})

    return structured_filters

def extract_page_data(task_url, category, filter_group, filter_option):
    headers = get_headers(task_url)
    for attempt in range(1, 4):
        try:
            response = requests.get(task_url, headers=headers, timeout=12)
            
            # Pagination redirect check
            if ';' in task_url and ';' not in response.url:
                return [], 0, "OK"
                
            if response.status_code == 200:
                html_text = response.text
                
                # Blazing fast string search (No BeautifulSoup)
                marker = "window.ApolloCacheState = "
                start_idx = html_text.find(marker)
                
                if start_idx != -1:
                    start_idx += len(marker)
                    raw_json = html_text[start_idx:].strip()
                    
                    cache_data, _ = json.JSONDecoder().raw_decode(raw_json)
                    fast_cache = cache_data.get('_FAST_CACHE', {})
                    
                    search_key = next((k for k in fast_cache.keys() if 'ListingQuery' in k or 'Catalog' in k), None)
                    if not search_key: return [], 0, "OK"
                        
                    total_items = fast_cache[search_key]['result']['listing']['page']['total']
                    
                    clean_products = []
                    for item in fast_cache[search_key]['result']['listing']['page']['products']:
                        product_info = item.get('product', {})
                        if product_info:
                            p_id = str(product_info.get('id'))
                            
                            selling_type_data = product_info.get('sellingType', {})
                            stype = "Опт і Роздріб" if 'universal' in selling_type_data else "Роздріб" if 'retail' in selling_type_data else "Unknown"

                            company = product_info.get('company', {})
                            c_id, c_slug = company.get('id', ''), company.get('slug', '')
                            
                            clean_model = {
                                "model_id": item.get('productModel', {}).get('model_id'), 
                                "min_price": item.get('productModel', {}).get('min_price'), 
                                "max_price": item.get('productModel', {}).get('max_price'), 
                                "product_count": item.get('productModel', {}).get('product_count'), 
                                "products": item.get('productModel', {}).get('model_product_ids', [])
                            } if item.get('productModel') else {}

                            clean_products.append({
                                "id": p_id, "category": category, "name": product_info.get('name'), 
                                "url": f"https://prom.ua/ua/p{p_id}-{product_info.get('urlText', '')}.html",
                                "brand": (product_info.get('manufacturerInfo') or {}).get('name', 'Unknown'),
                                "price": product_info.get('price'), "is_ad": bool(item.get('advert')),
                                "company_name": company.get('name', 'Unknown'),
                                "company_url": f"https://prom.ua/ua/c{c_id}-{c_slug}.html" if c_id and c_slug else "",
                                "algo_score": str(item.get('score', '0')), "filter_group": filter_group, "filter_option": filter_option,
                                "sku": product_info.get('sku', ''), "image_large": product_info.get('imageAlt', '') or product_info.get('image400x400', ''),
                                "measure_unit": product_info.get('measureUnit', 'шт.'), "selling_type": stype,
                                "availability_status": product_info.get('catalogPresence', {}).get('title', 'Unknown'),
                                "company_data_json": json.dumps({"regionName": company.get('regionName', ''), "isService": company.get('isService', False), "inTopSegment": company.get('inTopSegment', False), "opinionPositivePercent": company.get('opinionStats', {}).get('opinionPositivePercent', 0), "opinionTotal": company.get('opinionStats', {}).get('opinionTotal', 0)}, ensure_ascii=False),
                                "wholesale_json": json.dumps([{"min_qty": int(w.get('minimumOrderQuantity', 0)), "price": float(w.get('price', 0))} for w in (product_info.get('wholesalePrices') or []) if w.get('price')], ensure_ascii=False),
                                "product_model_json": json.dumps(clean_model, ensure_ascii=False),
                                "advert_weight": str((item.get('advert') or {}).get('advert_weight_adv', '0'))
                            })
                    return clean_products, total_items, "OK"
            time.sleep(2) 
        except Exception as e:
            time.sleep(2)
    return [], 0, "ERROR"

# ==========================================
# 4. PHASE 1.5: SQLITE INGESTION
# ==========================================
def ingest_jsonl_to_sqlite():
    if not os.path.exists(TEMP_FILE) or os.path.getsize(TEMP_FILE) == 0: return

    merged_data = {}
    with open(TEMP_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                p = json.loads(line)
                pid = p['id']
                if pid not in merged_data:
                    merged_data[pid] = {k: p.get(k, '') for k in ['category', 'name', 'url', 'brand', 'price', 'is_ad', 'company_name', 'company_url', 'algo_score', 'sku', 'image_large', 'measure_unit', 'selling_type', 'availability_status', 'company_data_json', 'wholesale_json', 'product_model_json', 'advert_weight']}
                    merged_data[pid]['filters'] = {}
                
                fg, fo = p['filter_group'], p['filter_option']
                if fg not in merged_data[pid]["filters"]: merged_data[pid]["filters"][fg] = []
                if fo not in merged_data[pid]["filters"][fg]: merged_data[pid]["filters"][fg].append(fo)
            except: pass

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    ids = list(merged_data.keys())
    existing_db_data = {}
    
    for i in range(0, len(ids), 900):
        chunk = ids[i:i + 900]
        cursor.execute(f"SELECT id, filters_json FROM products WHERE id IN ({','.join(['?'] * len(chunk))})", chunk)
        for row in cursor.fetchall():
            try: existing_db_data[row[0]] = json.loads(row[1]) if row[1] else {}
            except: existing_db_data[row[0]] = {}

    records_to_upsert = []
    for pid, mem in merged_data.items():
        db_filters = existing_db_data.get(pid, {})
        for grp, opts in mem["filters"].items():
            if grp not in db_filters: db_filters[grp] = []
            for opt in opts:
                if opt not in db_filters[grp]: db_filters[grp].append(opt)
                    
        records_to_upsert.append((
            pid, mem['category'], mem['name'], mem['url'], mem['brand'], str(mem['price']), mem['is_ad'], 
            mem['company_name'], mem['company_url'], mem['algo_score'], json.dumps(db_filters, ensure_ascii=False),
            mem['sku'], mem['image_large'], mem['measure_unit'], mem['selling_type'], mem['availability_status'], 
            mem['company_data_json'], mem['wholesale_json'], mem['product_model_json'], mem['advert_weight']
        ))
    
    for i in range(0, len(records_to_upsert), 900):
        cursor.executemany('''INSERT INTO products 
            (id, category, name, url, brand, price, is_ad, company_name, company_url, algo_score, filters_json, sku, image_large, measure_unit, selling_type, availability_status, company_data_json, wholesale_json, product_model_json, advert_weight) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET 
            filters_json = excluded.filters_json, name = excluded.name, url = excluded.url, brand = excluded.brand, price = excluded.price, is_ad = excluded.is_ad, company_name = excluded.company_name, company_url = excluded.company_url, algo_score = excluded.algo_score, sku = excluded.sku, image_large = excluded.image_large, measure_unit = excluded.measure_unit, selling_type = excluded.selling_type, availability_status = excluded.availability_status, company_data_json = excluded.company_data_json, wholesale_json = excluded.wholesale_json, product_model_json = excluded.product_model_json, advert_weight = excluded.advert_weight''', records_to_upsert[i:i + 900])
        
    conn.commit()
    conn.close()
    os.remove(TEMP_FILE)

# ==========================================
# 5. PHASE 2: COMPETITORS
# ==========================================
def extract_competitors_for_product(product_id, target_url):
    payload = {
        "operationName": "GlobalRecommendedBlockQuery",
        "variables": {"product_id": int(product_id), "visited_ids": [int(product_id)], "favorite_ids": [], "limit": 60, "offset": 0},
        "query": "query GlobalRecommendedBlockQuery($product_id: Long!, $visited_ids: [Long!]!, $favorite_ids: [Long!]!, $limit: Int, $offset: Int) { recommendedNew(product_id: $product_id visited_ids: $visited_ids favorite_ids: $favorite_ids limit: $limit offset: $offset) { product { id urlText __typename } __typename } }"
    }
    try:
        headers = {
            "accept": "*/*", 
            "content-type": "application/json", 
            "origin": "https://prom.ua", 
            "referer": target_url, 
            "x-apollo-operation-name": "GlobalRecommendedBlockQuery", 
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
        }
        response = requests.post("https://prom.ua/graphql", json=payload, headers=headers, timeout=15)
        if response.status_code == 200:
            return [str(item['product']['id']) for item in response.json().get('data', {}).get('recommendedNew', []) if item.get('product', {}).get('id')], 200
        return [], response.status_code
    except: return [], 500

def run_competitor_sweep():
    conn = sqlite3.connect(DB_NAME)
    pending = conn.cursor().execute("SELECT id, url FROM products WHERE competitors_json IS NULL").fetchall()
    conn.close()
    
    if not pending: return

    batch_results = []

    def fetch_in_memory(pid, url):
        comp_ids, status = extract_competitors_for_product(pid, url)
        if status == 429: return pid, None, "BLOCKED"
        return pid, json.dumps(comp_ids, ensure_ascii=False), "SUCCESS"

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_in_memory, p[0], p[1]): p for p in pending}
        for future in concurrent.futures.as_completed(futures):
            pid, comp_data, status = future.result()
            if status == "SUCCESS":
                batch_results.append((comp_data, pid))
            elif status == "BLOCKED":
                time.sleep(2)

    if batch_results:
        conn = sqlite3.connect(DB_NAME)
        conn.executemany("UPDATE products SET competitors_json = ? WHERE id = ?", batch_results)
        conn.commit()
        conn.close()

# ==========================================
# 6. THE 24/7 ORCHESTRATOR
# ==========================================
def process_category(target_category):
    with open(TEMP_FILE, 'w', encoding='utf-8') as f: pass 
    
    filters_dict = fetch_filters_via_graphql(target_category)
    if not filters_dict:
        raise Exception(f"Failed to fetch initial filters for {target_category}. Blocking completion to prevent false DONE state.")

    filter_tasks = [{"group": g, "option": o['option_name'], "url": o['url']} for g, opts in filters_dict.items() for o in opts]
    
    master_queue = []
    initial_errors = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(extract_page_data, f"{t['url']}&page=1", target_category, t['group'], t['option']): t for t in filter_tasks}
        for future in concurrent.futures.as_completed(futures):
            t = futures[future]
            items, total, status = future.result()
            
            if status == "ERROR":
                initial_errors += 1
                logging.warning(f"⚠️ Failed to load page: {t['url']}")
                
            if items:
                save_to_temp_file(items)
                for p in range(2, math.ceil(total / 30) + 1):
                    master_queue.append({"url": f"{t['url']}&page={p}", "group": t['group'], "option": t['option']})

    # The strict empty-data bypass: If we completely failed to get pages, crash immediately.
    if not master_queue and initial_errors > 0:
        raise Exception("No pagination data acquired but errors were detected. VPN might be blocked. Halting!")

    if master_queue:
        consecutive_errors = 0
        pages_done = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(extract_page_data, item['url'], target_category, item['group'], item['option']): item for item in master_queue}
            for future in concurrent.futures.as_completed(futures):
                items, _, status = future.result()
                if status == "ERROR":
                    consecutive_errors += 1
                    if consecutive_errors >= 15:
                        raise Exception("Circuit Breaker Tripped! 15 consecutive errors. Is the VPN blocked?")
                else:
                    consecutive_errors = 0
                    if items: save_to_temp_file(items)
                
                pages_done += 1
                if pages_done % 100 == 0:
                    logging.info(f"    🔥 [Heartbeat] {pages_done}/{len(master_queue)} pages parsed in {target_category}...")

    ingest_jsonl_to_sqlite()
    run_competitor_sweep()
    
    conn = sqlite3.connect(DB_NAME)
    conn.execute("INSERT OR REPLACE INTO category_progress VALUES (?, 'DONE')", (target_category,))
    conn.commit()
    conn.close()
    logging.info(f"✅ CATEGORY FULLY COMPLETE: {target_category}\n")

def main():
    setup_database()
    
    if not os.path.exists(LEAF_TARGETS_FILE):
        logging.error(f"❌ {LEAF_TARGETS_FILE} not found! Please run the master tree builder first.")
        return
        
    with open(LEAF_TARGETS_FILE, 'r') as f:
        all_targets = json.load(f)
        
    conn = sqlite3.connect(DB_NAME)
    done_cats = {r[0] for r in conn.execute("SELECT alias FROM category_progress WHERE status='DONE'").fetchall()}
    conn.close()
    
    pending_targets = [c for c in all_targets if c not in done_cats]
    logging.info(f"🚀 Starting 24/7 Run. Categories to process: {len(pending_targets)} / {len(all_targets)}")
    send_telegram_alert(f"🚀 Prom Scraper Started!\nCategories remaining: {len(pending_targets)}")

    for cat_alias in pending_targets:
        try:
            logging.info(f"=======================================")
            logging.info(f"🎯 PROCESSING CATEGORY: {cat_alias}")
            logging.info(f"=======================================")
            process_category(cat_alias)
        except Exception as e:
            msg = f"CRITICAL ERROR on category '{cat_alias}'. Script paused!\nReason: {str(e)}"
            logging.error(msg)
            send_telegram_alert(msg)
            ingest_jsonl_to_sqlite()
            break 
            
    if len(pending_targets) == 0:
        send_telegram_alert("🎉 ALL CATEGORIES SUCCESSFULLY SCRAPED! JOB COMPLETE.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("\n🛑 Interrupted manually! Saving temp files to DB...")
        ingest_jsonl_to_sqlite()