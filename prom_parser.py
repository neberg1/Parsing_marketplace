import requests
import json
import sqlite3
import time
import math
import logging
import concurrent.futures
import queue
import threading
from bs4 import BeautifulSoup
from datetime import datetime

# ==========================================
# 1. SETUP & LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("market_tracker.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

DB_NAME = "prom_market_dynamics.db"

# ==========================================
# 2. DATABASE INITIALIZATION
# ==========================================
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Tracks the actual scrape jobs
    cursor.execute('''CREATE TABLE IF NOT EXISTS scrape_sessions (
            session_id INTEGER PRIMARY KEY,
            run_type TEXT,
            target_category TEXT,
            status TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')
        
    # The Time-Series Database (Appends only)
    cursor.execute('''CREATE TABLE IF NOT EXISTS ranking_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            product_id TEXT,
            category_alias TEXT,
            name TEXT,
            url TEXT,
            category_position INTEGER,
            price REAL,
            stock_status TEXT
        )''')
        
    # The Delta Table (Stores the changes detected between runs)
    cursor.execute('''CREATE TABLE IF NOT EXISTS market_alerts (
            alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            product_id TEXT,
            category_alias TEXT,
            alert_type TEXT,
            message TEXT
        )''')
        
    # Indices for blazing fast SQL comparisons
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_session_cat ON ranking_history(session_id, category_alias);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prod_session ON ranking_history(product_id, session_id);")
    
    conn.commit()
    conn.close()

# ==========================================
# 3. THE BACKGROUND WRITER (NO-BLOCKING DB)
# ==========================================
def db_writer_worker(data_queue):
    """
    Runs in the background. Constantly pulls data from the scrapers' memory queue 
    and bulk-inserts it into SQLite. This ensures the 48 scrapers NEVER pause.
    """
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cursor = conn.cursor()
    batch = []
    
    while True:
        item = data_queue.get()
        if item is None: # Sentinel value meaning "Scraping is done, shut down"
            if batch:
                cursor.executemany('''INSERT INTO ranking_history 
                    (session_id, product_id, category_alias, name, url, category_position, price, stock_status) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', batch)
                conn.commit()
            break
            
        batch.append(item)
        
        # Dump to database every 500 items
        if len(batch) >= 500:
            cursor.executemany('''INSERT INTO ranking_history 
                    (session_id, product_id, category_alias, name, url, category_position, price, stock_status) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', batch)
            conn.commit()
            batch = []
            
        data_queue.task_done()
        
    conn.close()

# ==========================================
# 4. THE HIGH-SPEED SCRAPER LOGIC
# ==========================================
def fetch_and_extract_page(session_id, target_url, page_num, category_alias, data_queue, page_1_anchor_id=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8"
    }
    
    for attempt in range(1, 4):
        try:
            # allow_redirects=True is default, but we will catch it in response.url
            response = requests.get(target_url, headers=headers, timeout=12)
            
            if response.status_code == 200:
                
                # 🟢 CHECK 1: Did Prom.ua redirect us?
                if page_num > 1 and f";{page_num}" not in response.url:
                    
                    return 0, None
                
                soup = BeautifulSoup(response.text, 'html.parser')
                for script in soup.find_all('script'):
                    script_text = str(script.string)
                    if 'window.ApolloCacheState' in script_text:
                        start_idx = script_text.find("window.ApolloCacheState = ") + 26
                        raw_json = script_text[start_idx:].strip()
                        
                        cache_data, _ = json.JSONDecoder().raw_decode(raw_json)
                        fast_cache = cache_data.get('_FAST_CACHE', {})
                        search_key = next((k for k in fast_cache.keys() if 'ListingQuery' in k or 'Catalog' in k), None)
                        
                        if search_key:
                            total_items = fast_cache[search_key]['result']['listing']['page']['total']
                            products = fast_cache[search_key]['result']['listing']['page']['products']
                            
                            if not products:
                                return 0, None
                                
                            # 🟢 Get the very first item's ID on this page
                            first_item_id = str(products[0].get('product', {}).get('id'))
                            
                            # 🟢 CHECK 2: Is this just Page 1 repeating?
                            if page_num > 1 and page_1_anchor_id and first_item_id == page_1_anchor_id:
                               
                                return 0, None
                            
                            items_per_page = 30 
                            
                            for local_index, item in enumerate(products):
                                product_info = item.get('product', {})
                                if product_info:
                                    absolute_position = (page_num - 1) * items_per_page + (local_index + 1)
                                    prod_id = str(product_info.get('id'))
                                    
                                    raw_discount = product_info.get('discountedPrice')
                                    raw_price = product_info.get('price')
                                    if raw_discount: price = float(raw_discount)
                                    elif raw_price: price = float(raw_price)
                                    else: price = 0.0
                                    
                                    stock = product_info.get('catalogPresence', {}).get('value', 'Unknown')
                                    name = product_info.get('name', '')
                                    url = f"https://prom.ua/ua/p{prod_id}-{product_info.get('urlText', '')}.html"
                                    
                                    data_queue.put((
                                        session_id, prod_id, category_alias, name, url, absolute_position, price, stock
                                    ))
                                    
                            items_found = len(products)
                           
                            return items_found, first_item_id, total_items
                            
            time.sleep(2)
        except Exception as e:
            time.sleep(2)
            
    logging.error(f"    ❌ Page {page_num}: FAILED after 3 attempts.")
    return 0, None, 0

# ==========================================
# 5. THE DELTA ANALYZER (CHANGES & ALERTS)
# ==========================================
def analyze_deltas(current_session_id, category_alias):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Find the PREVIOUS successful run for this exact category
    cursor.execute('''
        SELECT session_id FROM scrape_sessions 
        WHERE status = 'completed' AND target_category = ? AND session_id < ?
        ORDER BY session_id DESC LIMIT 1
    ''', (category_alias, current_session_id))
    
    prev_run = cursor.fetchone()
    
    if not prev_run:
        logging.info(f"📊 Delta Analysis: No previous run found for {category_alias}. Establishing baseline.")
        conn.close()
        return
        
    prev_session_id = prev_run[0]
    logging.info(f"🔍 Analyzing Changes between Run {prev_session_id} and Run {current_session_id} for {category_alias}...")
    
    alerts_to_insert = []
    
    # A. Detect Price Changes and Rank Shifts (Items that exist in BOTH runs)
    cursor.execute('''
        SELECT 
            c.product_id, c.name, 
            p.price AS old_price, c.price AS new_price,
            p.category_position AS old_rank, c.category_position AS new_rank
        FROM ranking_history c
        JOIN ranking_history p ON c.product_id = p.product_id
        WHERE c.session_id = ? AND p.session_id = ? AND c.category_alias = ?
    ''', (current_session_id, prev_session_id, category_alias))
    
    for row in cursor.fetchall():
        prod_id, name, old_price, new_price, old_rank, new_rank = row
        
        if old_price != new_price:
            msg = f"PRICE CHANGE: {old_price} -> {new_price} | {name}"
            alerts_to_insert.append((current_session_id, prod_id, category_alias, "PRICE_CHANGE", msg))
            
        rank_diff = old_rank - new_rank
        if abs(rank_diff) >= 5: # Only alert if it jumped more than 5 spots
            direction = "UP" if rank_diff > 0 else "DOWN"
            msg = f"RANK MOVED {direction}: #{old_rank} to #{new_rank} | {name}"
            alerts_to_insert.append((current_session_id, prod_id, category_alias, "RANK_SHIFT", msg))

    # B. Detect Missing Items (Dropped out of the category)
    cursor.execute('''
        SELECT product_id, name, category_position FROM ranking_history 
        WHERE session_id = ? AND category_alias = ?
        AND product_id NOT IN (SELECT product_id FROM ranking_history WHERE session_id = ? AND category_alias = ?)
    ''', (prev_session_id, category_alias, current_session_id, category_alias))
    
    for row in cursor.fetchall():
        prod_id, name, old_rank = row
        msg = f"DROPPED OUT: Was #{old_rank}, now completely missing | {name}"
        alerts_to_insert.append((current_session_id, prod_id, category_alias, "DROPPED_OUT", msg))

    # C. Detect Brand New Arrivals
    cursor.execute('''
        SELECT product_id, name, category_position FROM ranking_history 
        WHERE session_id = ? AND category_alias = ?
        AND product_id NOT IN (SELECT product_id FROM ranking_history WHERE session_id = ? AND category_alias = ?)
    ''', (current_session_id, category_alias, prev_session_id, category_alias))
    
    for row in cursor.fetchall():
        prod_id, name, new_rank = row
        msg = f"NEW ARRIVAL: Appeared at #{new_rank} | {name}"
        alerts_to_insert.append((current_session_id, prod_id, category_alias, "NEW_ARRIVAL", msg))

    # Save Alerts
    if alerts_to_insert:
        cursor.executemany('''INSERT INTO market_alerts 
            (session_id, product_id, category_alias, alert_type, message) 
            VALUES (?, ?, ?, ?, ?)''', alerts_to_insert)
        conn.commit()
        logging.info(f"🚨 Delta Analysis Complete: Saved {len(alerts_to_insert)} market alerts/changes to database.")
    else:
        logging.info("⚖️ Delta Analysis Complete: No significant market changes detected.")
        
    conn.close()

# ==========================================
# 6. ORCHESTRATOR
# ==========================================
def run_category_sweep(category_alias, run_type="targeted"):
    session_id = int(time.time())
    logging.info(f"\n==================================================")
    logging.info(f"🚀 STARTING TRACKER SESSION: {session_id} | Target: {category_alias}")
    logging.info(f"==================================================")
    
    # 1. Log Session Start
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO scrape_sessions (session_id, run_type, target_category, status) VALUES (?, ?, ?, ?)", 
                   (session_id, run_type, category_alias, "running"))
    conn.commit()
    conn.close()

    # 2. Setup the Queue & Background Writer
    data_queue = queue.Queue()
    writer_thread = threading.Thread(target=db_writer_worker, args=(data_queue,))
    writer_thread.start()

    

    # 3. Ping Page 1 to get the Anchor ID and the Total Items reported by Prom
    page_1_url = f"https://prom.ua/ua/{category_alias}?sort=-score"
    items_on_page_1, page_1_anchor, total_items = fetch_and_extract_page(session_id, page_1_url, 1, category_alias, data_queue)
    
    if items_on_page_1 > 0:
        # 🟢 YOUR DYNAMIC LOGIC: Calculate base pages, then add the +48 buffer!
        base_pages = math.ceil(total_items / 30)
        buffer_pages = 48 
        max_pages_to_check = base_pages + buffer_pages 
        
        logging.info(f"📊 Target alive. Prom reports {total_items} items ({base_pages} pages). Adding +{buffer_pages} buffer. Scanning up to {max_pages_to_check} pages.")
        
        # 4. Launch 48 Workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=48) as executor:
            futures = [
                executor.submit(
                    fetch_and_extract_page, 
                    session_id, 
                    f"https://prom.ua/ua/{category_alias};{p}?sort=-score", 
                    p, 
                    category_alias, 
                    data_queue,
                    page_1_anchor # Passes the anchor check!
                ) 
                for p in range(2, max_pages_to_check + 1)
            ]
            concurrent.futures.wait(futures)

    # 5. Shut down queue safely
    data_queue.put(None) 
    writer_thread.join()

    # 6. Mark Session Complete & Trigger Analyzer
    conn = sqlite3.connect(DB_NAME)
    conn.execute("UPDATE scrape_sessions SET status = 'completed' WHERE session_id = ?", (session_id,))
    conn.commit()
    conn.close()
    
    analyze_deltas(session_id, category_alias)
    logging.info(f"🏁 SESSION {session_id} FULLY COMPLETE.\n")


if __name__ == "__main__":
    init_db()
    
    # --- TOGGLE YOUR TARGETS HERE ---
    
    # MODE A: Targeted List (Just testing specific categories)
    target_categories = ["Kaminy"] 
    
    # MODE B: Full Global Sweep (Uncomment to use)
    # try:
    #     with open("prom_category_tree.json", "r", encoding="utf-8") as f:
    #         category_tree = json.load(f)
    #         target_categories = [alias for alias in category_tree.keys()]
    # except Exception:
    #     pass

    for category in target_categories:
        run_category_sweep(category_alias=category, run_type="targeted_sweep")