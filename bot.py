import requests
import json
import time
import logging
import sqlite3
from datetime import datetime
import google.generativeai as genai
import os
import schedule
import threading
import asyncio
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from telegram import Update
from telegram.ext import ContextTypes
from requests.exceptions import HTTPError  # NEW: Added for rate limiting

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('/data/baxus_scraper.log'), logging.StreamHandler()]  # CHANGED: Log to /data
)

# NEW: Load environment variables
TOKEN = os.getenv("TOKEN")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# NEW: Validate environment variables
if not TOKEN or not GOOGLE_API_KEY:
    logging.error("Missing TOKEN or GOOGLE_API_KEY environment variables")
    exit(1)

# Scraper
BASE_URL = "https://services.baxus.co/api/search/listings"

def fetch_listings():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124",
        "Accept": "application/json",
    }
    all_listings = []
    raw_listings = []
    seen_ids = set()
    from_idx = 0
    size = 24
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    while True:
        url = f"{BASE_URL}?from={from_idx}&size={size}&listed=true&sort=price%3Adesc"
        logging.info(f"Fetching listings: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=10)
            # NEW: Handle rate limiting (HTTP 429)
            if response.status_code == 429:
                logging.warning("Rate limit hit, sleeping for 60 seconds")
                time.sleep(60)
                response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
        except HTTPError as e:
            logging.error(f"HTTP error fetching {url}: {e}")
            break
        except requests.RequestException as e:
            logging.error(f"Error fetching {url}: {e}")
            break

        try:
            listings = response.json()
            logging.info(f"Found {len(listings)} listings at from={from_idx}")
            if listings:
                logging.debug(f"Sample listing keys: {list(listings[0].keys())}")
                logging.debug(f"Full sample listing: {json.dumps(listings[0], indent=2)}")
                raw_listings.extend(listings)
        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error: {e}")
            break

        if not listings:
            logging.info(f"No more listings at from={from_idx}. Stopping.")
            break

        page_listings = []
        for item in listings:
            source = item.get('_source', {})
            possible_ids = {k: source.get(k) for k in ['id', 'listingId', '_id', 'assetId', 'listing_id', 'itemId', 'productId', 'sku', 'nftAddress']}
            logging.debug(f"Possible ID fields: {possible_ids}")
            
            listing_id = source.get('id') or source.get('nftAddress') or item.get('_id')
            if listing_id is None:
                logging.warning(f"Missing ID for listing: {json.dumps(item, indent=2)}")
                listing_id = str(hash(json.dumps(item, sort_keys=True)))
            logging.debug(f"Processing listing ID: {listing_id}")
            if listing_id in seen_ids:
                logging.debug(f"Skipping duplicate ID: {listing_id}")
                continue

            try:
                attributes = source.get('attributes', {})
                cleaned_item = {
                    'id': str(listing_id),
                    'name': source.get('name', '') or attributes.get('Name', '') or 'Unknown',
                    'description': source.get('description', '') or '',
                    'price': float(source.get('price', 0)) if str(source.get('price', '')).replace('.', '', 1).isdigit() else 0.0,
                    'spirit_type': source.get('spiritType', '') or source.get('type', '') or attributes.get('Type', '') or '',
                    'listed_date': source.get('listedDate', '') or '',
                    'attributes': attributes,
                    'image_url': source.get('imageUrl', '') or '',
                    'status': source.get('status', '') or '',
                    'producer': attributes.get('Producer', '') or source.get('producer', '') or '',
                    'volume': attributes.get('Size', '') or '',
                    'Age': attributes.get('Age', '') or attributes.get('Year Bottled', '') or attributes.get('Year Distilled', '') or '',
                    'ABV': attributes.get('ABV', '') or '',
                    'Size': attributes.get('Size', '') or ''
                }
                page_listings.append(cleaned_item)
                seen_ids.add(listing_id)
                logging.debug(f"Added listing: {cleaned_item['name']} (ID: {listing_id}, Price: {cleaned_item['price']}, Type: {cleaned_item['spirit_type']})")
            except (ValueError, TypeError) as e:
                logging.error(f"Error processing listing {listing_id}: {e}")
                continue

        all_listings.extend(page_listings)
        logging.info(f"Added {len(page_listings)} listings. Total: {len(all_listings)}")
        from_idx += size
        time.sleep(1)

    # CHANGED: Save JSON files to /data
    raw_json_file = f"/data/raw_json_{timestamp}.json"
    with open(raw_json_file, 'w', encoding='utf-8') as f:
        json.dump(raw_listings, f, indent=2)
    logging.info(f"Raw JSON saved to {raw_json_file}")

    output_file = f"/data/baxus_listings_{timestamp}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_listings, f, indent=2)
    logging.info(f"Saved {len(all_listings)} listings to {output_file}")
    return all_listings

# Database
def store_in_db(listings, db_file='/data/baxus.db'):  # CHANGED: Default to /data/baxus.db
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS listings (
        id TEXT PRIMARY KEY, name TEXT, description TEXT, price REAL,
        spirit_type TEXT, listed_date TEXT, attributes TEXT, image_url TEXT,
        status TEXT, producer TEXT, volume TEXT, Age TEXT, ABV TEXT, Size TEXT
    )''')
    valid_listings = 0
    for listing in listings:
        if listing['name'] == 'Unknown' and listing['price'] == 0.0 and not listing['spirit_type']:
            logging.warning(f"Skipping invalid listing {listing['id']}: {listing}")
            continue
        try:
            c.execute('INSERT OR REPLACE INTO listings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                      (listing['id'], listing['name'], listing['description'], listing['price'],
                       listing['spirit_type'], listing['listed_date'], json.dumps(listing['attributes']),
                       listing['image_url'], listing['status'], listing['producer'],
                       listing['volume'], listing['Age'], listing['ABV'], listing['Size']))
            logging.debug(f"Stored listing: {listing['name']} (ID: {listing['id']})")
            valid_listings += 1
        except sqlite3.Error as e:
            logging.error(f"Error storing listing {listing['id']}: {e}")
    conn.commit()
    c.execute("SELECT COUNT(*) FROM listings")
    count = c.fetchone()[0]
    logging.info(f"Stored {valid_listings} valid listings. Database contains {count} listings.")
    conn.close()

def get_db_summary(db_file='/data/baxus.db'):  # CHANGED: Default to /data/baxus.db
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    
    try:
        c.execute("SELECT DISTINCT spirit_type FROM listings WHERE spirit_type != ''")
        spirit_types = [row[0] for row in c.fetchall()]
        
        c.execute("SELECT DISTINCT producer FROM listings WHERE producer != ''")
        producers = [row[0] for row in c.fetchall()]
        
        c.execute("SELECT MIN(price), MAX(price), COUNT(*) FROM listings")
        min_price, max_price, count = c.fetchone() or (0, 0, 0)
        
        c.execute("SELECT spirit_type, COUNT(*) as cnt FROM listings WHERE spirit_type != '' GROUP BY spirit_type ORDER BY cnt DESC LIMIT 5")
        top_spirit_types = [row[0] for row in c.fetchall()]
        
        c.execute("SELECT producer, COUNT(*) as cnt FROM listings WHERE producer != '' GROUP BY producer ORDER BY cnt DESC LIMIT 5")
        top_producers = [row[0] for row in c.fetchall()]
        
        c.execute("SELECT DISTINCT json_extract(attributes, '$.Country') FROM listings WHERE json_extract(attributes, '$.Country') IS NOT NULL")
        countries = [row[0] for row in c.fetchall()]
        
        c.execute("SELECT DISTINCT json_extract(attributes, '$.Region') FROM listings WHERE json_extract(attributes, '$.Region') IS NOT NULL")
        regions = [row[0] for row in c.fetchall()]
        
        summary = {
            'spirit_types': spirit_types or ['Whisky', 'Bourbon', 'Scotch'],
            'producers': producers or ['Ardbeg', 'Macallan'],
            'top_spirit_types': top_spirit_types or ['Whisky', 'Bourbon', 'Scotch'],
            'top_producers': top_producers or ['Ardbeg', 'Macallan'],
            'countries': countries or ['Scotland', 'USA'],
            'regions': regions or ['Speyside', 'Kentucky'],
            'price_range': {'min': float(min_price or 0), 'max': float(max_price or 10000)},
            'total_listings': int(count or 0)
        }
    except sqlite3.Error as e:
        logging.error(f"Error getting db summary: {e}")
        summary = {
            'spirit_types': ['Whisky', 'Bourbon', 'Scotch'],
            'producers': ['Ardbeg', 'Macallan'],
            'top_spirit_types': ['Whisky', 'Bourbon', 'Scotch'],
            'top_producers': ['Ardbeg', 'Macallan'],
            'countries': ['Scotland', 'USA'],
            'regions': ['Speyside', 'Kentucky'],
            'price_range': {'min': 0.0, 'max': 10000.0},
            'total_listings': 0
        }
    finally:
        conn.close()
    return summary

# Bot
# CHANGED: Use environment variable for Google API key
async def parse_query_with_gemini(query: str) -> dict:
    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    prompt = f"""
    Parse the following user query for a whiskey marketplace bot into structured parameters.
    Extract:
    - spirit_type: Type of spirit (e.g., Bourbon, Scotch, Whisky, or empty string).
    - producer: Producer or distillery name (e.g., Macallan, Ardbeg, or empty string).
    - price: Maximum price if mentioned (numeric, or null if not specified).
    - Age: Age of whiskey (e.g., '12', '81', or empty string).
    - ABV: Alcohol by volume (e.g., '40', '41.6', or empty string).
    - Size: Bottle size in ml (e.g., '750', '700', or empty string).
    - country: Country of origin (e.g., 'Scotland', or empty string).
    - region: Region (e.g., 'Speyside', or empty string).
    - cask_type: Cask type (e.g., 'Sherry Oak', or empty string).
    - series: Series name (e.g., 'The Reach', or empty string).
    - year_bottled: Year bottled (e.g., '2022', or empty string).
    - year_distilled: Year distilled (e.g., '1940', or empty string).
    - query: General search term or keywords (e.g., 'cheap', 'smooth', or empty string).
    - intent: User intent (e.g., 'search', 'recommend', 'describe', 'compare', or 'general').
    Return JSON. Use empty strings or null for fields not present.
    Examples:
    - "Show me bourbons under $200" -> {{"spirit_type": "Bourbon", "producer": "", "price": 200, "Age": "", "ABV": "", "Size": "", "country": "", "region": "", "cask_type": "", "series": "", "year_bottled": "", "year_distilled": "", "query": "", "intent": "search"}}
    - "Recommend a Scotch aged 12 years" -> {{"spirit_type": "Scotch", "producer": "", "price": null, "Age": "12", "ABV": "", "Size": "", "country": "", "region": "", "cask_type": "", "series": "", "year_bottled": "", "year_distilled": "", "query": "", "intent": "recommend"}}
    - "Tell me about Macallan from Speyside" -> {{"spirit_type": "", "producer": "Macallan", "price": null, "Age": "", "ABV": "", "Size": "", "country": "", "region": "Speyside", "cask_type": "", "series": "", "year_bottled": "", "year_distilled": "", "query": "", "intent": "describe"}}
    - "Whiskies with ABV over 40%" -> {{"spirit_type": "", "producer": "", "price": null, "Age": "", "ABV": "40", "Size": "", "country": "", "region": "", "cask_type": "", "series": "", "year_bottled": "", "year_distilled": "", "query": "", "intent": "search"}}
    Query: "{query}"
    """
    try:
        response = model.generate_content(prompt)
        return json.loads(response.text.strip('```json\n').strip('```'))
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"Gemini response error: {e}, response: {response.text if response else 'None'}")
        return {"spirit_type": "", "producer": "", "price": None, "Age": "", "ABV": "", "Size": "", "country": "", "region": "", "cask_type": "", "series": "", "year_bottled": "", "year_distilled": "", "query": query, "intent": "general"}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Welcome to Baxus Whinery Bot! You can:\n"
        "- Use commands like /list, /search <term>, /type <type>, /producer <name>, /types\n"
        "- Or chat, e.g., 'Recommend a Scotch aged 12 years', 'Whiskies from Speyside', or 'Show me Macallan with ABV over 40%'"
    )

async def list_listings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('/data/baxus.db')  # CHANGED: Use /data/baxus.db
    c = conn.cursor()
    try:
        c.execute("SELECT name, price, spirit_type, id, producer, volume, Age, ABV, Size FROM listings ORDER BY RANDOM() LIMIT 5")
        listings = c.fetchall()
        logging.debug(f"Fetched {len(listings)} listings for /list")
        if not listings:
            await update.message.reply_text("No listings available. The database may be empty. Try again later.")
            return
        response = []
        for name, price, spirit_type, id, producer, volume, Age, ABV, Size in listings:
            name = name or "Unknown"
            price = f"${price:.2f}" if price is not None else "Price not available"
            spirit_type = spirit_type or "Unknown type"
            producer = producer or "Unknown producer"
            volume = volume or "Unknown volume"
            Age = Age or "Unknown Age"
            ABV = f"{ABV}%" if ABV else "Unknown ABV"
            Size = f"{Size}" if Size else "Unknown Size"
            response.append(f"{name} - {price} ({spirit_type}) by {producer}, {volume}, Age: {Age}, ABV: {ABV}, Size: {Size} - https://baxus.co/asset/{id}")
        await update.message.reply_text("\n".join(response))
    except sqlite3.Error as e:
        logging.error(f"Database error in list_listings: {e}")
        await update.message.reply_text("Error accessing the database. Please try again later.")
    finally:
        conn.close()

async def search_listings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = " ".join(context.args).lower()
    if not query:
        await update.message.reply_text("Please provide a search term, e.g., /search whisky or /search price from 100 to 500 or /search age above 12")
        return
    logging.debug(f"Executing search_listings with query: {query}")

    import re

    # Initialize search parameters
    price_min = None
    price_max = None
    age_min = None
    age_max = None
    text_query = query

    # Parse price range
    price_patterns = [
        r'price\s*(?:from|above|over)\s*(\d+(?:\.\d+)?)',  # price from/above/over 100
        r'price\s*(?:to|below|under)\s*(\d+(?:\.\d+)?)',   # price to/below/under 500
        r'price\s*between\s*(\d+(?:\.\d+)?)\s*(?:and|to|-)\s*(\d+(?:\.\d+)?)',  # price between 100 and 500
        r'price\s*(\d+(?:\.\d+)?)\s*(?:to|-)\s*(\d+(?:\.\d+)?)'  # price 100 to 500
    ]
    for pattern in price_patterns:
        match = re.search(pattern, query)
        if match:
            if len(match.groups()) == 1:
                value = float(match.group(1))
                if 'above' in pattern or 'over' in query:
                    price_min = value
                elif 'below' in pattern or 'under' in query:
                    price_max = value
            else:
                price_min = float(match.group(1))
                price_max = float(match.group(2))
            text_query = re.sub(pattern, '', query).strip()
            break

    # Parse age range
    age_patterns = [
        r'age\s*(?:from|above|over)\s*(\d+)',  # age from/above/over 12
        r'age\s*(?:to|below|under)\s*(\d+)',   # age to/below/under 18
        r'age\s*between\s*(\d+)\s*(?:and|to|-)\s*(\d+)',  # age between 12 and 18
        r'age\s*(\d+)\s*(?:to|-)\s*(\d+)'      # age 12 to 18
    ]
    for pattern in age_patterns:
        match = re.search(pattern, query)
        if match:
            if len(match.groups()) == 1:
                value = int(match.group(1))
                if 'above' in pattern or 'over' in query:
                    age_min = value
                elif 'below' in pattern or 'under' in query:
                    age_max = value
            else:
                age_min = int(match.group(1))
                age_max = int(match.group(2))
            text_query = re.sub(pattern, '', query).strip()
            break

    # Clean up text_query
    text_query = ' '.join(text_query.split()) if text_query else ''

    try:
        conn = sqlite3.connect('/data/baxus.db')  # CHANGED: Use /data/baxus.db
        c = conn.cursor()
        sql = "SELECT name, price, spirit_type, id, producer, volume, Age, ABV, Size FROM listings WHERE 1=1"
        conditions = []
        query_params = []

        # Add text search conditions
        if text_query and not (price_min or price_max or age_min or age_max):
            conditions.append("(lower(name) LIKE ? OR lower(description) LIKE ?)")
            query_params.extend([f"%{text_query}%", f"%{text_query}%"])

        # Add price range conditions
        if price_min is not None:
            conditions.append("price >= ?")
            query_params.append(price_min)
        if price_max is not None:
            conditions.append("price <= ?")
            query_params.append(price_max)

        # Add age range conditions
        if age_min is not None:
            conditions.append("CAST(Age AS INTEGER) >= ?")
            query_params.append(age_min)
        if age_max is not None:
            conditions.append("CAST(Age AS INTEGER) <= ?")
            query_params.append(age_max)

        # Build SQL query
        if conditions:
            sql += " AND " + " AND ".join(conditions)
        sql += " LIMIT 5"

        logging.debug(f"Executing SQL: {sql} with params: {query_params}")
        c.execute(sql, query_params)
        listings = c.fetchall()
        logging.debug(f"Fetched {len(listings)} listings for /search {query}")

        if not listings:
            await update.message.reply_text(f"No listings found for '{query}'. Try: /search whisky, /search price below 200, or /search age between 12 and 18")
            return

        response = []
        for name, price, spirit_type, id, producer, volume, Age, ABV, Size in listings:
            name = name or "Unknown"
            price = f"${price:.2f}" if price is not None else "Price not available"
            spirit_type = spirit_type or "Unknown type"
            producer = producer or "Unknown producer"
            volume = volume or "Unknown volume"
            Age = Age or "Unknown Age"
            ABV = f"{ABV}%" if ABV else "Unknown ABV"
            Size = f"{Size}ml" if Size else "Unknown Size"
            response.append(f"{name} - {price} ({spirit_type}) by {producer}, {volume}, Age: {Age}, ABV: {ABV}, Size: {Size} - https://baxus.co/asset/{id}")
        await update.message.reply_text("\n".join(response))
    except sqlite3.Error as e:
        logging.error(f"Database error in search_listings: {e}")
        await update.message.reply_text("Error accessing the database. Please try again later.")
    finally:
        conn.close()

async def filter_by_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    spirit_type = " ".join(context.args).lower()
    if not spirit_type:
        await update.message.reply_text("Please provide a spirit type, e.g., /type Whisky")
        return
    conn = sqlite3.connect('/data/baxus.db')  # CHANGED: Use /data/baxus.db
    c = conn.cursor()
    try:
        c.execute("SELECT name, price, spirit_type, id, producer, volume, Age, ABV, Size FROM listings "
                  "WHERE lower(spirit_type) LIKE ? LIMIT 5", (f"%{spirit_type}%",))
        listings = c.fetchall()
        logging.debug(f"Fetched {len(listings)} listings for /type {spirit_type}")
        if not listings:
            await update.message.reply_text(f"No listings found for type '{spirit_type}'. Try: Whisky, Bourbon, Scotch, etc.")
            return
        response = []
        for name, price, spirit_type, id, producer, volume, Age, ABV, Size in listings:
            name = name or "Unknown"
            price = f"${price:.2f}" if price is not None else "Price not available"
            spirit_type = spirit_type or "Unknown type"
            producer = producer or "Unknown producer"
            volume = volume or "Unknown volume"
            Age = Age or "Unknown Age"
            ABV = f"{ABV}%" if ABV else "Unknown ABV"
            Size = f"{Size}ml" if Size else "Unknown Size"
            response.append(f"{name} - {price} ({spirit_type}) by {producer}, {volume}, Age: {Age}, ABV: {ABV}, Size: {Size} - https://baxus.co/asset/{id}")
        await update.message.reply_text("\n".join(response))
    except sqlite3.Error as e:
        logging.error(f"Database error in filter_by_type: {e}")
        await update.message.reply_text("Error accessing the database. Please try again later.")
    finally:
        conn.close()

async def filter_by_producer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    producer = " ".join(context.args).lower()
    if not producer:
        await update.message.reply_text("Please provide a producer, e.g., /producer Ardbeg")
        return
    conn = sqlite3.connect('/data/baxus.db')  # CHANGED: Use /data/baxus.db
    c = conn.cursor()
    try:
        c.execute("SELECT name, price, spirit_type, id, producer, volume, Age, ABV, Size FROM listings "
                  "WHERE lower(producer) LIKE ? LIMIT 5", (f"%{producer}%",))
        listings = c.fetchall()
        logging.debug(f"Fetched {len(listings)} listings for /producer {producer}")
        if not listings:
            await update.message.reply_text(f"No listings found for producer '{producer}'. Try: Ardbeg, Macallan, etc.")
            return
        response = []
        for name, price, spirit_type, id, producer, volume, Age, ABV, Size in listings:
            name = name or "Unknown"
            price = f"${price:.2f}" if price is not None else "Price not available"
            spirit_type = spirit_type or "Unknown type"
            producer Paproducer or "Unknown producer"
            volume = volume or "Unknown volume"
            Age = Age or "Unknown Age"
            ABV = f"{ABV}%" if ABV else "Unknown ABV"
            Size = f"{Size}ml" if Size else "Unknown Size"
            response.append(f"{name} - {price} ({spirit_type}) by {producer}, {volume}, Age: {Age}, ABV: {ABV}, Size: {Size} - https://baxus.co/asset/{id}")
        await update.message.reply_text("\n".join(response))
    except sqlite3.Error as e:
        logging.error(f"Database error in filter_by_producer: {e}")
        await update.message.reply_text("Error accessing the database. Please try again later.")
    finally:
        conn.close()

async def list_types(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('/data/baxus.db')  # CHANGED: Use /data/baxus.db
    c = conn.cursor()
    try:
        c.execute("SELECT DISTINCT spirit_type FROM listings WHERE spirit_type != '' LIMIT 10")
        types = c.fetchall()
        logging.debug(f"Fetched {len(types)} spirit types for /types")
        if not types:
            await update.message.reply_text("No spirit types found in the database.")
            return
        response = "\n".join([t[0] for t in types])
        await update.message.reply_text(response)
    except sqlite3.Error as e:
        logging.error(f"Database error in list_types: {e}")
        await update.message.reply_text("Error accessing the database. Please try again later.")
    finally:
        conn.close()

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text
    db_summary = get_db_summary()
    
    params = await parse_query_with_gemini(query)
    logging.debug(f"Gemini parsed params: {params}")
    conn = sqlite3.connect('/data/baxus.db')  # CHANGED: Use /data/baxus.db
    c = conn.cursor()
    try:
        sql = "SELECT name, price, spirit_type, id, producer, volume, Age, ABV, Size, attributes FROM listings WHERE 1=1"
        conditions = []
        query_params = []
        order_by = ""
        
        if params['spirit_type']:
            conditions.append("lower(spirit_type) LIKE ?")
            query_params.append(f"%{params['spirit_type'].lower()}%")
        if params['producer']:
            conditions.append("lower(producer) LIKE ?")
            query_params.append(f"%{params['producer'].lower()}%")
        if params['Age']:
            conditions.append("Age = ?")
            query_params.append(params['Age'])
        if params['ABV']:
            conditions.append("CAST(ABV AS REAL) >= ?")
            query_params.append(float(params['ABV']))
        if params['Size']:
            conditions.append("Size = ?")
            query_params.append(params['Size'])
        if params['country']:
            conditions.append("json_extract(attributes, '$.Country') LIKE ?")
            query_params.append(f"%{params['country']}%")
        if params['region']:
            conditions.append("json_extract(attributes, '$.Region') LIKE ?")
            query_params.append(f"%{params['region']}%")
        if params['cask_type']:
            conditions.append("json_extract(attributes, '$.Cask Type') LIKE ?")
            query_params.append(f"%{params['cask_type']}%")
        if params['series']:
            conditions.append("json_extract(attributes, '$.Series') LIKE ?")
            query_params.append(f"%{params['series']}%")
        if params['year_bottled']:
            conditions.append("json_extract(attributes, '$.Year Bottled') = ?")
            query_params.append(params['year_bottled'])
        if params['year_distilled']:
            conditions.append("json_extract(attributes, '$.Year Distilled') = ?")
            query_params.append(params['year_distilled'])
        if params['query']:
            conditions.append("(lower(name) LIKE ? OR lower(description) LIKE ?)")
            query_params.extend([f"%{params['query'].lower()}%", f"%{params['query'].lower()}%"])
        if params['price'] is not None:
            conditions.append("price <= ?")
            query_params.append(params['price'])
        
        if params['intent'] == 'recommend':
            order_by = " ORDER BY price ASC"
        elif params['intent'] == 'compare':
            order_by = " ORDER BY price ASC"
        else:
            order_by = " ORDER BY RANDOM()"
        
        if conditions:
            sql += " AND " + " AND ".join(conditions)
        sql += order_by
        c.execute(sql + " LIMIT 5", query_params)
        listings = c.fetchall()
        logging.debug(f"Fetched {len(listings)} listings for query: {query}")
        
        listings_summary = [
            f"{name or 'Unknown'} - {'${:.2f}'.format(price) if price is not None else 'N/A'} ({spirit_type or 'Unknown'}) by {producer or 'Unknown'}, {volume or 'N/A'}, Age: {Age or 'N/A'}, ABV: {ABV or 'N/A'}%, Size: {Size or 'N/A'}ml"
            for name, price, spirit_type, id, producer, volume, Age, ABV, Size, attributes in listings
        ]
        
        genai.configure(api_key=GOOGLE_API_KEY)  # CHANGED: Use environment variable
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"""
        You are a conversational AI for a whiskey marketplace bot, designed to assist users with finding, recommending, and learning about whiskeys. The user asked: "{query}".
        
        Database summary:
        - Total listings: {db_summary['total_listings']}
        - Spirit types: {', '.join(db_summary['spirit_types'])}
        - Popular spirit types: {', '.join(db_summary['top_spirit_types'])}
        - Producers: {', '.join(db_summary['producers'])}
        - Popular producers: {', '.join(db_summary['top_producers'])}
        - Countries: {', '.join(db_summary['countries'])}
        - Regions: {', '.join(db_summary['regions'])}
        - Price range: ${db_summary['price_range']['min']} - ${db_summary['price_range']['max']}
        
        Matching listings (up to 5):
        {', '.join(listings_summary) or 'None'}
        
        User intent: {params['intent']}
        
        Respond conversationally based on the user's intent:
        - If intent is 'search', list matching whiskeys with details (name, price, spirit type, producer, volume, Age, ABV, Size, country, region, cask type, series, year bottled, year distilled).
        - If intent is 'recommend', suggest whiskeys that match the query, prioritizing popular or well-priced options.
        - If intent is 'describe', provide details about the whiskey, producer, or type, using the listings or general knowledge.
        - If intent is 'compare', compare the listed whiskeys by price, Age, ABV, Size, or other attributes.
        - If intent is 'budget_plan', offer combinations that match the budget and quantity.
        - If intent is 'Range', give a range ( -, between, from, below, above) with details ( name, price, spirit type, producer, volume, ABV, Size, country, region, cask type, series, year bottled, year distilled)
        - If intent is 'event_recommendation', suggest value-for-money bottles for groups.
        - For any unknown intent, respond helpfully using database knowledge.
        - If intent is 'general' or unclear, Use friendly, knowledgeable tone. Avoid markdown. Keep it natural like a whiskey expert talking to a friend.
        
        Queryable fields include: name, price, spirit type, producer, volume, Age, ABV (%), Size (ml), country, region, cask type, series, year bottled, year distilled, description, listed date, status, and other attributes (except Blurhash, PackageShot, and image_url).
        If no listings match, suggest alternatives based on the database summary (e.g., popular types or producers).
        Include details like Age, ABV, Size, or region where relevant.
        Keep the tone friendly, engaging, and knowledgeable, as if you're a whiskey expert chatting with a friend.
        """
        try:
            response = model.generate_content(prompt)
            await update.message.reply_text(response.text)
        except Exception as e:
            logging.error(f"Gemini conversational response error: {e}")
            await update.message.reply_text("Sorry, I couldn't process your request. Try something like 'Recommend a Scotch aged 12 years' or 'Whiskies from Speyside'.")
    except sqlite3.Error as e:
        logging.error(f"Database error in handle_message: {e}")
        await update.message.reply_text("Error accessing the database. Please try again later.")
    finally:
        conn.close()

def update_listings():
    listings = fetch_listings()
    store_in_db(listings)
    logging.info("Listings updated.")

def run_scheduler():
    schedule.every().day.at("02:00").do(update_listings)
    while True:
        schedule.run_pending()
        time.sleep(60)

async def main():
    # CHANGED: Add try-except for robust error handling
    try:
        listings = fetch_listings()
        if listings:
            store_in_db(listings)
            logging.info(f"Initial database population completed with {len(listings)} listings.")
        else:
            logging.warning("No listings fetched during initial population.")
    except Exception as e:
        logging.error(f"Error during initial setup: {e}")

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("list", list_listings))
    application.add_handler(CommandHandler("search", search_listings))
    application.add_handler(CommandHandler("type", filter_by_type))
    application.add_handler(CommandHandler("producer", filter_by_producer))
    application.add_handler(CommandHandler("types", list_types))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
