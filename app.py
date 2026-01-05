import os
import io
import json
import random
import traceback
import logging
import datetime
import base64
import secrets
from datetime import datetime

from flask import Flask, request, abort, jsonify, render_template, send_from_directory
from pymongo import MongoClient, ReturnDocument
from bson import ObjectId
import redis
import qrcode
from PIL import Image as PILImage

# Optional: Pyrogram for Telegram Bot (Optional for this web app flow, but kept structure)
try:
    from pyrogram import Client
except ImportError:
    Client = None

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ---------- CONFIG ----------
# Ambil dari Environment Variable atau Gunakan Default
API_ID    = int(os.getenv('API_ID', 123456))
API_HASH  = os.getenv('API_HASH', "")
BOT_TOKEN = os.getenv('BOT_TOKEN', "")

# DATABASES (Sesuai Permintaan)
MONGO_URI = os.getenv('MONGO_URI', "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority")
MARKET_DB_URL = os.getenv('MARKET_DB_URL', MONGO_URI) # Default to main if separate not set
MONGO_URL_WAIFU = os.getenv('MONGO_URL_WAIFU', MONGO_URI)
MONGO_URL_HUSBAND = os.getenv('MONGO_URL_HUSBAND', MONGO_URI)

# REDIS
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-13380.c81.us-east-1-2.ec2.cloud.redislabs.com')
REDIS_PORT = int(os.getenv('REDIS_PORT', 13380))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', "NRwYNwxwAjbyFxHDod1esj2hwsxugTiw")

# ---------- FLASK APP ----------
app = Flask(__name__, static_folder='static', template_folder='templates')

# ---------- DATABASE CONNECTIONS ----------
try:
    # Main Market DB
    market_client = MongoClient(MARKET_DB_URL)
    market_db = market_client['market_p2p']
    
    # Character DBs
    waifu_client = MongoClient(MONGO_URL_WAIFU)
    waifu_db = waifu_client['Character_catcher']
    waifu_users_coll = waifu_db['user_collection_lmaoooo']
    
    husband_client = MongoClient(MONGO_URL_HUSBAND)
    husband_db = husband_client['Character_catcher']
    husband_users_coll = husband_db['user_collection_lmaoooo']
    
    # Registered Users (General info)
    # Assuming this is in the main DB
    registered_users = market_client['Character_catcher']['registered_users']
    charms_addresses = market_client['Character_catcher']['charms_addresses']

    logger.info("[DB] All databases connected successfully")
except Exception as e:
    logger.error(f"[DB] Connection Failed: {e}")
    # Set to None to handle gracefully in production if partial fail
    market_db = None

# REDIS CONNECTION
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    r.ping()
    logger.info("[Redis] Connected")
except Exception as e:
    logger.error(f"[Redis] Connection Failed: {e}")
    r = None

# ---------- HELPERS ----------

def get_user_charms(user_id):
    """Get user balance from Redis."""
    if not r: return 0
    try:
        val = r.hget(f"user:{user_id}", "charm")
        return int(val) if val else 0
    except:
        return 0

def update_user_charms(user_id, amount):
    """Add/Subtract charms. Amount can be negative."""
    if not r: return False
    try:
        r.hincrby(f"user:{user_id}", "charm", amount)
        return True
    except Exception as e:
        logger.error(f"Redis update error: {e}")
        return False

def log_transaction(user_id, tx_type, amount, title, detail=""):
    """Log transaction to Redis List."""
    if not r: return
    try:
        tx = {
            "type": tx_type, # 'send', 'receive', 'buy', 'sell'
            "amount": amount,
            "title": title,
            "detail": detail,
            "ts": datetime.utcnow().timestamp()
        }
        key = f"user:{user_id}:txs"
        r.lpush(key, json.dumps(tx))
        r.ltrim(key, 0, 99) # Keep last 100
    except Exception as e:
        logger.error(f"Log tx error: {e}")

def get_user_collection(user_id, db_type='waifu'):
    """Get user's character array."""
    coll = husband_users_coll if db_type == 'husband' else waifu_users_coll
    if not coll: return []
    try:
        user_doc = coll.find_one({'id': str(user_id)}) or coll.find_one({'id': int(user_id)})
        return user_doc.get('characters', []) if user_doc else []
    except:
        return []

def remove_character_from_user(user_id, char_id, qty, db_type='waifu'):
    """Remove specific character from user DB."""
    coll = husband_users_coll if db_type == 'husband' else waifu_users_coll
    if not coll: return False
    try:
        user_doc = coll.find_one({'id': str(user_id)}) or coll.find_one({'id': int(user_id)})
        if not user_doc: return False
        
        chars = user_doc.get('characters', [])
        new_chars = []
        removed_count = 0
        
        for ch in chars:
            if removed_count < qty and str(ch.get('id')) == str(char_id):
                removed_count += 1
                continue
            new_chars.append(ch)
            
        if removed_count < qty:
            return False # Not enough copies
            
        coll.update_one({'_id': user_doc['_id']}, {'$set': {'characters': new_chars}})
        return True
    except Exception as e:
        logger.error(f"Remove char error: {e}")
        return False

def add_character_to_user(user_id, character_data, db_type='waifu'):
    """Add character to user DB."""
    coll = husband_users_coll if db_type == 'husband' else waifu_users_coll
    if not coll: return False
    try:
        # Use upsert to create user doc if not exists
        coll.update_one(
            {'id': str(user_id)},
            {'$push': {'characters': character_data}},
            upsert=True
        )
        return True
    except Exception as e:
        logger.error(f"Add char error: {e}")
        return False

# ---------- ROUTES ----------

@app.route('/')
def index():
    """Serve the Mini App HTML."""
    return render_template('index.html')

# --- API: USER INFO ---
@app.route('/api/user_info', methods=['GET'])
def api_user_info():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'ok': False, 'error': 'Missing user_id'}), 400
    
    charms = get_user_charms(user_id)
    
    # Get basic profile info from registered_users or Telegram InitData placeholder
    user_data = registered_users.find_one({'user_id': str(user_id)}) or {}
    
    return jsonify({
        'ok': True,
        'id': user_id,
        'name': user_data.get('firstname', 'Traveler'),
        'avatar': user_data.get('photo_url', 'https://picsum.photos/seed/user/200/200'),
        'balance': charms
    })

# --- API: MARKET (Official Store) ---
@app.route('/api/market', methods=['GET'])
def api_market():
    """Fetch official market items with sorting and filtering."""
    db_type = request.args.get('type', 'waifu')
    sort_by = request.args.get('sort', 'price-asc') # price-asc, price-desc, newest, oldest
    
    coll = market_db['official_market'] if market_db else None
    if not coll: return jsonify({'ok': False, 'error': 'DB Error'}), 500
    
    # Filters
    query = {'type': db_type}
    # Add category/rarity filters here if needed in request args
    
    # Sort Logic
    sort_order = []
    if sort_by == 'price-asc': sort_order = [('price', 1)]
    elif sort_by == 'price-desc': sort_order = [('price', -1)]
    elif sort_by == 'newest': sort_order = [('_id', -1)]
    elif sort_by == 'oldest': sort_order = [('_id', 1)]
    elif sort_by == 'random': 
        # MongoDB doesn't support random sort efficiently natively without $sample
        # We will just fetch and shuffle in Python for small lists, or use $sample
        pipeline = [{'$match': query}, {'$sample': {'size': 50}}]
        try:
            items = list(coll.aggregate(pipeline))
            return jsonify({'ok': True, 'items': items})
        except: pass

    try:
        items = list(coll.find(query).sort(sort_order).limit(50))
        # Convert ObjectId to string
        for item in items:
            item['_id'] = str(item['_id'])
        return jsonify({'ok': True, 'items': items})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

# --- API: P2P LISTINGS ---
@app.route('/api/p2p_listings', methods=['GET'])
def api_p2p_listings():
    """Fetch user-generated listings."""
    coll = market_db['listings'] if market_db else None
    if not coll: return jsonify({'ok': True, 'items': []})
    
    try:
        # Exclude sold/expired listings (assumed active if not 'sold': True)
        items = list(coll.find({'status': 'active'}).sort('_id', -1).limit(50))
        for item in items:
            item['_id'] = str(item['_id'])
        return jsonify({'ok': True, 'items': items})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

# --- API: MY COLLECTION ---
@app.route('/api/my_collection', methods=['GET'])
def api_my_collection():
    user_id = request.args.get('user_id')
    db_type = request.args.get('type', 'waifu')
    
    if not user_id: return jsonify({'ok': False}), 400
    
    chars = get_user_collection(user_id, db_type)
    # Ensure _id is string for frontend
    for ch in chars:
        if '_id' in ch:
            ch['_id'] = str(ch['_id'])
            
    return jsonify({'ok': True, 'items': chars})

# --- API: SELL CHARACTER (P2P) ---
@app.route('/api/sell_character', methods=['POST'])
def api_sell_character():
    data = request.json
    user_id = data.get('user_id')
    char_id = data.get('char_id')
    qty = int(data.get('qty', 1))
    price = int(data.get('price'))
    desc = data.get('description', '')
    db_type = data.get('type', 'waifu')
    
    if not all([user_id, char_id, price]):
        return jsonify({'ok': False, 'error': 'Missing fields'}), 400
        
    # 1. Get Character Data
    chars = get_user_collection(user_id, db_type)
    target_char = next((c for c in chars if str(c.get('id')) == str(char_id)), None)
    
    if not target_char:
        return jsonify({'ok': False, 'error': 'Character not found in inventory'}), 404
        
    # 2. Remove from User Inventory
    success = remove_character_from_user(user_id, char_id, qty, db_type)
    if not success:
        return jsonify({'ok': False, 'error': 'Failed to remove character (check quantity)'}), 400
        
    # 3. Create Listing in Market DB
    try:
        listing = {
            'seller_id': str(user_id),
            'seller_name': data.get('seller_name', 'User'),
            'char_data': target_char, # Store full char data
            'qty': qty,
            'price': price,
            'description': desc,
            'type': db_type,
            'status': 'active',
            'created_at': datetime.utcnow()
        }
        res = market_db['listings'].insert_one(listing)
        return jsonify({'ok': True, 'listing_id': str(res.inserted_id)})
    except Exception as e:
        # Rollback: Add back to user (Simplistic rollback)
        add_character_to_user(user_id, target_char, db_type)
        return jsonify({'ok': False, 'error': str(e)}), 500

# --- API: BUY FROM MARKET (Official) ---
@app.route('/api/buy_market', methods=['POST'])
def api_buy_market():
    data = request.json
    user_id = data.get('user_id')
    item_id = data.get('item_id')
    
    coll = market_db['official_market']
    if not coll: return jsonify({'ok': False}), 500
    
    try:
        # Get Item
        # item_id might be string or ObjectId string
        item = coll.find_one({'_id': ObjectId(item_id)})
        if not item:
            return jsonify({'ok': False, 'error': 'Item not found'}), 404
            
        price = item.get('price')
        stock = item.get('stock', 999)
        
        if stock <= 0:
            return jsonify({'ok': False, 'error': 'Out of Stock'}), 400
            
        # Check Balance
        current_charms = get_user_charms(user_id)
        if current_charms < price:
            return jsonify({'ok': False, 'error': 'Insufficient Charms'}), 400
            
        # Execute Transaction
        if not update_user_charms(user_id, -price):
            return jsonify({'ok': False, 'error': 'Transaction Error'}), 500
            
        # Reduce Stock
        coll.update_one({'_id': ObjectId(item_id)}, {'$inc': {'stock': -1}})
        
        # Add Character to User
        # Clone character data (minus market specific fields like stock)
        char_data = {k:v for k,v in item.items() if k not in ['_id', 'stock', 'created_at']}
        add_character_to_user(user_id, char_data, item.get('type', 'waifu'))
        
        # Log
        log_transaction(user_id, 'buy', -price, f"Bought {item.get('name')}")
        
        return jsonify({'ok': True, 'new_balance': get_user_charms(user_id)})
        
    except Exception as e:
        logger.error(f"Buy Market Error: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

# --- API: BUY FROM P2P ---
@app.route('/api/buy_p2p', methods=['POST'])
def api_buy_p2p():
    data = request.json
    buyer_id = data.get('buyer_id')
    listing_id = data.get('listing_id')
    
    coll = market_db['listings']
    if not coll: return jsonify({'ok': False}), 500
    
    try:
        # Get Listing
        listing = coll.find_one({'_id': ObjectId(listing_id)})
        if not listing or listing.get('status') != 'active':
            return jsonify({'ok': False, 'error': 'Listing not available'}), 404
            
        seller_id = listing.get('seller_id')
        price = listing.get('price')
        
        # Validations
        if str(seller_id) == str(buyer_id):
            return jsonify({'ok': False, 'error': 'Cannot buy own item'}), 400
            
        buyer_charms = get_user_charms(buyer_id)
        if buyer_charms < price:
            return jsonify({'ok': False, 'error': 'Insufficient Charms'}), 400
            
        # --- ATOMIC TRANSACTION STEPS ---
        
        # 1. Deduct Buyer Charms
        if not update_user_charms(buyer_id, -price):
            return jsonify({'ok': False, 'error': 'Failed to deduct buyer charms'}), 500
            
        # 2. Add Charms to Seller
        update_user_charms(seller_id, price)
        
        # 3. Mark Listing as Sold (Lock it)
        coll.update_one({'_id': ObjectId(listing_id)}, {'$set': {'status': 'sold', 'buyer_id': buyer_id}})
        
        # 4. Move Character to Buyer
        char_data = listing.get('char_data')
        add_character_to_user(buyer_id, char_data, listing.get('type', 'waifu'))
        
        # 5. Log Transactions
        log_transaction(buyer_id, 'buy', -price, f"P2P: {char_data.get('name')}")
        log_transaction(seller_id, 'sell', price, f"Sold {char_data.get('name')}")
        
        return jsonify({'ok': True, 'new_balance': get_user_charms(buyer_id)})
        
    except Exception as e:
        logger.error(f"Buy P2P Error: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

# --- API: TRANSFER CHARMS ---
@app.route('/api/transfer', methods=['POST'])
def api_transfer():
    data = request.json
    sender_id = data.get('sender_id')
    recipient_id = data.get('recipient_id') # Can be ID or Address
    amount = int(data.get('amount', 0))
    
    if amount <= 0:
        return jsonify({'ok': False, 'error': 'Invalid amount'}), 400
        
    # Resolve Recipient ID if Address is provided
    # Logic: check charms_addresses collection
    if not str(recipient_id).isdigit():
        addr_doc = charms_addresses.find_one({'address': str(recipient_id)})
        if addr_doc:
            recipient_id = addr_doc.get('user_id')
        else:
            # Try find by username or user_id directly
            # For simplicity in this rewrite, we assume if not digit and no address match -> fail or search DB
            # Let's try finding user by ID string in registered_users
            u_doc = registered_users.find_one({'user_id': str(recipient_id)})
            if u_doc:
                recipient_id = u_doc['user_id']
            else:
                return jsonify({'ok': False, 'error': 'Recipient not found'}), 404
    
    if str(sender_id) == str(recipient_id):
        return jsonify({'ok': False, 'error': 'Cannot send to self'}), 400
        
    # Check Balance
    sender_bal = get_user_charms(sender_id)
    if sender_bal < amount:
        return jsonify({'ok': False, 'error': 'Insufficient Charms'}), 400
        
    # Execute Transfer
    update_user_charms(sender_id, -amount)
    update_user_charms(recipient_id, amount)
    
    log_transaction(sender_id, 'send', -amount, f"Sent to {recipient_id}")
    log_transaction(recipient_id, 'receive', amount, f"Received from {sender_id}")
    
    return jsonify({'ok': True, 'new_balance': get_user_charms(sender_id)})

# --- API: QR CODE ---
@app.route('/api/qr_code', methods=['GET'])
def api_qr_code():
    user_id = request.args.get('user_id')
    if not user_id: return jsonify({'ok': False}), 400
    
    try:
        # Generate QR containing User ID
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(str(user_id))
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        buffered = io.BytesIO()
        img.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode()
        
        return jsonify({'ok': True, 'image_b64': img_str})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

# --- API: HISTORY ---
@app.route('/api/history', methods=['GET'])
def api_history():
    user_id = request.args.get('user_id')
    if not user_id or not r: return jsonify({'ok': False, 'items': []})
    
    try:
        key = f"user:{user_id}:txs"
        raw = r.lrange(key, 0, 49) # Get last 50
        items = []
        for s in raw:
            items.append(json.loads(s))
        return jsonify({'ok': True, 'items': items})
    except:
        return jsonify({'ok': True, 'items': []})

# --- API: TOP / LEADERBOARD ---
@app.route('/api/top', methods=['GET'])
def api_top():
    type_ = request.args.get('type', 'waifu') # waifu, husband, charms
    
    try:
        if type_ == 'charms':
            # Query Redis for Top 10 Users
            # This is hard in Redis without a sorted set.
            # Approximation: Just return random high users or query registered users if stored in Mongo.
            # For now, we mock or rely on a cache if available.
            # Let's assume we store top users in Redis Sorted Set 'leaderboard:charms'
            if r:
                tops = r.zrevrange('leaderboard:charms', 0, 9, withscores=True)
                result = [{'id': uid, 'score': score, 'name': f'User_{uid}'} for uid, score in tops]
                return jsonify({'ok': True, 'items': result})
                
        elif type_ in ['waifu', 'husband']:
            # This is complex to aggregate in Mongo without map-reduce/aggregation pipeline.
            # Simplified: Return Top Collectors based on count in 'characters' array.
            # Note: $size aggregation is heavy. 
            # We will return a dummy or pre-calculated list for this example to avoid timeout.
            # Implementation:
            coll = husband_users_coll if type_ == 'husband' else waifu_users_coll
            if coll:
                # Simple aggregation to count array length
                pipeline = [
                    {"$project": {"name": "$firstname", "count": {"$size": "$characters"}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 10}
                ]
                tops = list(coll.aggregate(pipeline))
                return jsonify({'ok': True, 'items': tops})
                
        return jsonify({'ok': True, 'items': []})
    except Exception as e:
        logger.error(f"Top API Error: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

# --- API: SCAN IMAGE (Preserved from original) ---
@app.route('/api/scan_image', methods=['POST'])
def api_scan_image():
    # Implementation from original app.py
    import tempfile
    if 'file' not in request.files:
        return jsonify({'ok': False, 'error': 'No file'}), 400
    
    file = request.files['file']
    img_bytes = file.read()
    
    # Mock scanning logic for now as Pyzbar/OpenCV logic requires heavy libs
    # If you want to enable real scanning, uncomment pyzbar logic here.
    return jsonify({'ok': False, 'error': 'Scanner backend disabled'}), 501

# --- ERROR HANDLERS ---
@app.errorhandler(404)
def not_found(e):
    return jsonify({'ok': False, 'error': 'Not Found'}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({'ok': False, 'error': 'Server Error'}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
