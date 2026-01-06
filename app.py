import os
import io
import json
import logging
import datetime
import base64
from datetime import datetime

from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient
from bson import ObjectId
import redis
import qrcode
from PIL import Image as PILImage

# --- CONFIG ---
API_ID    = int(os.getenv('API_ID', 123456))
API_HASH  = os.getenv('API_HASH', "")
MONGO_URI = os.getenv('MONGO_URI', "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority")
MARKET_DB_URL = os.getenv('MARKET_DB_URL', MONGO_URI)
MONGO_URL_WAIFU = os.getenv('MONGO_URL_WAIFU', MONGO_URI)
MONGO_URL_HUSBAND = os.getenv('MONGO_URL_HUSBAND', MONGO_URI)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis-13380.c81.us-east-1-2.ec2.cloud.redislabs.com')
REDIS_PORT = int(os.getenv('REDIS_PORT', 13380))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', "NRwYNwxwAjbyFxHDod1esj2hwsxugTiw")

app = Flask(__name__, static_folder='static', template_folder='templates')

# --- DB CONNECTIONS ---
try:
    market_client = MongoClient(MARKET_DB_URL)
    market_db = market_client['market_p2p']
    user_settings_coll = market_db['user_settings'] # Untuk Theme & BGM
    
    waifu_client = MongoClient(MONGO_URL_WAIFU)
    waifu_db = waifu_client['Character_catcher']
    waifu_users_coll = waifu_db['user_collection_lmaoooo']
    
    husband_client = MongoClient(MONGO_URL_HUSBAND)
    husband_db = husband_client['Character_catcher']
    husband_users_coll = husband_db['user_collection_lmaoooo']
    
    registered_users = market_client['Character_catcher']['registered_users']
    charms_addresses = market_client['Character_catcher']['charms_addresses']
    
    print("[DB] All Connected")
except Exception as e:
    print(f"[DB] Error: {e}")
    market_db = None

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    r.ping()
    print("[Redis] Connected")
except Exception as e:
    print(f"[Redis] Error: {e}")
    r = None

# --- CONSTANTS ---
CATEGORY_MAP = {
    '🏖': '🏖𝒐𝒖𝒎𝒎𝒆𝒓 🏖', '👘': '👘𝑲𝒊𝒎𝒐𝒏𝒐👘', '🧹': '🧹𝑴𝒂𝒊𝒅🧹',
    '🐰': '🐰𝑩𝒖𝒏𝒏𝒚🐰', '🏜️': '🏜️𝑬𝒈𝒚𝒑𝒕🏜️', '🎒': '🎒𝑺𝒄𝒉𝒐𝒐𝒍🎒',
    '💞': '💞𝑽𝒂𝒍𝒆𝒏𝒕𝒊𝒏𝒆💞', '🎃': '🎃𝑯𝒂𝒍𝒍𝒐𝒘𝒆𝒆𝒏🎃', '🥻': '🥻𝑺𝒂𝒓𝒆𝒆🥻',
    '💉': '💉𝑵𝒖𝒓𝒐𝒆💉', '☃️': '☃️𝑾𝒊𝒏𝒕𝒆𝒓☃️', '🎄': '🎄𝑪𝒉𝒓𝒊𝒐𝒕𝒎𝒂𝒐🎄',
    '👥': '👥𝐃𝐮𝐨👥', '🤝': '🤝𝐆𝐫𝐨𝐮𝐩🤝', '⚽': '⚽𝑭𝒐𝒈𝒐𝒕𝒃𝒂𝒍𝒍⚽',
    '🚨': '🚨𝑷𝒐𝒍𝒊𝒄𝒆🚨', '🏀': '🏀𝑩𝒂𝒐𝒌𝒆𝒕𝒃𝒂𝒍𝒍🏀', '🎩': '🎩𝑻𝒖𝒙𝒆𝒅𝒐🎩',
    '🏮': '🏮𝑪𝒉𝒊𝒏𝒆𝒐𝒆🏮', '📙': '📙𝑴𝒂𝒏𝒉𝒘𝒂📙', '👙': '👙𝑩𝒊𝒌𝒊𝒏𝒊👙',
    '🎊': '🎊𝑪𝒉𝒆𝒆𝒓𝒍𝒆𝒂𝒅𝒆𝒓𝒐🎊', '🎮': '🎮𝑮𝒂𝒎𝒆🎮', '💍': '💍𝑴𝒂𝒓𝒓𝒊𝒆𝒅💍',
    '👶': '👶𝑪𝒉𝒊𝒃𝒊👶', '🕷': '🕷𝑺𝒑𝒊𝒅𝒆𝒓🕷', '🎗️': '🎗️𝑪𝒐𝒍𝒍𝒆𝒄𝒕𝒐𝒓🎗️',
    '🔞': '🔞𝑵𝒖𝒅𝒆𝒐🔞', '🪽': '🪽𝑯𝒆𝒂𝒗𝒆𝒏𝒍𝒚🪽', '☀️': '☀️𝑪𝒐𝒘𝒃𝒐𝒚 ☀️', '🌑': '🌑𝒏𝒖𝒏🌑'
}

# --- HELPERS ---
def get_charms(uid):
    if not r: return 0
    try: return int(r.hget(f"user:{uid}", "charm") or 0)
    except: return 0

def update_charms(uid, amt):
    if not r: return False
    try: 
        r.hincrby(f"user:{uid}", "charm", amt)
        r.zadd('leaderboard:charms', {str(uid): get_charms(uid)})
        return True
    except: return False

def log_tx(uid, t_type, amt, title, detail=""):
    if not r: return
    try:
        tx = {"type": t_type, "amount": amt, "title": title, "detail": detail, "ts": datetime.utcnow().timestamp()}
        r.lpush(f"user:{uid}:txs", json.dumps(tx))
        r.ltrim(f"user:{uid}:txs", 0, 99)
    except: pass

# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/user_info', methods=['GET'])
def api_user_info():
    uid = request.args.get('user_id')
    u_data = registered_users.find_one({'user_id': str(uid)}) or {}
    
    # Get Settings
    settings = user_settings_coll.find_one({'user_id': str(uid)}) or {}
    
    return jsonify({
        'ok': True, 'id': uid, 
        'name': u_data.get('firstname', 'Traveler'), 
        'avatar': u_data.get('photo_url', 'https://picsum.photos/seed/user/200/200'),
        'balance': get_charms(uid),
        'theme_url': settings.get('theme_url', ''),
        'bgm_url': settings.get('bgm_url', ''),
        'lang': settings.get('lang', 'en')
    })

# --- MARKET & P2P CORE ---
@app.route('/api/market', methods=['GET'])
def api_market():
    db_type = request.args.get('type', 'waifu')
    sort_by = request.args.get('sort', 'price-asc')
    rarity_filter = request.args.get('rarity', 'All')
    
    if not market_db: return jsonify({'ok': True, 'items': []})
    
    query = {'type': db_type}
    if rarity_filter != 'All':
        query['rarity'] = rarity_filter
        
    sort_map = {
        'price-asc': [('price', 1)], 'price-desc': [('price', -1)],
        'newest': [('_id', -1)], 'oldest': [('_id', 1)], 'random': []
    }
    
    coll = market_db['official_market']
    try:
        if sort_by == 'random':
            items = list(coll.aggregate([{'$match': query}, {'$sample': {'size': 20}}]))
        else:
            items = list(coll.find(query).sort(sort_map.get(sort_by, [('price',1)])).limit(20))
            
        for item in items:
            item['_id'] = str(item['_id'])
            stock_key = f"market:stock:{item['_id']}"
            stock = int(r.get(stock_key) or 30) # Default limit 30
            item['stock'] = stock
        return jsonify({'ok': True, 'items': items})
    except: return jsonify({'ok': True, 'items': []})

@app.route('/api/buy_market', methods=['POST'])
def api_buy_market():
    data = request.json
    uid = data.get('user_id'); item_id = data.get('item_id')
    
    if not market_db: return jsonify({'ok': False}), 500
    coll = market_db['official_market']
    item = coll.find_one({'_id': ObjectId(item_id)})
    
    if not item: return jsonify({'ok': False, 'error': 'Not Found'}), 404
    if get_charms(uid) < item.get('price'): return jsonify({'ok': False, 'error': 'Insufficient Charms'}), 400
    
    # Check Stock
    stock_key = f"market:stock:{item_id}"
    current_stock = int(r.get(stock_key) or 30)
    if current_stock <= 0: return jsonify({'ok': False, 'error': 'Out of Stock'}), 400
    
    # Execute Buy
    update_charms(uid, -item.get('price'))
    r.decr(stock_key)
    
    # Add Char to User
    char_data = {k:v for k,v in item.items() if k not in ['_id', 'stock', 'created_at']}
    target_coll = husband_users_coll if item.get('type')=='husband' else waifu_users_coll
    target_coll.update_one({'id': str(uid)}, {'$push': {'characters': char_data}}, upsert=True)
    
    log_tx(uid, 'buy', -item.get('price'), f"Bought {item.get('name')}")
    return jsonify({'ok': True, 'new_balance': get_charms(uid)})

@app.route('/api/my_collection', methods=['GET'])
def api_my_collection():
    uid = request.args.get('user_id'); db_type = request.args.get('type', 'waifu')
    coll = husband_users_coll if db_type == 'husband' else waifu_users_coll
    try:
        user_doc = coll.find_one({'id': str(uid)}) or coll.find_one({'id': int(uid)})
        items = user_doc.get('characters', []) if user_doc else []
        return jsonify({'ok': True, 'items': items})
    except: return jsonify({'ok': True, 'items': []})

@app.route('/api/sell_character', methods=['POST'])
def api_sell_character():
    data = request.json
    uid = data.get('user_id'); char_id = data.get('char_id')
    price = data.get('price'); qty = int(data.get('qty', 1))
    desc = data.get('description', ''); category = data.get('category', '')
    db_type = data.get('type', 'waifu')
    
    coll_user = husband_users_coll if db_type == 'husband' else waifu_users_coll
    user_doc = coll_user.find_one({'id': str(uid)})
    if not user_doc: return jsonify({'ok': False, 'error': 'User not found'}), 404
    
    chars = user_doc.get('characters', [])
    target_char = None
    new_chars = []
    removed = 0
    for c in chars:
        if removed < qty and str(c.get('id')) == str(char_id):
            target_char = c
            removed += 1
            continue
        new_chars.append(c)
        
    if not target_char: return jsonify({'ok': False, 'error': 'Char not found'}), 404
    
    coll_user.update_one({'_id': user_doc['_id']}, {'$set': {'characters': new_chars}})
    
    listing = {
        'seller_id': str(uid), 'seller_name': data.get('seller_name'),
        'char_data': target_char, 'qty': qty, 'price': price,
        'description': desc, 'type': db_type, 'category': category,
        'status': 'active', 'created_at': datetime.utcnow()
    }
    res = market_db['listings'].insert_one(listing)
    
    return jsonify({'ok': True})

@app.route('/api/p2p_listings', methods=['GET'])
def api_p2p_listings():
    if not market_db: return jsonify({'ok': True, 'items': []})
    items = list(market_db['listings'].find({'status': 'active'}).sort('_id', -1).limit(20))
    for item in items: item['_id'] = str(item['_id'])
    return jsonify({'ok': True, 'items': items})

@app.route('/api/buy_p2p', methods=['POST'])
def api_buy_p2p():
    data = request.json
    buyer_id = data.get('buyer_id'); listing_id = data.get('listing_id')
    
    if not market_db: return jsonify({'ok': False}), 500
    listing = market_db['listings'].find_one({'_id': ObjectId(listing_id)})
    
    if not listing or listing.get('status') != 'active': return jsonify({'ok': False, 'error': 'Sold'}), 400
    if str(listing.get('seller_id')) == str(buyer_id): return jsonify({'ok': False, 'error': 'Own Item'}), 400
    if get_charms(buyer_id) < listing.get('price'): return jsonify({'ok': False, 'error': 'No Charms'}), 400
    
    update_charms(buyer_id, -listing.get('price'))
    update_charms(listing.get('seller_id'), listing.get('price'))
    
    market_db['listings'].update_one({'_id': ObjectId(listing_id)}, {'$set': {'status': 'sold', 'buyer_id': buyer_id}})
    
    char_data = listing.get('char_data')
    target_coll = husband_users_coll if listing.get('type')=='husband' else waifu_users_coll
    target_coll.update_one({'id': str(buyer_id)}, {'$push': {'characters': char_data}}, upsert=True)
    
    log_tx(buyer_id, 'p2p_buy', -listing.get('price'), f"P2P: {char_data.get('name')}")
    log_tx(listing.get('seller_id'), 'p2p_sell', listing.get('price'), f"Sold {char_data.get('name')}")
    
    return jsonify({'ok': True})

@app.route('/api/history', methods=['GET'])
def api_history():
    uid = request.args.get('user_id')
    if not r: return jsonify({'ok': True, 'items': []})
    try:
        raw = r.lrange(f"user:{uid}:txs", 0, 49)
        return jsonify({'ok': True, 'items': [json.loads(r) for r in raw]})
    except: return jsonify({'ok': True, 'items': []})

@app.route('/api/top', methods=['GET'])
def api_top():
    type_ = request.args.get('type', 'charms')
    if type_ == 'charms' and r:
        tops = r.zrevrange('leaderboard:charms', 0, 9, withscores=True)
        res = []
        for uid, score in tops:
            u = registered_users.find_one({'user_id': str(uid)})
            res.append({'name': u.get('firstname', 'User') if u else 'User', 'score': int(score)})
        return jsonify({'ok': True, 'items': res})
    elif type_ in ['waifu', 'husband']:
        coll = husband_users_coll if type_ == 'husband' else waifu_users_coll
        pipeline = [{"$project": {"name": "$firstname", "count": {"$size": "$characters"}}}, {"$sort": {"count": -1}}, {"$limit": 10}]
        tops = list(coll.aggregate(pipeline))
        return jsonify({'ok': True, 'items': tops})
    return jsonify({'ok': True, 'items': []})

@app.route('/api/qr_code', methods=['GET'])
def api_qr_code():
    uid = request.args.get('user_id')
    try:
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(str(uid)); qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO(); img.save(buf, format="PNG")
        return jsonify({'ok': True, 'image_b64': base64.b64encode(buf.getvalue()).decode()})
    except: return jsonify({'ok': False}), 500

@app.route('/api/update_settings', methods=['POST'])
def api_update_settings():
    data = request.json
    uid = data.get('user_id')
    theme_b64 = data.get('theme_b64')
    bgm_b64 = data.get('bgm_b64')
    lang = data.get('lang')
    
    try:
        update_doc = {}
        if theme_b64: update_doc['theme_url'] = theme_b64
        if bgm_b64: update_doc['bgm_url'] = bgm_b64
        if lang: update_doc['lang'] = lang
        
        if update_doc:
            user_settings_coll.update_one({'user_id': str(uid)}, {'$set': update_doc}, upsert=True)
        
        return jsonify({'ok': True})
    except Exception as e:
        print(f"Settings Error: {e}")
        return jsonify({'ok': False}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
