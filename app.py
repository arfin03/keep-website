# app.py
import os
import json
import traceback
from datetime import datetime
from typing import Any

from flask import Flask, request, jsonify, render_template, Response

# defensive imports
try:
    from pymongo import MongoClient
except Exception:
    MongoClient = None

try:
    from bson import ObjectId
except Exception:
    ObjectId = None

try:
    import redis
except Exception:
    redis = None

# safe int parser
def safe_int(v, default):
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default

# defaults (env override)
DEFAULT_AVATAR = os.getenv('DEFAULT_AVATAR', 'https://picsum.photos/200')
DEFAULT_NAME = os.getenv('DEFAULT_NAME', 'Traveler')

# flask app early
app = Flask(__name__, static_folder='static', template_folder='templates')

# capture init error
_init_error = None

try:
    # ---------- config ----------
    MONGO_URI = os.getenv('MONGO_URI', "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority")
    MARKET_DB_URL = os.getenv('MARKET_DB_URL', MONGO_URI)
    MONGO_URL_WAIFU = os.getenv('MONGO_URL_WAIFU', MONGO_URI)
    MONGO_URL_HUSBAND = os.getenv('MONGO_URL_HUSBAND', MONGO_URI)

    REDIS_HOST = os.getenv('REDIS_HOST', 'redis-13380.c81.us-east-1-2.ec2.cloud.redislabs.com')
    REDIS_PORT = safe_int(os.getenv('REDIS_PORT'), 13380)
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', "NRwYNwxwAjbyFxHDod1esj2hwsxugTiw")

    # ---------- helpers to init clients ----------
    def safe_mongo(uri: str):
        if MongoClient is None or not uri:
            return None
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            try:
                client.admin.command('ping')
            except Exception:
                pass
            return client
        except Exception:
            return None

    def safe_redis(host: str, port: int, password: str):
        if redis is None or not host:
            return None
        try:
            r = redis.Redis(host=host, port=port, password=password, decode_responses=True,
                            socket_connect_timeout=5, socket_timeout=5)
            try:
                r.ping()
            except Exception:
                pass
            return r
        except Exception:
            return None

    # ---------- init clients ----------
    market_client = safe_mongo(MARKET_DB_URL)
    waifu_client = safe_mongo(MONGO_URL_WAIFU)
    husband_client = safe_mongo(MONGO_URL_HUSBAND)

    if waifu_client is None:
        waifu_client = market_client
    if husband_client is None:
        husband_client = market_client

    r = safe_redis(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def get_collection(client, dbname, collname):
        try:
            if client is None:
                return None
            return client[dbname][collname]
        except Exception:
            return None

    # ---------- collections (FIXED: avoid 'or' with Collection objects) ----------
    waifu_users_coll = get_collection(waifu_client, 'Character_catcher', 'user_collection_lmaoooo')
    if waifu_users_coll is None:
        waifu_users_coll = get_collection(waifu_client, 'Character_catcher', 'user_collection_waifu')

    husband_users_coll = get_collection(husband_client, 'Character_catcher', 'user_collection_lmaoooo')
    if husband_users_coll is None:
        husband_users_coll = get_collection(husband_client, 'Character_catcher', 'user_collection_husband')

    registered_users = None
    global_user_profiles_coll = None
    top_global_coll = None  # collection to store cache

    if market_client is not None:
        try:
            registered_users = market_client['Character_catcher']['registered_users']
        except Exception:
            registered_users = None
        try:
            global_user_profiles_coll = market_client['Character_catcher']['user_collection_lmoooo']
        except Exception:
            try:
                global_user_profiles_coll = market_client['Character_catcher']['user_collection_lmaoooo']
            except Exception:
                global_user_profiles_coll = None

        # create/get top_global_db collection
        try:
            top_global_coll = market_client['Character_catcher']['top_global_db']
        except Exception:
            top_global_coll = None

    # ---------- helpers ----------
    def serialize_mongo(obj: Any):
        if isinstance(obj, list):
            return [serialize_mongo(i) for i in obj]
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                try:
                    if ObjectId is not None and isinstance(v, ObjectId):
                        out[k] = str(v)
                    else:
                        out[k] = serialize_mongo(v)
                except Exception:
                    try:
                        out[k] = str(v)
                    except Exception:
                        out[k] = None
            return out
        return obj

    def _try_many_fields_for_avatar(doc: Any):
        if not isinstance(doc, dict):
            return None
        avatar = None
        try:
            avatar = (doc.get('photo_url') or doc.get('photo') or doc.get('avatar') or doc.get('avatar_url') or doc.get('picture') or doc.get('image') or doc.get('photo_url_thumb'))
        except Exception:
            avatar = None
        try:
            profile = doc.get('profile') or doc.get('profile_info') or None
        except Exception:
            profile = None
        if isinstance(profile, dict):
            avatar = avatar or profile.get('avatar') or profile.get('photo') or profile.get('picture') or profile.get('image')
        if isinstance(avatar, str) and avatar.strip():
            return avatar
        return None

    def get_charms(uid: str) -> int:
        try:
            if r is None:
                return 0
            return int(r.hget(f"user:{uid}", "charm") or 0)
        except Exception:
            return 0

    # --- REPLACE or ADD these snippets in app.py ---

    # 1) enhanced upsert logging (replace existing upsert_top_global)
    def upsert_top_global(uid: str, firstname: str = None, username: str = None, avatar: str = None):
        """
        Ensure top_global_db and Redis hold latest profile/charms for uid.
        Also log to stdout so Heroku logs show what's happening.
        """
        uid_s = str(uid)
        charms = 0
        try:
            charms = get_charms(uid_s)
        except Exception:
            charms = 0
        now = datetime.utcnow()

        # write to redis hash if available
        if r is not None:
            try:
                mapping = {}
                if avatar is not None:
                    mapping['avatar'] = avatar
                if username is not None:
                    mapping['username'] = username
                if firstname is not None:
                    mapping['firstname'] = firstname
                mapping['charm'] = str(charms)
                if mapping:
                    # hset with mapping if redis-py supports it; fallback to hset per key
                    try:
                        r.hset(f"user:{uid_s}", mapping=mapping)
                    except Exception:
                        for k, v in mapping.items():
                            r.hset(f"user:{uid_s}", k, v)
                r.zadd('leaderboard:charms', {str(uid_s): charms})
            except Exception as ex:
                print(f"[upsert_top_global][redis_error] uid={uid_s} err={ex}", flush=True)

        # write to mongo top_global_db
        if top_global_coll is not None:
            try:
                doc = {
                    'user_id': uid_s,
                    'username': username,
                    'firstname': firstname,
                    'avatar': avatar,
                    'charms': int(charms),
                    'updated_at': now
                }
                top_global_coll.update_one({'user_id': uid_s}, {'$set': doc}, upsert=True)
                print(f"[upsert_top_global][mongo_upsert] uid={uid_s} username={username} firstname={firstname} charms={charms} avatar={avatar}", flush=True)
            except Exception as ex:
                print(f"[upsert_top_global][mongo_error] uid={uid_s} err={ex}", flush=True)
        else:
            print(f"[upsert_top_global][mongo_missing] uid={uid_s} charms={charms}", flush=True)

    # 2) debug endpoint to inspect status of top_global_db, registered_users, and redis
    @app.route('/api/debug_top_status')
    def api_debug_top_status():
        info = {
            "top_global_coll_exists": top_global_coll is not None,
            "registered_users_exists": registered_users is not None,
            "global_user_profiles_coll_exists": global_user_profiles_coll is not None,
            "waifu_users_coll_exists": waifu_users_coll is not None,
            "husband_users_coll_exists": husband_users_coll is not None,
            "redis_available": r is not None,
        }
        try:
            if top_global_coll is not None:
                info['top_global_count'] = int(top_global_coll.count_documents({}))
                info['top_global_sample'] = list(top_global_coll.find({}, {"_id": 0}).limit(5))
            else:
                info['top_global_count'] = 0
                info['top_global_sample'] = []
        except Exception as ex:
            info['top_global_error'] = str(ex)

        try:
            if registered_users is not None:
                info['registered_users_count'] = int(registered_users.count_documents({}))
            else:
                info['registered_users_count'] = 0
        except Exception as ex:
            info['registered_users_error'] = str(ex)

        try:
            if r is not None:
                info['redis_zcard_leaderboard'] = r.zcard('leaderboard:charms')
            else:
                info['redis_zcard_leaderboard'] = 0
        except Exception as ex:
            info['redis_error'] = str(ex)

        return jsonify({"ok": True, "info": info})

        def update_charms(uid: str, amt: int, typ: str = None) -> bool:
            """
            Increments charms both in redis and top_global_db.
            """
            try:
                if r is None:
                    # no redis: try to update mongo top_global only (if exists)
                    if top_global_coll is not None:
                        try:
                            # increment charms in mongo doc
                            res = top_global_coll.find_one_and_update({'user_id': str(uid)}, {'$inc': {'charms': int(amt)}, '$set': {'updated_at': datetime.utcnow()}}, upsert=True)
                            return True
                        except Exception:
                            return False
                    return False

                # increment in redis
                r.hincrby(f"user:{uid}", "charm", amt)
                current = get_charms(uid)
                # update leaderboards
                r.zadd('leaderboard:charms', {str(uid): current})
                if typ and str(typ).lower() in ('waifu', 'husband'):
                    r.zadd(f'leaderboard:charms:{typ}', {str(uid): current})
                try:
                    r.publish('charms_updates', json.dumps({"user_id": str(uid), "charms": current, "type": typ}))
                except Exception:
                    pass

                # update top_global_db
                if top_global_coll is not None:
                    try:
                        # set charms to current (upsert doc if missing)
                        top_global_coll.update_one({'user_id': str(uid)}, {'$set': {'charms': int(current), 'updated_at': datetime.utcnow()}}, upsert=True)
                    except Exception:
                        pass

                return True
            except Exception:
                return False

    except Exception:
        _init_error = traceback.format_exc()
        # minimal safe fallbacks (to avoid NameError in routes)
        market_client = waifu_client = husband_client = None
        registered_users = global_user_profiles_coll = top_global_coll = None
        r = None
        def serialize_mongo(x): return x
        def get_charms(uid): return 0
        def update_charms(uid, amt, typ=None): return False
        def ensure_user_profile(uid, first_name=None, username=None, avatar=None): return {'user_id': str(uid), 'firstname': first_name or DEFAULT_NAME, 'username': username, 'photo_url': avatar or DEFAULT_AVATAR}
        def upsert_top_global(uid, firstname=None, username=None, avatar=None): return None
        def build_top_from_users_coll(users_coll, limit=100): return []

# ================= ROUTES =================
@app.route('/')
def index():
    if _init_error:
        return ("<h3>App started but init failed</h3>"
                "<p>Check <a href='/__init_error'>/__init_error</a> for details.</p>"), 500
    return render_template('index.html')

@app.route('/__init_error')
def show_init_error():
    if _init_error:
        return Response(_init_error, mimetype='text/plain'), 500
    return jsonify({"ok": True, "msg": "no init error"}), 200

# debug status
@app.route('/api/debug_top_status')
def api_debug_top_status():
    info = {
        "top_global_coll_exists": top_global_coll is not None,
        "registered_users_exists": registered_users is not None,
        "global_user_profiles_coll_exists": global_user_profiles_coll is not None,
        "waifu_users_coll_exists": waifu_users_coll is not None,
        "husband_users_coll_exists": husband_users_coll is not None,
        "redis_available": r is not None,
    }
    try:
        if top_global_coll is not None:
            info['top_global_count'] = int(top_global_coll.count_documents({}))
            sample = []
            for d in list(top_global_coll.find({}, {"_id": 0}).limit(5)):
                dd = dict(d)
                if 'updated_at' in dd and isinstance(dd['updated_at'], datetime):
                    dd['updated_at'] = dd['updated_at'].strftime("%a, %d %b %Y %H:%M:%S GMT")
                sample.append(dd)
            info['top_global_sample'] = sample
        else:
            info['top_global_count'] = 0
            info['top_global_sample'] = []
    except Exception as ex:
        info['top_global_error'] = str(ex)
    try:
        if registered_users is not None:
            info['registered_users_count'] = int(registered_users.count_documents({}))
        else:
            info['registered_users_count'] = 0
    except Exception as ex:
        info['registered_users_error'] = str(ex)
    try:
        if r is not None:
            info['redis_zcard_leaderboard'] = r.zcard('leaderboard:charms')
        else:
            info['redis_zcard_leaderboard'] = 0
    except Exception as ex:
        info['redis_error'] = str(ex)
    return jsonify({"ok": True, "info": info})

@app.route('/api/inspect_user')
def api_inspect_user():
    uid = request.args.get('user_id') or request.args.get('uid')
    if not uid:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    uid_s = str(uid)
    out = {'ok': True, 'user_id': uid_s, 'sources': {}}
    try:
        if registered_users is not None:
            out['sources']['registered_users'] = _find_doc_in_coll_variants(registered_users, uid_s)
    except Exception as ex:
        out['sources']['registered_users_error'] = str(ex)
    try:
        if global_user_profiles_coll is not None:
            out['sources']['global_user_profiles_coll'] = _find_doc_in_coll_variants(global_user_profiles_coll, uid_s)
    except Exception as ex:
        out['sources']['global_user_profiles_coll_error'] = str(ex)
    try:
        if waifu_users_coll is not None:
            out['sources']['waifu_users_coll'] = _find_doc_in_coll_variants(waifu_users_coll, uid_s)
    except Exception as ex:
        out['sources']['waifu_users_coll_error'] = str(ex)
    try:
        if husband_users_coll is not None:
            out['sources']['husband_users_coll'] = _find_doc_in_coll_variants(husband_users_coll, uid_s)
    except Exception as ex:
        out['sources']['husband_users_coll_error'] = str(ex)
    try:
        if top_global_coll is not None:
            out['sources']['top_global_coll'] = top_global_coll.find_one({'user_id': uid_s})
    except Exception as ex:
        out['sources']['top_global_coll_error'] = str(ex)
    try:
        if r is not None:
            out['redis_hash'] = r.hgetall(f"user:{uid_s}") or {}
            try:
                out['redis_score'] = r.zscore('leaderboard:charms', uid_s)
            except Exception:
                out['redis_score'] = None
    except Exception as ex:
        out['redis_error'] = str(ex)
    return jsonify(out)

@app.route('/api/user_info', methods=['GET', 'POST'])
def api_user_info():
    if request.method == 'POST':
        data = request.get_json(silent=True) or request.form.to_dict()
    else:
        data = request.args.to_dict()
    uid = data.get('user_id') or data.get('id') or data.get('uid')
    firstname = data.get('firstname') or data.get('first_name') or None
    username = data.get('username') or data.get('user_name') or None
    avatar = data.get('avatar') or data.get('photo_url') or data.get('avatar_url') or None
    if uid is None:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    doc = ensure_user_profile(uid, first_name=firstname, username=username, avatar=avatar)
    uid_s = str(uid)
    charms = get_charms(uid_s)
    # final avatar: prefer provided param, then redis/registered doc, else None (frontend may fallback)
    avatar_final = avatar
    try:
        if not avatar_final and r is not None:
            h = r.hgetall(f"user:{uid_s}") or {}
            avatar_final = h.get('avatar') or h.get('photo_url') or avatar_final
    except Exception:
        pass
    if not avatar_final and isinstance(doc, dict):
        avatar_final = doc.get('avatar') or doc.get('photo_url') or None
    # do not force DEFAULT_AVATAR here (so frontend can show placeholder or handle)
    return jsonify({
        "ok": True,
        "id": uid_s,
        "name": (firstname or (doc.get('firstname') if isinstance(doc, dict) else DEFAULT_NAME) or DEFAULT_NAME),
        "username": (username or (doc.get('username') if isinstance(doc, dict) else None)),
        "avatar": avatar_final,
        "balance": charms
    })

# ---------- My Collection: REQUIRE character image (img_url) ----------
@app.route('/api/my_collection')
def api_my_collection():
    uid = request.args.get('user_id')
    db_type = request.args.get('type', 'waifu')
    users_coll = husband_users_coll if db_type == 'husband' else waifu_users_coll
    try:
        if not uid:
            return jsonify({"ok": False, "error": "missing user_id"}), 400
        user_doc = None
        if users_coll is not None:
            try:
                user_doc = users_coll.find_one({'id': str(uid)}) or users_coll.find_one({'id': int(uid)})
            except Exception:
                try:
                    user_doc = users_coll.find_one({'user_id': str(uid)}) or users_coll.find_one({'user_id': int(uid)})
                except Exception:
                    user_doc = None
        if not user_doc:
            return jsonify({"ok": True, "items": []})
        # possible arrays that store owned characters
        raw_items = user_doc.get('characters') or user_doc.get('waifu') or user_doc.get('husband') or user_doc.get('char') or []
        # normalize and filter: character MUST have image url in any known field
        items = []
        for c in (raw_items if isinstance(raw_items, list) else []):
            try:
                # c may be a dict or scalar id. we only support dicts containing image fields.
                if not isinstance(c, dict):
                    continue
                img = (c.get('img_url') or c.get('image') or c.get('avatar') or c.get('photo') or c.get('picture') or c.get('image_url') or c.get('thumbnail'))
                if not img or not isinstance(img, str) or not img.strip():
                    # skip item without image (as per requirement)
                    continue
                # build normalized character
                item = {
                    "id": str(c.get('id') or c.get('_id') or c.get('char_id') or ''),
                    "name": c.get('name') or c.get('title') or c.get('character_name') or 'Unknown',
                    "rarity": c.get('rarity') or c.get('rank') or None,
                    "img_url": img
                }
                items.append(item)
            except Exception:
                continue
        return jsonify({"ok": True, "items": items})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "items": [], "error": str(e)}), 500

# ---------- Top endpoint uses profile avatar (not character image) ----------
@app.route('/api/top')
def api_top():
    try:
        limit = safe_int(request.args.get('limit'), 100)
        if limit <= 0 or limit > 100:
            limit = 100
        typ = (request.args.get('type') or '').lower().strip()

        # prefer top_global when no type requested
        if top_global_coll is not None and not typ:
            try:
                cursor = top_global_coll.find({}, {"_id": 0}).sort("charms", -1).limit(limit)
                items = []
                rank = 1
                for doc in cursor:
                    uid = str(doc.get('user_id'))
                    name = doc.get('firstname') or DEFAULT_NAME
                    username = doc.get('username') or None
                    # if top_global has avatar use it, but prefer redis/registered for freshest
                    avatar = doc.get('avatar') or None
                    # prefer redis value if present and non-default
                    try:
                        if r is not None:
                            h = r.hgetall(f"user:{uid}") or {}
                            a = h.get('avatar') or h.get('photo_url')
                            if a:
                                avatar = a
                    except Exception:
                        pass
                    # try registered_users if still missing
                    if (not avatar or avatar == DEFAULT_AVATAR) and registered_users is not None:
                        try:
                            ru = registered_users.find_one({'user_id': uid})
                            if ru:
                                avatar = _try_many_fields_for_avatar(ru) or avatar
                                name = ru.get('firstname') or name
                                username = username or ru.get('username')
                        except Exception:
                            pass
                    if not avatar:
                        avatar = None  # explicitly allow null (frontend should handle)
                    items.append({
                        "rank": rank,
                        "user_id": uid,
                        "name": name,
                        "username": username,
                        "avatar": avatar,
                        "charms": int(doc.get('charms') or 0),
                        "score": int(doc.get('charms') or 0),
                        "count": int(doc.get('charms') or 0)
                    })
                    rank += 1
                return jsonify({"ok": True, "items": items})
            except Exception:
                pass

        # else build from redis or coll aggregation (type-specific)
        redis_key = 'leaderboard:charms'
        users_coll = None
        if typ == 'waifu':
            redis_key = 'leaderboard:charms:waifu'
            users_coll = waifu_users_coll
        elif typ == 'husband':
            redis_key = 'leaderboard:charms:husband'
            users_coll = husband_users_coll

        raw = []
        try:
            if r is not None:
                raw = r.zrevrange(redis_key, 0, limit - 1, withscores=True)
        except Exception:
            raw = []

        if not raw and typ in ('waifu', 'husband'):
            if users_coll is not None:
                try:
                    agg = build_top_from_users_coll(users_coll, limit=limit)
                    raw = []
                    for d in agg:
                        uid = d.get('user_id') or d.get('id') or d.get('_id')
                        if not uid:
                            continue
                        raw.append((str(uid), int(d.get('character_count') or 0)))
                except Exception:
                    raw = []

        if not raw and typ == '':
            try:
                if r is not None:
                    raw = r.zrevrange('leaderboard:charms', 0, limit - 1, withscores=True)
            except Exception:
                raw = []

        if not raw:
            raw = []
            if registered_users is not None:
                try:
                    for u in registered_users.find({}, {'user_id': 1}):
                        uid = u.get('user_id')
                        if not uid:
                            continue
                        c = get_charms(uid)
                        if c > 0:
                            raw.append((str(uid), int(c)))
                    raw.sort(key=lambda x: -x[1])
                    raw = raw[:limit]
                except Exception:
                    raw = []

        items = []
        rank = 1
        for member, score in raw:
            uid = str(member)
            name = DEFAULT_NAME
            username = None
            avatar = None

            # 1) prefer redis profile hash
            try:
                if r is not None:
                    h = r.hgetall(f"user:{uid}") or {}
                    if h.get('firstname'):
                        name = h.get('firstname')
                    if h.get('username'):
                        username = username or h.get('username')
                    a = h.get('avatar') or h.get('photo_url')
                    if a and a != DEFAULT_AVATAR:
                        avatar = a
            except Exception:
                pass

            # 2) ensure_user_profile will persist and return best-known profile
            try:
                if (not avatar) or (not name or name == DEFAULT_NAME):
                    prof = ensure_user_profile(uid)
                    if isinstance(prof, dict):
                        if prof.get('firstname'):
                            name = prof.get('firstname')
                        if prof.get('username'):
                            username = username or prof.get('username')
                        a = prof.get('avatar') or prof.get('photo_url')
                        if a and a != DEFAULT_AVATAR:
                            avatar = a
            except Exception:
                pass

            # 3) top_global doc fallback
            if (not avatar) and top_global_coll is not None:
                try:
                    tg = top_global_coll.find_one({'user_id': uid})
                    if tg:
                        avatar = tg.get('avatar') or avatar
                        name = tg.get('firstname') or name
                        username = username or tg.get('username')
                except Exception:
                    pass

            # 4) last try: collections
            if (not avatar) and waifu_users_coll is not None:
                try:
                    d = _find_doc_in_coll_variants(waifu_users_coll, uid)
                    if d:
                        avatar = _try_many_fields_for_avatar(d) or avatar
                        name = d.get('first_name') or d.get('firstname') or name
                        username = username or d.get('username')
                except Exception:
                    pass
            if (not avatar) and husband_users_coll is not None:
                try:
                    d = _find_doc_in_coll_variants(husband_users_coll, uid)
                    if d:
                        avatar = _try_many_fields_for_avatar(d) or avatar
                        name = d.get('first_name') or d.get('firstname') or name
                        username = username or d.get('username')
                except Exception:
                    pass

            # final: if avatar still None set None (frontend should use placeholder)
            avatar = avatar or None

            items.append({
                "rank": rank,
                "user_id": uid,
                "name": name,
                "username": username,
                "avatar": avatar,
                "charms": int(score),
                "score": int(score),
                "count": int(score)
            })
            rank += 1

        return jsonify({"ok": True, "items": items})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e), "items": []}), 500

@app.route('/api/rebuild_top_global')
def api_rebuild_top_global():
    if top_global_coll is None:
        return jsonify({"ok": False, "error": "top_global_db collection not available"}), 400
    try:
        limit = safe_int(request.args.get('limit'), 10000)
        count = 0
        if registered_users is not None:
            for u in registered_users.find({}, {'user_id': 1, 'firstname': 1, 'photo_url': 1, 'avatar': 1, 'username': 1}):
                uid = u.get('user_id')
                if not uid:
                    continue
                firstname = u.get('firstname') or DEFAULT_NAME
                avatar = _try_many_fields_for_avatar(u) or u.get('photo_url') or u.get('avatar') or None
                username = u.get('username') or None
                charms = get_charms(uid)
                top_global_coll.update_one({'user_id': str(uid)}, {'$set': {'user_id': str(uid), 'firstname': firstname, 'avatar': avatar, 'username': username, 'charms': int(charms), 'updated_at': datetime.utcnow()}}, upsert=True)
                # also write to redis
                try:
                    if r is not None:
                        mapping = {'firstname': firstname, 'charm': str(charms)}
                        if username:
                            mapping['username'] = username
                        if avatar:
                            mapping['avatar'] = avatar; mapping['photo_url'] = avatar
                        try:
                            r.hset(f"user:{uid}", mapping=mapping)
                        except Exception:
                            for k, v in mapping.items():
                                try:
                                    r.hset(f"user:{uid}", k, v)
                                except Exception:
                                    pass
                except Exception:
                    pass
                count += 1
                if count >= limit:
                    break
            return jsonify({"ok": True, "count": count})
        if r is not None:
            pairs = r.zrevrange('leaderboard:charms', 0, limit - 1, withscores=True)
            for uid, score in pairs:
                firstname = DEFAULT_NAME
                avatar = None
                username = None
                try:
                    if global_user_profiles_coll is not None:
                        d = _find_doc_in_coll_variants(global_user_profiles_coll, str(uid))
                        if d:
                            firstname = d.get('firstname') or d.get('first_name') or firstname
                            username = username or d.get('username')
                            avatar = _try_many_fields_for_avatar(d) or avatar
                    if firstname == DEFAULT_NAME and waifu_users_coll is not None:
                        d2 = _find_doc_in_coll_variants(waifu_users_coll, str(uid))
                        if d2:
                            firstname = d2.get('first_name') or d2.get('firstname') or firstname
                            username = username or d2.get('username')
                            avatar = avatar or _try_many_fields_for_avatar(d2)
                    if (not avatar) and husband_users_coll is not None:
                        d3 = _find_doc_in_coll_variants(husband_users_coll, str(uid))
                        if d3:
                            firstname = firstname or d3.get('first_name') or d3.get('firstname')
                            username = username or d3.get('username')
                            avatar = avatar or _try_many_fields_for_avatar(d3)
                except Exception:
                    pass
                top_global_coll.update_one({'user_id': str(uid)}, {'$set': {'user_id': str(uid), 'firstname': firstname, 'username': username, 'avatar': avatar, 'charms': int(score), 'updated_at': datetime.utcnow()}}, upsert=True)
            return jsonify({"ok": True, "count": len(pairs)})
        return jsonify({"ok": False, "error": "no source to rebuild from"}), 400
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

# SSE stream unchanged
@app.route('/stream/charms')
def stream_charms():
    def event_stream():
        if r is None:
            while True:
                yield "data: {}\n\n"
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe('charms_updates')
        try:
            for message in pubsub.listen():
                if message is None:
                    continue
                if message.get('type') != 'message':
                    continue
                data = message.get('data')
                yield f"data: {data}\n\n"
        except GeneratorExit:
            try:
                pubsub.close()
            except Exception:
                pass
    return Response(event_stream(), mimetype='text/event-stream')

if __name__ == "__main__":
    port = safe_int(os.getenv('PORT'), 5000)
    app.run(host="0.0.0.0", port=port, threaded=True)
