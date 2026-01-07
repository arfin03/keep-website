# app.py (full replacement - cleaned, fixed indentation & avatar handling)
import os
import json
import traceback
import re
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
# Do NOT force picsum here — frontend should show local placeholder if needed
DEFAULT_AVATAR = None
DEFAULT_NAME = os.getenv('DEFAULT_NAME', 'Traveler')

app = Flask(__name__, static_folder='static', template_folder='templates')
_init_error = None

try:
    # ---------- config ----------
    MONGO_URI = os.getenv('MONGO_URI', "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority")
    MARKET_DB_URL = os.getenv('MARKET_DB_URL', MONGO_URI)
    MONGO_URL_WAIFU = os.getenv('MONGO_URL_WAIFU', MONGO_URI)
    MONGO_URL_HUSBAND = os.getenv('MONGO_URL_HUSBAND', MONGO_URI)

    # Redis env handling
    REDIS_HOST = os.getenv('REDIS_HOST', 'redis-13380.c81.us-east-1-2.ec2.cloud.redislabs.com')  
    REDIS_PORT = safe_int(os.getenv('REDIS_PORT'), 13380)  
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', "NRwYNwxwAjbyFxHDod1esj2hwsxugTiw")  

    # ---------- init helpers ----------
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
                return r
            except Exception as ex:
                print(f"[safe_redis] ping failed: {ex}", flush=True)
                try:
                    r.close()
                except Exception:
                    pass
                return None
        except Exception as ex:
            print(f"[safe_redis] init failed: {ex}", flush=True)
            return None

    market_client = safe_mongo(MARKET_DB_URL)
    waifu_client = safe_mongo(MONGO_URL_WAIFU)
    husband_client = safe_mongo(MONGO_URL_HUSBAND)

    # fallback to market_client if specific clients missing
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

    # ---------- collections ----------
    waifu_users_coll = get_collection(waifu_client, 'Character_catcher', 'user_collection_lmaoooo')
    if waifu_users_coll is None:
        waifu_users_coll = get_collection(waifu_client, 'Character_catcher', 'user_collection_waifu')

    husband_users_coll = get_collection(husband_client, 'Character_catcher', 'user_collection_lmaoooo')
    if husband_users_coll is None:
        husband_users_coll = get_collection(husband_client, 'Character_catcher', 'user_collection_husband')

    registered_users = None
    global_user_profiles_coll = None
    top_global_coll = None

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

    # avatar extraction: tries many common fields, skips picsum/static placeholders
    def _try_many_fields_for_avatar(doc: Any):
        if not isinstance(doc, dict):
            return None
        cand = None
        try:
            cand = (doc.get('photo_url') or doc.get('photo') or doc.get('avatar') or
                    doc.get('avatar_url') or doc.get('picture') or doc.get('image') or
                    doc.get('img_url') or doc.get('img') or doc.get('image_url') or None)
        except Exception:
            cand = None

        try:
            profile = doc.get('profile') or doc.get('profile_info') or None
        except Exception:
            profile = None
        if isinstance(profile, dict) and not cand:
            cand = (profile.get('avatar') or profile.get('photo') or profile.get('picture') or profile.get('image') or None)

        # telegram-specific: some DBs store file id or userpic token
        try:
            tgfile = doc.get('telegram_photo') or doc.get('tg_photo') or doc.get('userpic') or doc.get('photo_file_id')
            if tgfile and isinstance(tgfile, str) and tgfile.strip():
                if tgfile.startswith('http'):
                    cand = cand or tgfile
                else:
                    cand = cand or f"https://t.me/i/userpic/320/{tgfile}"
        except Exception:
            pass

        # if list, pick first valid
        if isinstance(cand, list):
            for el in cand:
                if isinstance(el, str) and el.strip().startswith('http'):
                    if 'picsum.photos' in el or el.startswith('/static/'):
                        continue
                    return el.strip()
            return None

        if isinstance(cand, str):
            m = re.search(r'(https?://[^\s,;\'"]+)', cand)
            if m:
                url = m.group(1).strip()
                if 'picsum.photos' in url:
                    return None
                if url.startswith('/static/') and 'userpic' not in url:
                    return None
                return url
        return None

    # pick first valid http url from value (skip picsum)
    def _pick_first_valid_image(value):
        if isinstance(value, list):
            for el in value:
                if isinstance(el, str) and el.strip().startswith('http'):
                    if 'picsum.photos' in el:
                        continue
                    return el.strip()
            return None
        if isinstance(value, str):
            m = re.search(r'(https?://[^\s,;\'"]+)', value)
            if m:
                url = m.group(1).strip()
                if 'picsum.photos' in url:
                    return None
                return url
        return None

    # ---------- Charms helpers ----------
    def get_charms(uid: str) -> int:
        try:
            if r is not None:
                try:
                    h = r.hgetall(f"user:{uid}") or {}
                    v = h.get('charm') or h.get('charms') or h.get('balance') or None
                    if v is not None:
                        try:
                            return int(float(v))
                        except Exception:
                            pass
                    s = r.zscore('leaderboard:charms', str(uid))
                    if s is not None:
                        return int(float(s))
                except Exception as ex:
                    print(f"[get_charms][redis_error] {ex}", flush=True)
            if top_global_coll is not None:
                try:
                    doc = top_global_coll.find_one({'user_id': str(uid)})
                    if doc and 'charms' in doc:
                        return int(doc.get('charms') or 0)
                except Exception as ex:
                    print(f"[get_charms][mongo_error] {ex}", flush=True)
            return 0
        except Exception:
            return 0

    def update_charms(uid: str, amt: int, typ: str = None) -> bool:
        try:
            if r is not None:
                try:
                    r.hincrby(f"user:{uid}", "charm", int(amt))
                except Exception:
                    try:
                        cur = int(r.hget(f"user:{uid}", "charm") or 0) + int(amt)
                        r.hset(f"user:{uid}", "charm", str(cur))
                    except Exception:
                        pass
                try:
                    cur2 = int(r.hget(f"user:{uid}", "charm") or 0)
                    r.hset(f"user:{uid}", "charms", str(cur2))
                except Exception:
                    pass
                try:
                    current = get_charms(uid)
                    r.zadd('leaderboard:charms', {str(uid): int(current)})
                    if typ and str(typ).lower() in ('waifu', 'husband'):
                        r.zadd(f'leaderboard:charms:{typ}', {str(uid): int(current)})
                except Exception:
                    pass
                try:
                    r.publish('charms_updates', json.dumps({"user_id": str(uid), "charms": get_charms(uid), "type": typ}))
                except Exception:
                    pass
            else:
                if top_global_coll is not None:
                    try:
                        top_global_coll.update_one({'user_id': str(uid)}, {'$inc': {'charms': int(amt)}, '$set': {'updated_at': datetime.utcnow()}}, upsert=True)
                    except Exception:
                        pass
            if top_global_coll is not None:
                try:
                    top_global_coll.update_one({'user_id': str(uid)}, {'$set': {'charms': int(get_charms(uid)), 'updated_at': datetime.utcnow()}}, upsert=True)
                except Exception:
                    pass
            return True
        except Exception as ex:
            print(f"[update_charms] err={ex}", flush=True)
            return False

    # ---------- doc find & normalize ----------
    def _find_doc_in_coll_variants(coll, uid_s):
        if coll is None:
            return None
        try:
            doc = coll.find_one({'user_id': uid_s})
            if doc:
                return doc
            doc = coll.find_one({'id': uid_s})
            if doc:
                return doc
            if uid_s.isdigit():
                try:
                    doc = coll.find_one({'id': int(uid_s)}) or coll.find_one({'user_id': int(uid_s)})
                    if doc:
                        return doc
                except Exception:
                    pass
            try:
                doc = coll.find_one({'_id': uid_s})
                if doc:
                    return doc
            except Exception:
                pass
        except Exception:
            return None
        return None

    def _normalize_profile_doc(raw):
        if not isinstance(raw, dict):
            return None
        out = {}
        out['user_id'] = str(raw.get('user_id') or raw.get('id') or raw.get('_id') or '')
        out['firstname'] = raw.get('firstname') or raw.get('first_name') or raw.get('name') or raw.get('displayName') or None
        out['username'] = raw.get('username') or raw.get('user_name') or raw.get('handle') or None
        avatar = None
        try:
            avatar = raw.get('avatar') or raw.get('photo_url') or raw.get('photo') or raw.get('picture') or raw.get('image') or raw.get('img_url') or None
        except Exception:
            avatar = None
        try:
            profile = raw.get('profile') or raw.get('profile_info') or None
            if isinstance(profile, dict) and not avatar:
                avatar = profile.get('avatar') or profile.get('photo') or profile.get('picture') or None
        except Exception:
            pass
        out['photo_url'] = avatar
        out['avatar'] = avatar
        return out

    # ---------- upsert_top_global ----------
    def upsert_top_global(uid: str, firstname: str = None, username: str = None, avatar: str = None):
        uid_s = str(uid)
        try:
            charms = get_charms(uid_s)
        except Exception:
            charms = 0
        now = datetime.utcnow()

        try:
            if r is not None:
                h = r.hgetall(f"user:{uid_s}") or {}
                existing_av = h.get('avatar') or h.get('photo_url')
                if existing_av and 'picsum.photos' not in existing_av:
                    avatar = avatar or existing_av
                firstname = firstname or h.get('firstname') or firstname
                username = username or h.get('username') or username
        except Exception:
            pass

        chosen_avatar = None
        if avatar and isinstance(avatar, str) and 'picsum.photos' not in avatar and not avatar.startswith('/static/'):
            chosen_avatar = avatar

        if r is not None:
            try:
                mapping = {'charm': str(charms), 'charms': str(charms)}
                if firstname is not None:
                    mapping['firstname'] = firstname
                if username is not None:
                    mapping['username'] = username
                if chosen_avatar is not None:
                    mapping['avatar'] = chosen_avatar
                    mapping['photo_url'] = chosen_avatar
                try:
                    r.hset(f"user:{uid_s}", mapping=mapping)
                except Exception:
                    for k, v in mapping.items():
                        try:
                            r.hset(f"user:{uid_s}", k, v)
                        except Exception:
                            pass
                try:
                    r.zadd('leaderboard:charms', {str(uid_s): int(charms)})
                except Exception:
                    pass
            except Exception as ex:
                print(f"[upsert_top_global][redis_error] uid={uid_s} err={ex}", flush=True)

        if top_global_coll is not None:
            try:
                doc = {'user_id': uid_s, 'username': username, 'firstname': firstname, 'charms': int(charms), 'updated_at': now}
                if chosen_avatar is not None:
                    doc['avatar'] = chosen_avatar
                top_global_coll.update_one({'user_id': uid_s}, {'$set': doc}, upsert=True)
            except Exception as ex:
                print(f"[upsert_top_global][mongo_error] uid={uid_s} err={ex}", flush=True)

    # ---------- ensure_user_profile ----------
    def ensure_user_profile(uid: str, first_name: str = None, username: str = None, avatar: str = None):
        if uid is None:
            return None
        uid_s = str(uid)

        doc = None
        try:
            if registered_users is not None:
                doc = _find_doc_in_coll_variants(registered_users, uid_s)
        except Exception:
            doc = None

        def _is_bad_registered(d):
            if not isinstance(d, dict):
                return True
            fn = d.get('firstname') or d.get('first_name') or None
            ph = d.get('photo_url') or d.get('avatar') or None
            if not fn or fn == DEFAULT_NAME:
                return True
            if ph and isinstance(ph, str) and ('picsum.photos' in ph or ph.startswith('/static/')):
                return True
            return False

        if doc and not _is_bad_registered(doc):
            norm = _normalize_profile_doc(doc) or {}
            if first_name:
                norm['firstname'] = first_name
            if username:
                norm['username'] = username
            if avatar:
                norm['avatar'] = avatar
                norm['photo_url'] = avatar
            if not norm.get('firstname'):
                norm['firstname'] = DEFAULT_NAME
            if not norm.get('avatar'):
                norm['avatar'] = None
            try:
                update_doc = {'user_id': uid_s, 'firstname': norm['firstname']}
                if norm.get('photo_url'):
                    update_doc['photo_url'] = norm['photo_url']
                if norm.get('avatar'):
                    update_doc['avatar'] = norm['avatar']
                if norm.get('username'):
                    update_doc['username'] = norm['username']
                registered_users.update_one({'user_id': uid_s}, {'$set': update_doc}, upsert=True)
            except Exception:
                pass
            try:
                chosen_av = norm.get('avatar') or norm.get('photo_url')
                if r is not None and chosen_av and isinstance(chosen_av, str) and 'picsum.photos' not in chosen_av and not chosen_av.startswith('/static/'):
                    try:
                        r.hset(f"user:{uid_s}", mapping={'avatar': chosen_av, 'photo_url': chosen_av, 'firstname': norm['firstname'], 'username': norm.get('username') or ''})
                    except Exception:
                        try:
                            r.hset(f"user:{uid_s}", 'avatar', chosen_av)
                            r.hset(f"user:{uid_s}", 'photo_url', chosen_av)
                        except Exception:
                            pass
            except Exception:
                pass
            try:
                upsert_top_global(uid_s, firstname=norm['firstname'], username=norm.get('username'), avatar=norm.get('avatar'))
            except Exception:
                pass
            return norm

        candidates = []
        try:
            if global_user_profiles_coll is not None:
                d = _find_doc_in_coll_variants(global_user_profiles_coll, uid_s)
                if d:
                    candidates.append(d)
        except Exception:
            pass
        try:
            if waifu_users_coll is not None:
                d = _find_doc_in_coll_variants(waifu_users_coll, uid_s)
                if d:
                    candidates.append(d)
        except Exception:
            pass
        try:
            if husband_users_coll is not None:
                d = _find_doc_in_coll_variants(husband_users_coll, uid_s)
                if d:
                    candidates.append(d)
        except Exception:
            pass

        chosen = None
        for c in candidates:
            n = _normalize_profile_doc(c)
            if not n:
                continue
            if not chosen:
                chosen = n
                continue
            if (n.get('avatar') and 'picsum.photos' not in str(n.get('avatar'))) and (not chosen.get('avatar') or 'picsum.photos' in str(chosen.get('avatar') or '')):
                chosen = n
            if n.get('firstname') and (not chosen.get('firstname') or chosen.get('firstname') == DEFAULT_NAME):
                chosen = n

        if not chosen:
            chosen = {'user_id': uid_s, 'firstname': first_name or DEFAULT_NAME, 'username': username or None, 'photo_url': avatar or None, 'avatar': avatar or None}
        else:
            if first_name:
                chosen['firstname'] = first_name
            if username:
                chosen['username'] = username
            if avatar:
                chosen['avatar'] = avatar
                chosen['photo_url'] = avatar

        try:
            to_save = {'user_id': uid_s, 'firstname': chosen.get('firstname') or DEFAULT_NAME}
            if chosen.get('photo_url') and 'picsum.photos' not in str(chosen.get('photo_url')) and not str(chosen.get('photo_url')).startswith('/static/'):
                to_save['photo_url'] = chosen.get('photo_url')
            if chosen.get('avatar') and 'picsum.photos' not in str(chosen.get('avatar')) and not str(chosen.get('avatar')).startswith('/static/'):
                to_save['avatar'] = chosen.get('avatar')
            if chosen.get('username'):
                to_save['username'] = chosen.get('username')
            if registered_users is not None:
                registered_users.update_one({'user_id': uid_s}, {'$set': to_save}, upsert=True)
        except Exception:
            pass

        try:
            chosen_av = chosen.get('avatar') or chosen.get('photo_url')
            if r is not None and chosen_av and isinstance(chosen_av, str) and 'picsum.photos' not in chosen_av and not chosen_av.startswith('/static/'):
                try:
                    r.hset(f"user:{uid_s}", mapping={'avatar': chosen_av, 'photo_url': chosen_av, 'firstname': chosen.get('firstname') or DEFAULT_NAME, 'username': chosen.get('username') or ''})
                except Exception:
                    try:
                        r.hset(f"user:{uid_s}", 'avatar', chosen_av)
                        r.hset(f"user:{uid_s}", 'photo_url', chosen_av)
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            upsert_top_global(uid_s, firstname=chosen.get('firstname'), username=chosen.get('username'), avatar=chosen.get('avatar'))
        except Exception:
            pass

        return chosen

    # ---------- build top from users coll ----------
    def build_top_from_users_coll(users_coll, limit=100):
        if users_coll is None:
            return []
        try:
            pipeline = [
                {"$addFields": {"character_count": {"$cond": {"if": {"$isArray": "$characters"}, "then": {"$size": "$characters"}, "else": 0}}}},
                {"$project": {"user_id": {"$ifNull": ["$id", "$user_id"]}, "first_name": 1, "username": 1, "character_count": 1}},
                {"$sort": {"character_count": -1}},
                {"$limit": limit}
            ]
            cursor = users_coll.aggregate(pipeline)
            return list(cursor)
        except Exception:
            return []

except Exception:
    _init_error = traceback.format_exc()
    market_client = waifu_client = husband_client = None
    registered_users = global_user_profiles_coll = top_global_coll = None
    r = None
    def serialize_mongo(x): return x
    def get_charms(uid): return 0
    def ensure_user_profile(uid, first_name=None, username=None, avatar=None):
        return {'user_id': str(uid), 'firstname': first_name or DEFAULT_NAME, 'username': username, 'photo_url': avatar or None, 'avatar': avatar or None}
    def upsert_top_global(uid, firstname=None, username=None, avatar=None): return None
    def build_top_from_users_coll(users_coll, limit=100): return []

# ================= ROUTES =================
@app.route('/')
def index():
    if _init_error:
        return ("<h3>App started but init failed</h3><p>Check <a href='/__init_error'>/__init_error</a> for details.</p>"), 500
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

# api_user_info (GET or POST) - returns avatar if available (may be null)
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
    avatar_final = None
    try:
        if avatar and isinstance(avatar, str) and 'picsum.photos' not in avatar and not avatar.startswith('/static/'):
            avatar_final = avatar
        if not avatar_final and r is not None:
            try:
                h = r.hgetall(f"user:{uid_s}") or {}
                a = h.get('avatar') or h.get('photo_url') or h.get('photo') or None
                if a and isinstance(a, str) and 'picsum.photos' not in a and not a.startswith('/static/'):
                    avatar_final = a
            except Exception as ex:
                print(f"[api_user_info][redis_read_err] {ex}", flush=True)
        if not avatar_final and isinstance(doc, dict):
            a = doc.get('avatar') or doc.get('photo_url')
            if a and isinstance(a, str) and 'picsum.photos' not in a and not a.startswith('/static/'):
                avatar_final = a
    except Exception:
        pass

    name_final = firstname or (doc.get('firstname') if isinstance(doc, dict) else None) or DEFAULT_NAME
    username_final = username or (doc.get('username') if isinstance(doc, dict) else None)

    return jsonify({
        "ok": True,
        "id": uid_s,
        "name": name_final,
        "username": username_final,
        "avatar": avatar_final,
        "balance": get_charms(uid_s)
    })

# My Collection: strict image requirement, skip picsum
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
                user_doc = users_coll.find_one({'id': str(uid)})
                if not user_doc and uid.isdigit():
                    try:
                        user_doc = users_coll.find_one({'id': int(uid)})
                    except Exception:
                        pass
                if not user_doc:
                    try:
                        user_doc = users_coll.find_one({'user_id': str(uid)}) or users_coll.find_one({'user_id': int(uid)})
                    except Exception:
                        pass
            except Exception:
                user_doc = None

        if not user_doc:
            return jsonify({"ok": True, "items": []})

        raw_items = user_doc.get('characters') or user_doc.get('waifu') or user_doc.get('husband') or user_doc.get('char') or []
        items = []
        for c in (raw_items if isinstance(raw_items, list) else []):
            try:
                if not isinstance(c, dict):
                    continue
                candidate = (c.get('img_url') or c.get('image') or c.get('image_url') or c.get('avatar') or
                             c.get('photo') or c.get('picture') or c.get('thumbnail') or c.get('img') or None)
                img = _pick_first_valid_image(candidate)
                if not img:
                    found = None
                    for k, v in c.items():
                        if isinstance(v, (list, str)):
                            try_img = _pick_first_valid_image(v)
                            if try_img:
                                found = try_img
                                break
                    img = found
                if not img:
                    # skip items without real image
                    continue

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

# Top endpoint (profile avatar prioritized)
@app.route('/api/top')
def api_top():
    try:
        limit = safe_int(request.args.get('limit'), 100)
        if limit <= 0 or limit > 100:
            limit = 100
        typ = (request.args.get('type') or '').lower().strip()

        # prefer top_global when available and not type-specific
        if top_global_coll is not None and not typ:
            try:
                cursor = top_global_coll.find({}, {"_id": 0}).sort("charms", -1).limit(limit)
                items = []
                rank = 1
                for doc in cursor:
                    uid = str(doc.get('user_id'))
                    name = doc.get('firstname') or DEFAULT_NAME
                    username = doc.get('username') or None
                    avatar = doc.get('avatar') or None
                    try:
                        if r is not None:
                            h = r.hgetall(f"user:{uid}") or {}
                            a = h.get('avatar') or h.get('photo_url')
                            if a and 'picsum.photos' not in a:
                                avatar = a
                            if h.get('firstname'):
                                name = h.get('firstname')
                            if h.get('username'):
                                username = username or h.get('username')
                    except Exception:
                        pass
                    if (not avatar or 'picsum.photos' in str(avatar)) and registered_users is not None:
                        try:
                            ru = registered_users.find_one({'user_id': uid})
                            if ru:
                                a2 = _try_many_fields_for_avatar(ru)
                                if a2:
                                    avatar = a2
                                name = ru.get('firstname') or name
                                username = username or ru.get('username')
                        except Exception:
                            pass
                    avatar = avatar or None
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
            except Exception as ex:
                print("[api_top][top_global_read_error]", ex, flush=True)

        # else build from redis or coll aggregation
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

        if not raw and typ in ('waifu', 'husband') and users_coll is not None:
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

        if not raw and (not typ):
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

            try:
                if r is not None:
                    h = r.hgetall(f"user:{uid}") or {}
                    if h.get('firstname'):
                        name = h.get('firstname')
                    if h.get('username'):
                        username = username or h.get('username')
                    a = h.get('avatar') or h.get('photo_url')
                    if a and 'picsum.photos' not in a:
                        avatar = a
            except Exception:
                pass

            try:
                prof = ensure_user_profile(uid)
                if isinstance(prof, dict):
                    if prof.get('firstname'):
                        name = prof.get('firstname')
                    if prof.get('username'):
                        username = username or prof.get('username')
                    a = prof.get('avatar') or prof.get('photo_url')
                    if a and 'picsum.photos' not in a:
                        avatar = a
            except Exception:
                pass

            if (not avatar) and top_global_coll is not None:
                try:
                    tg = top_global_coll.find_one({'user_id': uid})
                    if tg:
                        tg_av = tg.get('avatar')
                        if tg_av and 'picsum.photos' not in tg_av:
                            avatar = tg_av
                        name = tg.get('firstname') or name
                        username = username or tg.get('username')
                except Exception:
                    pass

            if (not avatar) and waifu_users_coll is not None:
                try:
                    d = _find_doc_in_coll_variants(waifu_users_coll, uid)
                    if d:
                        a2 = _try_many_fields_for_avatar(d)
                        if a2:
                            avatar = a2
                        name = d.get('first_name') or d.get('firstname') or name
                        username = username or d.get('username')
                except Exception:
                    pass
            if (not avatar) and husband_users_coll is not None:
                try:
                    d = _find_doc_in_coll_variants(husband_users_coll, uid)
                    if d:
                        a2 = _try_many_fields_for_avatar(d)
                        if a2:
                            avatar = a2
                        name = d.get('first_name') or d.get('firstname') or name
                        username = username or d.get('username')
                except Exception:
                    pass

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
                if avatar and 'picsum.photos' in avatar:
                    avatar = None
                top_global_coll.update_one({'user_id': str(uid)}, {'$set': {'user_id': str(uid), 'firstname': firstname, 'avatar': avatar, 'username': username, 'charms': int(get_charms(uid)), 'updated_at': datetime.utcnow()}}, upsert=True)
                try:
                    if r is not None:
                        mapping = {'firstname': firstname, 'charm': str(get_charms(uid)), 'charms': str(get_charms(uid))}
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
                if avatar and 'picsum.photos' in str(avatar):
                    avatar = None
                top_global_coll.update_one({'user_id': str(uid)}, {'$set': {'user_id': str(uid), 'firstname': firstname, 'username': username, 'avatar': avatar, 'charms': int(score), 'updated_at': datetime.utcnow()}}, upsert=True)
            return jsonify({"ok": True, "count": len(pairs)})
        return jsonify({"ok": False, "error": "no source to rebuild from"}), 400
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

# SSE stream (charms updates)
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

# local runner
if __name__ == "__main__":
    port = safe_int(os.getenv('PORT'), 5000)
    app.run(host="0.0.0.0", port=port, threaded=True)
