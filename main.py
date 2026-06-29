import time
import hmac
import hashlib
import json
import socket
import os
import threading
from datetime import datetime, date
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    import requests
except ImportError:
    raise SystemExit('Run: pip install requests')

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    raise SystemExit('Run: pip install psycopg2-binary')

# ── CONFIG ───────────────────────────────────────────────────
API_KEY            = os.environ.get('BYBIT_API_KEY', 'YOUR_API_KEY_HERE')
API_SECRET         = os.environ.get('BYBIT_API_SECRET', 'YOUR_API_SECRET_HERE')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', 'YOUR_BOT_TOKEN_HERE')
TELEGRAM_CHAT_ID   = os.environ.get('TELEGRAM_CHAT_ID', '6447141249')
DATABASE_URL       = os.environ.get('DATABASE_URL', '')
BASE_URL           = 'https://api.bybit.com'
CHECK_INTERVAL     = 60
KEEPALIVE_INTERVAL = 90
DAILY_REPORT_HOUR  = 8
PORT               = int(os.environ.get('PORT', 8080))

# ── RUNTIME ──────────────────────────────────────────────────
_seen_orders      = set()
_start_time       = datetime.now()
_last_keepalive   = 0
_last_report_date = None
_last_pnl_date    = None
_db_conn          = None


# ══════════════════════════════════════════════════════════════
# HEALTH SERVER (keeps Render free tier alive)
# ══════════════════════════════════════════════════════════════
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'CypherX Bybit Monitor - OK')
    def log_message(self, format, *args):
        pass

def run_health_server():
    server = HTTPServer(('0.0.0.0', PORT), HealthHandler)
    server.serve_forever()


# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
def log(msg, level='INFO'):
    ts   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[{}] [{}] {}'.format(ts, level, msg), flush=True)


def eat():
    return datetime.now().strftime('%H:%M:%S %d/%m/%Y')


# ══════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════
def get_db():
    global _db_conn
    try:
        if _db_conn is None or _db_conn.closed:
            _db_conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        return _db_conn
    except Exception as e:
        log('DB connection error: {}'.format(e), 'ERROR')
        return None


def init_db():
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id    TEXT PRIMARY KEY,
                side        TEXT,
                amount      NUMERIC,
                price       NUMERIC,
                quantity    NUMERIC,
                fiat        TEXT,
                buyer       TEXT,
                status      TEXT,
                created_at  TIMESTAMP DEFAULT NOW()
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS pnl_daily (
                report_date   DATE PRIMARY KEY,
                order_count   INTEGER,
                total_usdt    NUMERIC,
                total_kes     NUMERIC,
                avg_price     NUMERIC,
                closing_bal   NUMERIC,
                created_at    TIMESTAMP DEFAULT NOW()
            )
        ''')
        conn.commit()
        log('Database initialized.')
    except Exception as e:
        log('DB init error: {}'.format(e), 'ERROR')
        conn.rollback()


def save_order(order):
    conn = get_db()
    if not conn:
        return
    try:
        oid    = order.get('orderId',       order.get('id',                 ''))
        side   = str(order.get('side',      order.get('tradeType',          '')))
        amount = float(order.get('amount',  order.get('orderAmount',        0)) or 0)
        price  = float(order.get('price',   0) or 0)
        qty    = float(order.get('quantity',order.get('notifyTokenQuantity',0)) or 0)
        fiat   = order.get('currencyId',    order.get('fiatCurrency',       'KES'))
        buyer  = order.get('buyerNickName', order.get('nickName',           ''))
        status = str(order.get('orderStatus', order.get('status',           '')))
        cur    = conn.cursor()
        cur.execute('''
            INSERT INTO orders (order_id, side, amount, price, quantity, fiat, buyer, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_id) DO UPDATE SET status=EXCLUDED.status
        ''', (oid, side, amount, price, qty, fiat, buyer, status))
        conn.commit()
    except Exception as e:
        log('Save order error: {}'.format(e), 'WARN')
        conn.rollback()


def get_today_pnl():
    conn = get_db()
    if not conn:
        return 0, 0.0, 0.0, 0.0
    try:
        today = date.today()
        cur   = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('''
            SELECT COUNT(*) as cnt,
                   COALESCE(SUM(quantity),0) as total_usdt,
                   COALESCE(SUM(amount),0)   as total_kes,
                   COALESCE(AVG(price),0)    as avg_price
            FROM orders
            WHERE DATE(created_at) = %s
              AND side IN ('1','Sell','SELL')
        ''', (today,))
        row = cur.fetchone()
        return (
            int(row['cnt']),
            float(row['total_usdt']),
            float(row['total_kes']),
            float(row['avg_price'])
        )
    except Exception as e:
        log('P&L fetch error: {}'.format(e), 'WARN')
        return 0, 0.0, 0.0, 0.0


def save_pnl(order_count, total_usdt, total_kes, avg_price, balance):
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO pnl_daily (report_date, order_count, total_usdt, total_kes, avg_price, closing_bal)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (report_date) DO UPDATE SET
                order_count=EXCLUDED.order_count,
                total_usdt=EXCLUDED.total_usdt,
                total_kes=EXCLUDED.total_kes,
                avg_price=EXCLUDED.avg_price,
                closing_bal=EXCLUDED.closing_bal
        ''', (date.today(), order_count, total_usdt, total_kes, avg_price, float(balance) if balance != 'N/A' else 0))
        conn.commit()
    except Exception as e:
        log('Save P&L error: {}'.format(e), 'WARN')
        conn.rollback()


# ══════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════
def send_telegram(msg):
    if TELEGRAM_BOT_TOKEN == 'YOUR_BOT_TOKEN_HERE':
        log('Telegram not configured', 'WARN')
        return
    try:
        r = requests.post(
            'https://api.telegram.org/bot{}/sendMessage'.format(TELEGRAM_BOT_TOKEN),
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'},
            timeout=10
        )
        if r.status_code == 200:
            log('Telegram sent.')
        else:
            log('Telegram failed: {}'.format(r.text[:80]), 'WARN')
    except Exception as e:
        log('Telegram error: {}'.format(str(e)[:80]), 'WARN')


# ══════════════════════════════════════════════════════════════
# NETWORK
# ══════════════════════════════════════════════════════════════
def check_internet():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect(('api.bybit.com', 443))
        s.close()
        return True
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════
# BYBIT API
# ══════════════════════════════════════════════════════════════
def get_timestamp():
    return str(int(time.time() * 1000))


def sign(payload):
    return hmac.new(
        API_SECRET.encode('utf-8'),
        payload.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()


def bybit_post(endpoint, body=None):
    if body is None:
        body = {}
    ts          = get_timestamp()
    recv_window = '20000'
    body_str    = json.dumps(body, separators=(',', ':'))
    payload     = ts + API_KEY + recv_window + body_str
    signature   = sign(payload)
    headers = {
        'X-BAPI-API-KEY':     API_KEY,
        'X-BAPI-TIMESTAMP':   ts,
        'X-BAPI-SIGN':        signature,
        'X-BAPI-RECV-WINDOW': recv_window,
        'Content-Type':       'application/json',
    }
    try:
        r = requests.post(
            BASE_URL + endpoint,
            headers=headers,
            data=body_str,
            timeout=15
        )
        if not r.text.strip():
            return None
        return r.json()
    except Exception as e:
        log('API error: {}'.format(str(e)[:80]), 'WARN')
        return None


def refresh_online_status():
    global _last_keepalive
    data = bybit_post('/v5/p2p/user/personal/info', {})
    if data is None:
        log('Keepalive: no response', 'WARN')
        return False
    if data.get('ret_code', -1) == 0:
        _last_keepalive = time.time()
        is_online = data.get('result', {}).get('isOnline', '?')
        log('Keepalive OK - isOnline: {}'.format(is_online))
        return True
    log('Keepalive failed: {}'.format(data.get('ret_msg', 'unknown')), 'WARN')
    return False


def fetch_pending_orders():
    data = bybit_post('/v5/p2p/order/pending/simplifyList', {})
    if data is None:
        return []
    if data.get('ret_code', -1) != 0:
        log('Orders API error: {}'.format(data.get('ret_msg', 'unknown')), 'WARN')
        return []
    result = data.get('result', {})
    if isinstance(result, dict):
        return result.get('items', result.get('list', []))
    return []


def fetch_balance():
    ts          = get_timestamp()
    recv_window = '20000'
    query       = 'accountType=FUND&coin=USDT'
    payload     = ts + API_KEY + recv_window + query
    signature   = sign(payload)
    headers = {
        'X-BAPI-API-KEY':     API_KEY,
        'X-BAPI-TIMESTAMP':   ts,
        'X-BAPI-SIGN':        signature,
        'X-BAPI-RECV-WINDOW': recv_window,
    }
    try:
        r    = requests.get(
            BASE_URL + '/v5/asset/transfer/query-account-coins-balance',
            headers=headers,
            params={'accountType': 'FUND', 'coin': 'USDT'},
            timeout=15
        )
        data  = r.json()
        coins = data.get('result', {}).get('balance', [])
        for c in coins:
            if c.get('coin') == 'USDT':
                return c.get('walletBalance', 'N/A')
    except Exception as e:
        log('Balance error: {}'.format(str(e)[:80]), 'WARN')
    return 'N/A'


def format_order_alert(order):
    oid    = order.get('orderId',       order.get('id',                 'N/A'))
    amount = order.get('amount',        order.get('orderAmount',        'N/A'))
    price  = order.get('price',         'N/A')
    fiat   = order.get('currencyId',    order.get('fiatCurrency',       'KES'))
    qty    = order.get('quantity',      order.get('notifyTokenQuantity','N/A'))
    side   = order.get('side',          order.get('tradeType',          'N/A'))
    status = order.get('orderStatus',   order.get('status',             'N/A'))
    buyer  = order.get('buyerNickName', order.get('nickName',           'N/A'))
    side_label = '\U0001f7e2 BUY' if str(side) in ['0', 'Buy', 'BUY'] else '\U0001f534 SELL'
    return (
        '\U0001f6d2 *New P2P Order!*\n\n'
        '{} Order\n'
        '\u2015\u2015\u2015\u2015\u2015\u2015\u2015\u2015\n'
        '\U0001f4b0 Amount : {} {}\n'
        '\U0001f4b5 Price  : {} {}\n'
        '\U0001f4e6 Qty    : {} USDT\n'
        '\U0001f464 Buyer  : {}\n'
        '\U0001f4cb Status : {}\n'
        '\U0001f9fe Order ID: `{}`\n\n'
        '\U0001f550 {} EAT'
    ).format(side_label, amount, fiat, price, fiat, qty, buyer, status, oid, eat())


# ══════════════════════════════════════════════════════════════
# REPORTS
# ══════════════════════════════════════════════════════════════
def send_pnl_summary():
    global _last_pnl_date
    today = date.today()
    if _last_pnl_date == today:
        return
    log('Generating P&L summary...')
    order_count, total_usdt, total_kes, avg_price = get_today_pnl()
    balance = fetch_balance()
    save_pnl(order_count, total_usdt, total_kes, avg_price, balance)
    _last_pnl_date = today
    send_telegram((
        '\U0001f4b9 *Daily P&L Summary*\n'
        '\U0001f4c5 {}\n\n'
        '\u2015\u2015\u2015\u2015\u2015\u2015\u2015\u2015\u2015\u2015\n'
        '\U0001f4e6 Orders completed : {}\n'
        '\U0001f4b5 USDT sold        : {:.4f} USDT\n'
        '\U0001f4b0 KES received     : {:.2f} KES\n'
        '\U0001f4c8 Avg sell price   : {:.2f} KES/USDT\n'
        '\U0001f4b3 Current balance  : {} USDT\n'
        '\u2015\u2015\u2015\u2015\u2015\u2015\u2015\u2015\u2015\u2015\n\n'
        '\U0001f550 {} EAT'
    ).format(eat(), order_count, total_usdt, total_kes, avg_price, balance, eat()))


def maybe_send_daily_report():
    global _last_report_date
    now   = datetime.now()
    today = date.today()
    if now.hour == DAILY_REPORT_HOUR and _last_report_date != today:
        _last_report_date = today
        balance    = fetch_balance()
        uptime_hrs = int((now - _start_time).total_seconds() / 3600)
        send_telegram((
            '\U0001f4ca *Daily Summary - {} EAT*\n\n'
            '\U0001f4b0 Balance : {} USDT\n'
            '\u23f1 Uptime  : {} hours\n'
            '\U0001f30d Platform: Render (Cloud)'
        ).format(eat(), balance, uptime_hrs))
        send_pnl_summary()


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def main():
    log('=' * 50)
    log('  CypherX Bybit P2P Monitor - Render Cloud')
    log('=' * 50)
    log('Check interval  : {}s'.format(CHECK_INTERVAL))
    log('Keepalive       : {}s'.format(KEEPALIVE_INTERVAL))
    log('Daily report    : {}:00 EAT'.format(DAILY_REPORT_HOUR))
    log('Health server   : port {}'.format(PORT))

    # Start health server in background thread
    t = threading.Thread(target=run_health_server, daemon=True)
    t.start()
    log('Health server started on port {}'.format(PORT))

    # Init database
    init_db()

    # Seed existing orders
    log('Seeding existing orders...')
    for o in fetch_pending_orders():
        oid = o.get('orderId', o.get('id', ''))
        if oid:
            _seen_orders.add(oid)
    log('Seeded {} existing orders.'.format(len(_seen_orders)))

    # Initial keepalive
    refresh_online_status()

    send_telegram((
        '\U0001f7e2 *Bybit Monitor - Render Cloud Started*\n\n'
        'Order alerts: ON\n'
        'Keepalive: every {}s\n'
        'P&L summary: daily 08:00 EAT\n'
        'Database: PostgreSQL\n\n'
        '\U0001f550 {} EAT'
    ).format(KEEPALIVE_INTERVAL, eat()))

    cycle        = 0
    net_outage   = False
    outage_start = None

    while True:
        time.sleep(CHECK_INTERVAL)
        cycle += 1
        try:
            maybe_send_daily_report()

            if not check_internet():
                if not net_outage:
                    net_outage   = True
                    outage_start = datetime.now()
                    log('Network outage detected.', 'WARN')
                else:
                    mins = int((datetime.now() - outage_start).total_seconds() / 60)
                    log('Network down - {} min.'.format(mins), 'WARN')
                continue

            if net_outage:
                net_outage = False
                dur = int((datetime.now() - outage_start).total_seconds() / 60)
                log('Network restored after {} min.'.format(dur))
                send_telegram('\U0001f7e2 *Network Restored*\n\nBack online after {} min.\n\n\U0001f550 {} EAT'.format(dur, eat()))

            # Keepalive
            if time.time() - _last_keepalive >= KEEPALIVE_INTERVAL:
                refresh_online_status()

            # Order monitoring
            orders = fetch_pending_orders()
            log('Cycle {} - {} pending orders.'.format(cycle, len(orders)))
            for o in orders:
                oid = o.get('orderId', o.get('id', ''))
                if oid and oid not in _seen_orders:
                    _seen_orders.add(oid)
                    save_order(o)
                    log('NEW ORDER: {}'.format(oid))
                    send_telegram(format_order_alert(o))

            # 12hr heartbeat
            if cycle > 0 and cycle % 720 == 0:
                balance    = fetch_balance()
                uptime_hrs = int((datetime.now() - _start_time).total_seconds() / 3600)
                send_telegram((
                    '\U0001f49a *12hr Heartbeat - Render*\n\n'
                    'Cycles : {}\nBalance: {} USDT\nUptime : {} hrs\n\n\U0001f550 {} EAT'
                ).format(cycle, balance, uptime_hrs, eat()))

        except Exception as e:
            log('Loop error: {}'.format(str(e)[:120]), 'ERROR')


if __name__ == '__main__':
    main()
