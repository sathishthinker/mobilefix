from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash, Response, send_file
import fitz
from functools import wraps
from datetime import datetime, timedelta, timezone, date
import calendar, csv, io, hashlib, hmac, os, re, json, random, string, base64, time, sqlite3, uuid, secrets
import urllib.request, urllib.error
from werkzeug.security import generate_password_hash, check_password_hash

try:
    import psycopg2, psycopg2.extras
except ImportError:
    psycopg2 = None

try:
    import pyotp
except ImportError:
    pyotp = None

app = Flask(__name__)
DATABASE_URL = os.environ.get('DATABASE_URL', '')
USE_PG = bool(DATABASE_URL)
IST = timezone(timedelta(hours=5, minutes=30))

def _load_secret_key():
    env = os.environ.get('SECRET_KEY')
    if env:
        return env
    os.makedirs('instance', exist_ok=True)
    path = os.path.join('instance', 'secret_key')
    if os.path.exists(path):
        with open(path, 'rb') as f:
            return f.read()
    key = os.urandom(24)
    with open(path, 'wb') as f:
        f.write(key)
    return key

app.secret_key = _load_secret_key()
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=365)
app.config['TEMPLATES_AUTO_RELOAD'] = True
def sql_now():
    if USE_PG:
        return "to_char(NOW() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')"
    return "strftime('%Y-%m-%d %H:%M:%S','now')"

def sql_date(col):
    if USE_PG:
        return f"SUBSTRING({col} FROM 1 FOR 10)"
    return f"substr({col}, 1, 10)"

def sql_hour(col):
    if USE_PG:
        return f"SUBSTRING({col} FROM 12 FOR 2)"
    return f"substr({col}, 12, 2)"

def sql_dow(col):
    if USE_PG:
        return f"EXTRACT(DOW FROM {col}::timestamp)::int"
    return f"CAST(strftime('%w', {col}) AS INTEGER)"

# ── DB Wrapper ─────────────────────────────────────────────────────────────────
class DbWrapper:
    """Thin wrapper that matches the sqlite3 interface used throughout."""
    def __init__(self, conn, pg=True):
        self._conn = conn
        self._pg = pg
        if pg:
            self._cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            self._cur = conn.cursor()

    def execute(self, sql, params=()):
        if not self._pg:
            sql = sql.replace('%s', '?')
        self._cur.execute(sql, params)
        return self._cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        try: self._cur.close()
        except: pass
        try: self._conn.close()
        except: pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        self.close()

def get_db():
    if USE_PG:
        if psycopg2 is None:
            raise RuntimeError('DATABASE_URL is set but psycopg2 is not installed')
        return DbWrapper(psycopg2.connect(DATABASE_URL), pg=True)
    os.makedirs('instance', exist_ok=True)
    db_path = os.environ.get('DATABASE_PATH', os.path.join('instance', 'mobilefix.db'))
    conn = sqlite3.connect(db_path, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA foreign_keys = ON')
    return DbWrapper(conn, pg=False)

def _now_str():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def _today_str():
    return datetime.now(IST).strftime('%Y-%m-%d')

def _hsn_code(val):
    s = re.sub(r'\D', '', str(val or ''))
    return s if 4 <= len(s) <= 8 else ''

def credit_due_bounds():
    today = datetime.now(IST).date()
    return today.isoformat(), (today + timedelta(days=15)).isoformat()

def valid_credit_due_date(due_s):
    s = (due_s or '').strip()[:10]
    if not s:
        return False
    dmin, dmax = credit_due_bounds()
    return dmin <= s <= dmax

# ── Template filter ────────────────────────────────────────────────────────────
from markupsafe import Markup, escape

GST_STATE_LIST = [
    ('01', 'JAMMU & KASHMIR'),
    ('02', 'HIMACHAL PRADESH'),
    ('03', 'PUNJAB'),
    ('04', 'CHANDIGARH'),
    ('05', 'UTTARAKHAND'),
    ('06', 'HARYANA'),
    ('07', 'DELHI'),
    ('08', 'RAJASTHAN'),
    ('09', 'UTTAR PRADESH'),
    ('10', 'BIHAR'),
    ('11', 'SIKKIM'),
    ('12', 'ARUNACHAL PRADESH'),
    ('13', 'NAGALAND'),
    ('14', 'MANIPUR'),
    ('15', 'MIZORAM'),
    ('16', 'TRIPURA'),
    ('17', 'MEGHALAYA'),
    ('18', 'ASSAM'),
    ('19', 'WEST BENGAL'),
    ('20', 'JHARKHAND'),
    ('21', 'ODISHA'),
    ('22', 'CHHATTISGARH'),
    ('23', 'MADHYA PRADESH'),
    ('24', 'GUJARAT'),
    ('26', 'DADRA & NAGAR HAVELI AND DAMAN & DIU'),
    ('27', 'MAHARASHTRA'),
    ('28', 'ANDHRA PRADESH'),
    ('29', 'KARNATAKA'),
    ('30', 'GOA'),
    ('31', 'LAKSHADWEEP'),
    ('32', 'KERALA'),
    ('33', 'TAMIL NADU'),
    ('34', 'PUDUCHERRY'),
    ('35', 'ANDAMAN & NICOBAR ISLANDS'),
    ('36', 'TELANGANA'),
    ('37', 'ANDHRA PRADESH (NEW)'),
    ('38', 'LADAKH'),
    ('96', 'FOREIGN COUNTRY'),
    ('97', 'OTHER TERRITORY'),
]
GST_STATE_BY_CODE = {c: n for c, n in GST_STATE_LIST}

def _gst_state_label(code):
    code = str(code or '').strip()[:2]
    if not re.match(r'^\d{2}$', code):
        return ''
    name = GST_STATE_BY_CODE.get(code)
    return f'{code}-{name}' if name else ''

def _gst_state_from_gstin(gstin):
    g = re.sub(r'[^0-9A-Z]', '', (gstin or '').upper())
    if len(g) < 2 or not g[:2].isdigit():
        return ''
    return _gst_state_label(g[:2])

def _seed_gst_states(db):
    db.execute('''CREATE TABLE IF NOT EXISTS gst_states (
        code TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )''')
    for code, name in GST_STATE_LIST:
        db.execute(
            "INSERT INTO gst_states (code, name) VALUES (%s, %s) ON CONFLICT (code) DO UPDATE SET name=excluded.name",
            (code, name))
    db.commit()

PAY_METHOD_COLORS = {
    'Cash': ('#15803d', '#dcfce7'),
    'UPI': ('#6d28d9', '#ede9fe'),
    'Card': ('#0369a1', '#e0f2fe'),
    'Bank': ('#c2410c', '#ffedd5'),
    'Bank Transfer': ('#c2410c', '#ffedd5'),
    'Credit': ('#be123c', '#ffe4e6'),
}

@app.template_global()
def pay_method_tag(method):
    m = (method or '').strip()
    if not m:
        return Markup('<span style="color:#94a3b8;">—</span>')
    fg, bg = PAY_METHOD_COLORS.get(m, ('#475569', '#f1f5f9'))
    return Markup(
        f'<span class="pay-m-tag" style="color:{fg};background:{bg};">'
        f'{escape(m)}</span>'
    )

@app.template_filter('to_ist')
def to_ist_filter(dt_str):
    if not dt_str: return '—'
    try:
        dt = datetime.fromisoformat(str(dt_str)[:19]) + timedelta(hours=5, minutes=30)
        return dt.strftime('%Y-%m-%d %H:%M')
    except:
        return str(dt_str)[:16]

def inventory_is_low_stock(qty, reorder_qty):
    try:
        threshold = float(reorder_qty or 0)
        on_hand = float(qty or 0)
    except (TypeError, ValueError):
        return False
    return threshold > 0 and on_hand <= threshold

@app.template_filter('is_low_stock')
def is_low_stock_filter(item):
    try:
        if isinstance(item, dict):
            return inventory_is_low_stock(item.get('qty'), item.get('reorder_qty'))
        return inventory_is_low_stock(item['qty'], item['reorder_qty'])
    except Exception:
        return False

SALES_ENDPOINTS = {
    'sales_hub', 'sale_new', 'print_sale', 'print_sale_refund', 'return_sale', 'inventory', 'inventory_search',
    'inventory_item_log', 'print_inventory_log', 'sales_customers', 'sales_customer_search', 'sales_customer_by_phone',
    'collect_sale', 'collect_all_sales'
}
SERVICE_ENDPOINTS = {
    'service_hub', 'jobs', 'add_job', 'update_job', 'verify_happy_code', 'set_reminder',
    'deliver_job', 'cancel_job_route', 'record_refund', 'rework_job', 'delete_job',
    'invoices', 'print_invoice', 'mark_invoice_paid', 'collect_all_service', 'customers', 'customer_search',
    'verify_imei_pin', 'print_barcode', 'print_job_card'
}

def _ensure_csrf():
    tok = session.get('_csrf')
    if not tok:
        session['_csrf'] = secrets.token_urlsafe(32)
    return session['_csrf']

@app.context_processor
def inject_csrf():
    return {'csrf_token': _ensure_csrf()}

@app.context_processor
def inject_gst_states():
    return {'gst_states': [{'code': c, 'name': n} for c, n in GST_STATE_LIST]}

@app.before_request
def _csrf_protect():
    if request.method in ('GET', 'HEAD', 'OPTIONS', 'TRACE'):
        return
    if request.endpoint == 'static':
        return
    expected = session.get('_csrf')
    sent = (
        request.headers.get('X-CSRFToken')
        or request.headers.get('X-CSRF-Token')
        or request.form.get('_csrf')
    )
    if not sent and request.is_json:
        data = request.get_json(silent=True) or {}
        if isinstance(data, dict):
            sent = data.get('_csrf')
    if not expected or not sent or not hmac.compare_digest(str(sent), str(expected)):
        wants_json = (
            request.is_json
            or (request.path or '').startswith('/api/')
            or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        )
        if wants_json:
            return jsonify({'ok': False, 'error': 'Session expired. Refresh the page and try again.'}), 403
        flash('Session expired. Please refresh and try again.', 'error')
        ep = request.endpoint or ''
        auth_eps = (
            'login', 'register', 'login_2fa',
            'forgot_password', 'forgot_password_otp', 'forgot_password_reset',
        )
        if ep in auth_eps:
            try:
                return redirect(url_for(ep))
            except Exception:
                pass
        return redirect(request.referrer or url_for('login'))

@app.before_request
def _set_app_area():
    if not session.get('user_id') or session.get('role') == 'admin':
        return
    ep = request.endpoint or ''
    if ep == 'dashboard':
        session['app_area'] = 'home'
    elif ep in SALES_ENDPOINTS:
        session['app_area'] = 'sales'
    elif ep in SERVICE_ENDPOINTS:
        session['app_area'] = 'service'
    elif ep in ('reports', 'reports_print') and not session.get('app_area'):
        session['app_area'] = 'service'

@app.before_request
def _enforce_shop_rules():
    if session.get('role') == 'admin':
        return
    if session.get('impersonator_id'):
        ep = request.endpoint or ''
        if ep in ('static', 'logout', 'admin_stop_impersonate', None):
            return
        if not session.get('staff_id'):
            return
    ep = request.endpoint or ''
    if session.get('user_id') and session.get('shop_role') == 'salesperson' and ep in OWNER_ONLY_ENDPOINTS:
        return _owner_denied()
    if not session.get('user_id'):
        return
    if ep in ('static', 'logout', None):
        return
    token = session.get('device_token')
    staff_id = session.get('staff_id')
    if not token and not staff_id:
        return
    db = get_db()
    try:
        if token:
            row = db.execute(
                "SELECT id FROM shop_devices WHERE owner_id=%s AND token=%s",
                (session['user_id'], token)).fetchone()
            if not row:
                session.clear()
                flash('This device was removed by the shop owner. Sign in again.', 'error')
                return redirect(url_for('login'))
        if staff_id:
            st = db.execute(
                "SELECT * FROM shop_staff WHERE id=%s AND owner_id=%s",
                (staff_id, session['user_id'])).fetchone()
            if not st or int(st['enabled'] or 0) != 1:
                session.clear()
                flash('This sales login is disabled. Ask the shop owner.', 'error')
                return redirect(url_for('login'))
            flags = staff_flags(st)
            session['can_sale'] = flags['can_sale']
            session['can_collect'] = flags['can_collect']
            session['can_jobs'] = flags['can_jobs']
            session['staff_kind'] = flags['job_kind']
            if ep == 'sale_new' and not flags['can_sale']:
                return _staff_perm_denied('New Sale')
            if ep in ('collect_sale', 'collect_all_sales', 'collect_all_service') and not flags['can_collect']:
                return _staff_perm_denied('Collect payment')
            if ep in ('add_job', 'update_job', 'deliver_job', 'rework_job') and not flags['can_jobs']:
                return _staff_perm_denied('Service jobs')
    finally:
        db.close()

@app.before_request
def _auto_close_shop_shifts():
    if session.get('role') == 'admin' or not session.get('user_id'):
        return
    ep = request.endpoint or ''
    if ep in ('static', 'logout', 'login', None):
        return
    uid = session['user_id']
    db = get_db()
    try:
        user = db.execute(
            "SELECT shop_open_time, shop_close_time FROM users WHERE id=%s", (uid,)).fetchone()
        close_at = shop_last_close_utc(
            _rg(user, 'shop_open_time') if user else None,
            _rg(user, 'shop_close_time') if user else None)
        if not close_at or session.get('_shift_ac') == close_at:
            return
        auto_close_overdue_shifts(db, uid, user)
        session['_shift_ac'] = close_at
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()

@app.before_request
def _record_shop_page_view():
    _log_shop_page_view()

@app.context_processor
def inject_shop_logo():
    dmin, dmax = credit_due_bounds()
    extra = {
        'due_min': dmin, 'due_max': dmax, 'app_area': session.get('app_area') or 'home',
        'is_owner': session.get('shop_role') != 'salesperson',
        'shop_role': session.get('shop_role') or 'owner',
        'staff_name': session.get('staff_name') or '',
        'can_sale': int(session.get('can_sale', 1) or 0) if session.get('shop_role') == 'salesperson' else 1,
        'can_collect': int(session.get('can_collect', 1) or 0) if session.get('shop_role') == 'salesperson' else 1,
        'can_jobs': int(session.get('can_jobs', 1) or 0) if session.get('shop_role') == 'salesperson' else 1,
        'staff_kind': session.get('staff_kind') or 'sales',
        'plan_display_name': plan_display_name,
    }
    if not session.get('user_id') or session.get('role') == 'admin':
        return {'shop_logo': None, 'global_notification': None, **extra}
    db = get_db()
    row = db.execute("SELECT logo FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    
    notif_rows = db.execute("SELECT key, value FROM app_settings WHERE key IN ('global_notif_active', 'global_notif_msg', 'global_notif_id')").fetchall()
    notif = {r['key']: r['value'] for r in notif_rows}
    global_notif = None
    if notif.get('global_notif_active') == '1' and notif.get('global_notif_msg'):
        global_notif = {
            'active': True,
            'msg': notif.get('global_notif_msg'),
            'id': notif.get('global_notif_id', '')
        }
        
    db.close()
    extra['shop_logo'] = row['logo'] if row else None
    extra['app_area'] = session.get('app_area') or 'home'
    extra['global_notification'] = global_notif
    return extra

def generate_happy_code():
    return ''.join(random.choices(string.digits, k=6))

# ── DB Init ────────────────────────────────────────────────────────────────────
def init_db():
    db = get_db()
    pk = "SERIAL PRIMARY KEY" if USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
    totp_col = "BOOLEAN DEFAULT FALSE" if USE_PG else "INTEGER DEFAULT 0"
    try:
        db.execute(f'''CREATE TABLE IF NOT EXISTS users (
            id {pk},
            phone TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            shop_name TEXT,
            address TEXT,
            role TEXT DEFAULT 'user',
            enabled INTEGER DEFAULT 1,
            trial_start TEXT,
            subscription_plan TEXT,
            subscription_end TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS repair_jobs (
            id {pk},
            user_id INTEGER,
            customer_name TEXT,
            customer_phone TEXT,
            device_model TEXT,
            imei TEXT,
            imei_billing TEXT,
            issue TEXT,
            aadhar_number TEXT,
            received_without TEXT,
            status TEXT DEFAULT 'Received',
            cost REAL DEFAULT 0,
            notes TEXT,
            expected_return TEXT,
            delivery_date TEXT,
            cancel_reason TEXT,
            quote_items TEXT,
            advance_amount REAL DEFAULT 0,
            advance_method TEXT,
            paid_status TEXT DEFAULT 'Unpaid',
            happy_code TEXT,
            reminder_date TEXT,
            rework_details TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS invoices (
            id {pk},
            user_id INTEGER,
            job_id INTEGER,
            customer_name TEXT,
            customer_phone TEXT,
            items TEXT,
            total REAL,
            advance_amount REAL DEFAULT 0,
            discount REAL DEFAULT 0,
            pay_method TEXT,
            paid TEXT DEFAULT 'Unpaid',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(job_id) REFERENCES repair_jobs(id)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS subscription_history (
            id {pk},
            user_id INTEGER NOT NULL,
            plan TEXT,
            start_date TEXT,
            end_date TEXT,
            activated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )''')
        existing_imei = db.execute(
            "SELECT value FROM app_settings WHERE key='imei_override_code'").fetchone()
        if not existing_imei:
            imei_code = (os.environ.get('IMEI_OVERRIDE_CODE') or secrets.token_urlsafe(8)).replace('-', '')[:12].upper()
            db.execute(
                "INSERT INTO app_settings (key,value) VALUES ('imei_override_code',%s) ON CONFLICT (key) DO NOTHING",
                (imei_code,))
            os.makedirs('instance', exist_ok=True)
            with open(os.path.join('instance', 'imei_override.txt'), 'w', encoding='utf-8') as fh:
                fh.write(imei_code + '\n')
        db.commit()

        # Seed admin (no well-known default password)
        admin_email = os.environ.get('ADMIN_EMAIL', 'admin@mobilefix.com')
        admin_phone = os.environ.get('ADMIN_PHONE', '0000000000')
        admin_exists = db.execute("SELECT id FROM users WHERE role='admin' LIMIT 1").fetchone()
        if not admin_exists:
            admin_plain = os.environ.get('ADMIN_PASSWORD') or secrets.token_urlsafe(14)
            if not os.environ.get('ADMIN_PASSWORD'):
                os.makedirs('instance', exist_ok=True)
                with open(os.path.join('instance', 'admin_bootstrap.txt'), 'w', encoding='utf-8') as fh:
                    fh.write(f'email={admin_email}\nphone={admin_phone}\npassword={admin_plain}\n')
                print('Admin account created. Password saved to instance/admin_bootstrap.txt', flush=True)
            db.execute(
                '''INSERT INTO users (phone,email,password,shop_name,role,enabled,trial_start)
                   VALUES (%s,%s,%s,'MobileFix Admin','admin',1,%s)
                   ON CONFLICT (email) DO NOTHING''',
                (admin_phone, admin_email, hash_pw(admin_plain), _now_str()))
            db.commit()

        def _add_column(table, col):
            stmt = (
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col}"
                if USE_PG else
                f"ALTER TABLE {table} ADD COLUMN {col}"
            )
            try:
                db.execute(stmt)
                db.commit()
            except Exception:
                db.rollback()

        for col in [
            "imei TEXT", "imei_billing TEXT", "aadhar_number TEXT", "received_without TEXT",
            "expected_return TEXT", "delivery_date TEXT", "cancel_reason TEXT", "quote_items TEXT",
            "advance_amount REAL DEFAULT 0", "advance_method TEXT", "paid_status TEXT DEFAULT 'Unpaid'",
            "happy_code TEXT", "reminder_date TEXT", "rework_details TEXT", "advance_history TEXT",
            "diagnosed_at TEXT", "refund_amount REAL DEFAULT 0", "refund_method TEXT", "refund_date TEXT",
            "device_brand TEXT", "original_job_id INTEGER", "diagnosis_history TEXT"
        ]:
            _add_column("repair_jobs", col)

        for col in ["advance_amount REAL DEFAULT 0", "discount REAL DEFAULT 0",
                    "pay_method TEXT", "paid TEXT DEFAULT 'Unpaid'", "due_date TEXT",
                    "payment_history TEXT"]:
            _add_column("invoices", col)

        for col in ["logo TEXT", "google_review_link TEXT", "phone TEXT",
                    "door_no TEXT", "street TEXT", "city TEXT", "pincode TEXT",
                    "totp_secret TEXT", f"totp_enabled {totp_col}",
                    "imei_skip INTEGER DEFAULT 0", "imei_skip_pin TEXT",
                    "extra_staff INTEGER DEFAULT 0",
                    "extra_devices INTEGER DEFAULT 0",
                    "shop_open_time TEXT DEFAULT '09:00'",
                    "shop_close_time TEXT DEFAULT '21:00'",
                    "absent_alert_hide_date TEXT",
                    "admin_notes TEXT",
                    "gstin TEXT", "state TEXT"]:
            _add_column("users", col)

        db.execute(f'''CREATE TABLE IF NOT EXISTS login_logs (
            id {pk},
            user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
            identifier TEXT,
            ip_address TEXT,
            user_agent TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
        )''')
        for col in ["fail_reason TEXT", "device_type TEXT", "location TEXT"]:
            _add_column("login_logs", col)
        db.execute(f'''CREATE TABLE IF NOT EXISTS inventory_items (
            id {pk},
            user_id INTEGER NOT NULL,
            sku TEXT,
            name TEXT NOT NULL,
            category TEXT DEFAULT 'Accessory',
            unit TEXT DEFAULT 'PCS',
            qty REAL DEFAULT 0,
            reorder_qty REAL DEFAULT 0,
            cost_price REAL DEFAULT 0,
            sell_price REAL DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )''')
        _add_column("inventory_items", "sub_category TEXT")
        
        db.execute(f'''CREATE TABLE IF NOT EXISTS inventory_categories (
            id {pk},
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS inventory_subcategories (
            id {pk},
            user_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(category_id) REFERENCES inventory_categories(id)
        )''')
        
        # Seed default categories for existing users
        users = db.execute('SELECT id FROM users').fetchall()
        for u in users:
            uid = u['id']
            c_count = db.execute('SELECT COUNT(*) as c FROM inventory_categories WHERE user_id=%s', (uid,)).fetchone()['c']
            if c_count == 0:
                for cat in ['Phone', 'Accessory', 'Spare', 'Other']:
                    db.execute('INSERT INTO inventory_categories (user_id, name) VALUES (%s, %s)', (uid, cat))
                    
        db.execute(f'''CREATE TABLE IF NOT EXISTS stock_movements (
            id {pk},
            user_id INTEGER NOT NULL,
            item_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            qty REAL NOT NULL,
            ref_type TEXT,
            ref_id INTEGER,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(item_id) REFERENCES inventory_items(id)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS inventory_item_logs (
            id {pk},
            user_id INTEGER NOT NULL,
            item_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            summary TEXT,
            qty_before REAL,
            qty_after REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(item_id) REFERENCES inventory_items(id)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS warranty_replacements (
            id {pk},
            user_id INTEGER NOT NULL,
            item_id INTEGER NOT NULL,
            qty REAL NOT NULL,
            supplier TEXT NOT NULL,
            reason TEXT NOT NULL,
            reason_other TEXT,
            faulty_serial TEXT,
            new_serial TEXT,
            claim_no TEXT,
            note TEXT,
            status TEXT DEFAULT 'taken',
            taken_at TEXT,
            replaced_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(item_id) REFERENCES inventory_items(id)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS sales_bills (
            id {pk},
            user_id INTEGER NOT NULL,
            bill_no INTEGER,
            customer_name TEXT,
            customer_phone TEXT,
            items TEXT,
            subtotal REAL DEFAULT 0,
            discount REAL DEFAULT 0,
            total REAL DEFAULT 0,
            pay_method TEXT DEFAULT 'Cash',
            paid_status TEXT DEFAULT 'Paid',
            paid_amount REAL DEFAULT 0,
            due_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )''')
        db.commit()
        for col in ["qty_before REAL", "qty_after REAL"]:
            _add_column("stock_movements", col)
        _add_column("inventory_items", "serial_no TEXT")
        _add_column("inventory_items", "hsn_code TEXT")
        _add_column("inventory_items", "gst_rate REAL DEFAULT 0")
        for col in ["paid_amount REAL DEFAULT 0", "due_date TEXT",
                    "return_reason TEXT", "refund_amount REAL DEFAULT 0",
                    "refund_method TEXT", "refund_date TEXT", "txn_id TEXT",
                    "orig_discount REAL", "staff_id INTEGER", "staff_name TEXT",
                    "is_gst INTEGER DEFAULT 0", "gst_rate REAL DEFAULT 0",
                    "customer_gstin TEXT", "billing_state TEXT", "customer_address TEXT",
                    "cgst REAL DEFAULT 0", "sgst REAL DEFAULT 0", "igst REAL DEFAULT 0"]:
            _add_column("sales_bills", col)
        for col in ["staff_id INTEGER", "staff_name TEXT"]:
            _add_column("repair_jobs", col)
            _add_column("invoices", col)
        try:
            db.execute(
                "UPDATE sales_bills SET paid_amount=total WHERE paid_status='Paid' AND COALESCE(paid_amount,0)=0")
            repair_sale_statuses(db)
            db.commit()
        except Exception:
            db.rollback()

        db.execute(f'''CREATE TABLE IF NOT EXISTS sales_customers (
            id {pk},
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )''')
        for col in ["gstin TEXT", "state TEXT", "address TEXT"]:
            _add_column("sales_customers", col)
        db.execute(f'''CREATE TABLE IF NOT EXISTS purchase_bills (
            id {pk},
            user_id INTEGER NOT NULL,
            bill_no TEXT NOT NULL,
            vendor_name TEXT,
            vendor_gstin TEXT,
            vendor_state TEXT,
            invoice_date TEXT,
            subtotal REAL DEFAULT 0,
            cgst REAL DEFAULT 0,
            sgst REAL DEFAULT 0,
            igst REAL DEFAULT 0,
            total REAL DEFAULT 0,
            items TEXT,
            created_at TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS loyalty_accounts (
            id {pk},
            user_id INTEGER NOT NULL,
            phone TEXT NOT NULL,
            points INTEGER DEFAULT 0,
            updated_at TEXT,
            UNIQUE(user_id, phone)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS loyalty_ledger (
            id {pk},
            user_id INTEGER NOT NULL,
            phone TEXT NOT NULL,
            area TEXT,
            kind TEXT,
            points INTEGER DEFAULT 0,
            rupees REAL DEFAULT 0,
            ref_type TEXT,
            ref_id INTEGER,
            note TEXT,
            created_at TEXT
        )''')
        for col in ["loyalty_points INTEGER DEFAULT 0", "loyalty_rupees REAL DEFAULT 0",
                    "payment_history TEXT"]:
            _add_column("invoices", col)
            _add_column("sales_bills", col)
        db.execute(f'''CREATE TABLE IF NOT EXISTS shop_staff (
            id {pk},
            owner_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            email TEXT,
            password TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(owner_id) REFERENCES users(id)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS shop_devices (
            id {pk},
            owner_id INTEGER NOT NULL,
            token TEXT NOT NULL,
            label TEXT,
            user_agent TEXT,
            ip_address TEXT,
            staff_id INTEGER,
            last_seen TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(owner_id) REFERENCES users(id)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS shop_staff_log (
            id {pk},
            owner_id INTEGER NOT NULL,
            staff_id INTEGER,
            staff_name TEXT,
            action TEXT,
            ref_type TEXT,
            ref_id INTEGER,
            detail TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS admin_shop_log (
            id {pk},
            shop_user_id INTEGER NOT NULL,
            admin_id INTEGER,
            admin_name TEXT,
            action TEXT NOT NULL,
            detail TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS shop_page_views (
            id {pk},
            user_id INTEGER NOT NULL,
            staff_id INTEGER,
            staff_name TEXT,
            who TEXT,
            endpoint TEXT,
            path TEXT,
            page TEXT,
            method TEXT,
            ip_address TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        try:
            db.execute('CREATE INDEX IF NOT EXISTS idx_spv_user_at ON shop_page_views (user_id, created_at)')
            db.execute('CREATE INDEX IF NOT EXISTS idx_spv_created ON shop_page_views (created_at)')
            db.commit()
        except Exception:
            db.rollback()
        db.execute(f'''CREATE TABLE IF NOT EXISTS shop_shifts (
            id {pk},
            owner_id INTEGER NOT NULL,
            staff_id INTEGER NOT NULL,
            clock_in TEXT NOT NULL,
            clock_out TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        for col in [
            "in_photo TEXT", "out_photo TEXT",
            "in_lat REAL", "in_lng REAL", "in_acc REAL",
            "out_lat REAL", "out_lng REAL", "out_acc REAL",
            "in_client_time TEXT", "out_client_time TEXT",
            "in_source TEXT", "out_source TEXT",
        ]:
            _add_column("shop_shifts", col)
        db.execute(f'''CREATE TABLE IF NOT EXISTS shop_lunch_breaks (
            id {pk},
            owner_id INTEGER NOT NULL,
            staff_id INTEGER NOT NULL,
            shift_id INTEGER,
            lunch_out TEXT NOT NULL,
            lunch_in TEXT,
            source TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS shop_login_alerts (
            id {pk},
            owner_id INTEGER NOT NULL,
            staff_id INTEGER,
            staff_name TEXT,
            device_label TEXT,
            ip_address TEXT,
            seen INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS notification_acks (
            id {pk},
            notif_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )''')
        db.execute(f'''CREATE TABLE IF NOT EXISTS global_notif_history (
            id TEXT PRIMARY KEY,
            msg TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        db.commit()
        for col in [
            "job_kind TEXT DEFAULT 'sales'",
            "can_sale INTEGER DEFAULT 1",
            "can_collect INTEGER DEFAULT 1",
            "can_jobs INTEGER DEFAULT 1",
            "target_sales REAL DEFAULT 0",
            "target_jobs INTEGER DEFAULT 0",
            "commission_pct REAL DEFAULT 0",
            "salary_monthly REAL DEFAULT 0",
            "salary_days INTEGER DEFAULT 26",
            "salary_hours REAL DEFAULT 8",
        ]:
            _add_column("shop_staff", col)
        db.execute("UPDATE users SET trial_start=%s WHERE trial_start IS NULL", (_now_str(),))
        db.commit()

        try:
            db.execute("""UPDATE users SET
                shop_name = UPPER(shop_name),
                door_no   = UPPER(COALESCE(door_no,'')),
                street    = UPPER(COALESCE(street,'')),
                city      = UPPER(COALESCE(city,''))
                WHERE role != 'admin'""")
            db.commit()
        except Exception:
            db.rollback()
        try:
            _seed_gst_states(db)
        except Exception:
            db.rollback()
    finally:
        db.close()

# ── Helpers ────────────────────────────────────────────────────────────────────
def session_staff_stamp():
    if session.get('shop_role') == 'salesperson' and session.get('staff_id'):
        return session.get('staff_id'), (session.get('staff_name') or 'SALES').strip().upper()
    return None, 'OWNER'

def _rg(row, key, default=None):
    if row is None:
        return default
    try:
        if key not in row.keys():
            return default
    except Exception:
        pass
    try:
        v = row[key]
    except Exception:
        return default
    return default if v is None else v

def staff_flags(row):
    kind = (_rg(row, 'job_kind') or 'sales')
    if str(kind) == 'tech':
        ds, dc, dj = 0, 0, 1
    else:
        ds, dc, dj = 1, 1, 1
    def flag(key, default):
        v = _rg(row, key, None)
        if v is None:
            return default
        try:
            return 1 if int(v) else 0
        except (TypeError, ValueError):
            return default
    return {
        'job_kind': 'tech' if str(kind) == 'tech' else 'sales',
        'can_sale': flag('can_sale', ds),
        'can_collect': flag('can_collect', dc),
        'can_jobs': flag('can_jobs', dj),
        'target_sales': float(_rg(row, 'target_sales') or 0),
        'target_jobs': int(_rg(row, 'target_jobs') or 0),
        'commission_pct': float(_rg(row, 'commission_pct') or 0),
        'salary_monthly': float(_rg(row, 'salary_monthly') or 0),
        'salary_days': int(_rg(row, 'salary_days') or 26),
        'salary_hours': float(_rg(row, 'salary_hours') or 8),
    }

def log_staff(db, owner_id, action, ref_type=None, ref_id=None, detail='', staff_id=None, staff_name=None):
    if staff_id is None and staff_name is None:
        staff_id, staff_name = session_staff_stamp()
    try:
        db.execute(
            '''INSERT INTO shop_staff_log (owner_id,staff_id,staff_name,action,ref_type,ref_id,detail,created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
            (owner_id, staff_id, staff_name, action, ref_type, ref_id, (detail or '')[:240], _now_str()))
    except Exception:
        pass

SHOP_PAGE_LABELS = {
    'dashboard': 'Home',
    'shop_profile': 'Profile',
    'shop_attendance': 'Attendance',
    'service_hub': 'Service',
    'jobs': 'Repair Jobs',
    'add_job': 'New Job',
    'set_reminder': 'Job Reminder',
    'invoices': 'Invoices',
    'print_invoice': 'Print Invoice',
    'sales_hub': 'Sales',
    'sale_new': 'New Sale',
    'print_sale': 'Print Bill',
    'print_sale_refund': 'Print Refund',
    'inventory': 'Inventory',
    'print_inventory_log': 'Stock Log Print',
    'sales_customers': 'Sales Customers',
    'customers': 'Customers',
    'reports': 'Reports',
    'reports_print': 'Print Reports',
    'settings': 'Settings',
    'shop_team': 'Team',
    'shop_staff_perf_print': 'Staff Performance',
    'shop_staff_perf_all_print': 'All Staff Performance',
    'shop_staff_perf_owner_print': 'Owner Performance',
    'manage_categories': 'Categories',
    'setup_2fa': 'Two-Factor Setup',
    'subscription_page': 'Subscription',
    'user_manual': 'Manual',
}

SHOP_PAGE_SKIP = {
    'static', 'logout', 'login', 'register', 'login_2fa', 'index',
    'forgot_password', 'forgot_password_2fa', 'forgot_password_reset',
    'shop_shift_photo', 'inventory_item_log', 'inventory_search',
    'sales_customer_search', 'sales_customer_by_phone', 'customer_search',
    'api_loyalty', 'shop_staff_perf_csv', 'shop_staff_perf_all_csv',
    'shop_staff_perf_owner_csv', 'terms_page', 'privacy_page', 'refund_policy_page',
}

def _session_who():
    if session.get('shop_role') == 'salesperson':
        return 'Tech' if session.get('staff_kind') == 'tech' else 'Sales'
    return 'Owner'

def _shop_page_label(endpoint, path):
    if endpoint and endpoint in SHOP_PAGE_LABELS:
        return SHOP_PAGE_LABELS[endpoint]
    if endpoint:
        return endpoint.replace('_', ' ').title()[:80]
    return (path or '/')[:80]

def _is_ajax_request():
    if request.is_json:
        return True
    xhr = (request.headers.get('X-Requested-With') or '').lower()
    if xhr in ('xmlhttprequest', 'fetch'):
        return True
    accept = (request.headers.get('Accept') or '').lower()
    if 'application/json' in accept and 'text/html' not in accept:
        return True
    return False

def _log_shop_page_view():
    if session.get('role') == 'admin' or not session.get('user_id'):
        return
    if request.method != 'GET':
        return
    ep = request.endpoint or ''
    if not ep or ep in SHOP_PAGE_SKIP or ep.startswith('api_') or ep.startswith('admin_'):
        return
    path = (request.path or '/')[:180]
    if path.startswith('/api/') or path.startswith('/admin/'):
        return
    if _is_ajax_request():
        return
    key = f'{ep}:{path}'
    now_ts = time.time()
    last_key = session.get('_pv_key')
    last_at = float(session.get('_pv_at') or 0)
    if last_key == key and (now_ts - last_at) < 25:
        return
    session['_pv_key'] = key
    session['_pv_at'] = now_ts
    uid = session['user_id']
    staff_id, staff_name = session_staff_stamp()
    who = _session_who()
    ip = (request.headers.get('X-Forwarded-For') or request.remote_addr or '').split(',')[0].strip()[:60]
    now_ist = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    db = get_db()
    try:
        db.execute(
            '''INSERT INTO shop_page_views
               (user_id,staff_id,staff_name,who,endpoint,path,page,method,ip_address,created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
            (uid, staff_id, staff_name, who, ep[:80], path,
             _shop_page_label(ep, path), 'GET', ip, now_ist))
        if not session.get('_pv_pruned'):
            cutoff = (datetime.now(IST) - timedelta(days=60)).strftime('%Y-%m-%d %H:%M:%S')
            db.execute("DELETE FROM shop_page_views WHERE created_at < %s", (cutoff,))
            session['_pv_pruned'] = 1
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()

def log_admin_shop(db, shop_user_id, action, detail=''):
    admin_id = session.get('user_id')
    admin_name = 'Admin'
    try:
        if admin_id:
            row = db.execute(
                "SELECT shop_name, email FROM users WHERE id=%s", (admin_id,)).fetchone()
            if row:
                admin_name = (row['shop_name'] or row['email'] or 'Admin')[:80]
        db.execute(
            '''INSERT INTO admin_shop_log (shop_user_id,admin_id,admin_name,action,detail,created_at)
               VALUES (%s,%s,%s,%s,%s,%s)''',
            (shop_user_id, admin_id, admin_name, action, (detail or '')[:500], _now_str()))
    except Exception:
        pass

def _title_words(s):
    if not s:
        return s or ''
    return re.sub(r'\b([a-z])', lambda m: m.group(1).upper(), str(s))

def _parse_hm(s, default_h, default_m):
    raw = (str(s or '').strip())[:5]
    if re.match(r'^\d{2}:\d{2}$', raw):
        h, m = int(raw[:2]), int(raw[3:5])
        if 0 <= h <= 23 and 0 <= m <= 59:
            return h * 60 + m
    return default_h * 60 + default_m

def shop_is_open_now(open_time, close_time, now=None):
    """True when current IST time is within shop opening hours."""
    now = now or datetime.now(IST)
    cur = now.hour * 60 + now.minute
    open_m = _parse_hm(open_time, 9, 0)
    close_m = _parse_hm(close_time, 21, 0)
    if open_m == close_m:
        return True
    if open_m < close_m:
        return open_m <= cur <= close_m
    return cur >= open_m or cur <= close_m

def shop_last_close_utc(open_time, close_time, now=None):
    """UTC clock-out stamp for the last closing, or None while the shop is open / 24h."""
    now = now or datetime.now(IST)
    open_m = _parse_hm(open_time, 9, 0)
    close_m = _parse_hm(close_time, 21, 0)
    if open_m == close_m:
        return None
    if shop_is_open_now(open_time, close_time, now):
        return None
    ch, cm = divmod(close_m, 60)
    today = now.date()
    close_today = datetime(today.year, today.month, today.day, ch, cm, tzinfo=IST)
    last = close_today if now >= close_today else close_today - timedelta(days=1)
    return last.astimezone(timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')

def auto_close_overdue_shifts(db, uid, user=None):
    """Clock out open shifts and lunch after shop closing time."""
    if user is None:
        try:
            user = db.execute(
                "SELECT shop_open_time, shop_close_time FROM users WHERE id=%s", (uid,)).fetchone()
        except Exception:
            db.rollback()
            return 0
    if not user:
        return 0
    close_at = shop_last_close_utc(_rg(user, 'shop_open_time'), _rg(user, 'shop_close_time'))
    if not close_at:
        return 0
    n = 0
    try:
        for lb in db.execute(
            "SELECT id, staff_id FROM shop_lunch_breaks "
            "WHERE owner_id=%s AND lunch_in IS NULL AND lunch_out<=%s",
            (uid, close_at)).fetchall():
            db.execute(
                "UPDATE shop_lunch_breaks SET lunch_in=%s WHERE id=%s AND owner_id=%s",
                (close_at, lb['id'], uid))
            log_staff(db, uid, 'lunch_in', 'shift', lb['staff_id'],
                      'Auto closed at shop close', staff_id=lb['staff_id'])
            n += 1
        for sh in db.execute(
            "SELECT id, staff_id FROM shop_shifts "
            "WHERE owner_id=%s AND clock_out IS NULL AND clock_in<=%s",
            (uid, close_at)).fetchall():
            db.execute(
                "UPDATE shop_shifts SET clock_out=%s, out_source=%s, out_client_time=%s "
                "WHERE id=%s AND owner_id=%s",
                (close_at, 'auto', close_at, sh['id'], uid))
            log_staff(db, uid, 'clock_out', 'shift', sh['staff_id'],
                      'Auto closed at shop close', staff_id=sh['staff_id'])
            n += 1
        if n:
            db.commit()
    except Exception:
        db.rollback()
        return 0
    return n

def _staff_stamp_sql(sid):
    if sid is None:
        return " AND (staff_id IS NULL OR staff_id=0)", []
    return " AND staff_id=%s", [sid]

def _shift_hours(a, b):
    try:
        t1 = datetime.strptime(str(a)[:19], '%Y-%m-%d %H:%M:%S')
        t2 = datetime.strptime(str(b)[:19], '%Y-%m-%d %H:%M:%S')
        return round(max(0, (t2 - t1).total_seconds()) / 3600.0, 2)
    except Exception:
        return 0.0

def staff_open_lunch(db, uid, sid):
    row = db.execute(
        "SELECT id, lunch_out, shift_id FROM shop_lunch_breaks "
        "WHERE owner_id=%s AND staff_id=%s AND lunch_in IS NULL ORDER BY id DESC LIMIT 1",
        (uid, sid)).fetchone()
    return dict(row) if row else None

def staff_lunch_breaks(db, uid, sid, from_d=None, to_d=None, day=None):
    sql = "SELECT id, lunch_out, lunch_in, shift_id FROM shop_lunch_breaks WHERE owner_id=%s AND staff_id=%s"
    params = [uid, sid]
    if day:
        sql += f" AND {sql_date('lunch_out')}=%s"
        params.append(day)
    elif from_d and to_d:
        sql += f" AND {sql_date('lunch_out')}>=%s AND {sql_date('lunch_out')}<=%s"
        params.extend([from_d, to_d])
    sql += " ORDER BY id"
    return [dict(r) for r in db.execute(sql, tuple(params)).fetchall()]

def _lunch_hours_for_range(db, uid, sid, from_d, to_d):
    total = 0.0
    for r in staff_lunch_breaks(db, uid, sid, from_d=from_d, to_d=to_d):
        total += _shift_hours(r['lunch_out'], r['lunch_in'] or _now_str())
    return round(total, 2)

def staff_salary_cfg(row):
    flags = row if isinstance(row, dict) and 'salary_monthly' in row else staff_flags(row)
    monthly = max(0.0, float(flags.get('salary_monthly') or 0))
    try:
        work_days = int(flags.get('salary_days') or 26)
    except (TypeError, ValueError):
        work_days = 26
    work_days = min(31, max(1, work_days))
    try:
        day_hours = float(flags.get('salary_hours') or 8)
    except (TypeError, ValueError):
        day_hours = 8.0
    if day_hours <= 0:
        day_hours = 8.0
    daily = round(monthly / work_days, 2) if monthly else 0.0
    hourly = round(daily / day_hours, 2) if daily else 0.0
    return {
        'salary_monthly': monthly,
        'salary_days': work_days,
        'salary_hours': day_hours,
        'daily_rate': daily,
        'hourly_rate': hourly,
    }

def staff_salary_calc(cfg, present_days, hours_worked):
    present_days = int(present_days or 0)
    hours_worked = round(float(hours_worked or 0), 2)
    monthly = float(cfg.get('salary_monthly') or 0)
    work_days = int(cfg.get('salary_days') or 26)
    daily = float(cfg.get('daily_rate') or 0)
    hourly = float(cfg.get('hourly_rate') or 0)
    if monthly <= 0:
        due = 0.0
    elif present_days >= work_days:
        due = monthly
    else:
        due = round(present_days * daily, 2)
    return {
        **cfg,
        'present_days': present_days,
        'hours_worked': hours_worked,
        'salary_due': due,
        'pay_by_hours': round(hours_worked * hourly, 2) if hourly else 0.0,
    }

def _parse_geo(raw_lat, raw_lng, raw_acc=None):
    try:
        lat = float(raw_lat)
        lng = float(raw_lng)
    except (TypeError, ValueError):
        return None
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        return None
    acc = None
    try:
        if raw_acc not in (None, ''):
            acc = float(raw_acc)
    except (TypeError, ValueError):
        acc = None
    return lat, lng, acc

def _parse_selfie(photo):
    if not photo or not isinstance(photo, str):
        return None
    photo = photo.strip()
    if not photo.startswith('data:image/') or ';base64,' not in photo:
        return None
    if len(photo) > 220000:
        return None
    b64 = photo.split(';base64,', 1)[1]
    try:
        raw = base64.b64decode(b64)
    except Exception:
        return None
    if len(raw) < 400 or len(raw) > 160000:
        return None
    return photo

def _clock_payload():
    data = request.get_json(silent=True) if request.is_json else None
    src = data if isinstance(data, dict) else request.form
    return src

def _clock_wants_json():
    if request.is_json:
        return True
    acc = (request.headers.get('Accept') or '')
    return 'application/json' in acc

def load_staff_panel(db, uid, staff_rows, devices, from_d=None, to_d=None):
    auto_close_overdue_shifts(db, uid)
    today = _today_str()
    if not from_d or not to_d:
        y, m = date.today().year, date.today().month
        from_d, to_d = month_span(y, m)
    collected = "COALESCE(paid_amount, CASE WHEN paid_status='Paid' THEN total ELSE 0 END)"
    out = []
    for s in staff_rows:
        sid = s['id']
        flags = staff_flags(s)
        month = load_staff_month_sheet(
            db, uid, sid, from_d, to_d, with_lines=False, commission_pct=flags['commission_pct'])
        today_row = db.execute(
            f"SELECT COUNT(*) AS n, COALESCE(SUM({collected}),0) AS amt FROM sales_bills "
            f"WHERE user_id=%s AND staff_id=%s AND paid_status!='Returned' AND {sql_date('created_at')}=%s",
            (uid, sid, today)).fetchone()
        today_jobs = db.execute(
            f"SELECT COUNT(*) AS n FROM repair_jobs WHERE user_id=%s AND staff_id=%s AND {sql_date('created_at')}=%s",
            (uid, sid, today)).fetchone()
        first_last = db.execute(
            f"SELECT MIN(created_at) AS first_at, MAX(created_at) AS last_at FROM ("
            f" SELECT created_at FROM sales_bills WHERE user_id=%s AND staff_id=%s AND {sql_date('created_at')}=%s"
            f" UNION ALL"
            f" SELECT created_at FROM repair_jobs WHERE user_id=%s AND staff_id=%s AND {sql_date('created_at')}=%s"
            f") t", (uid, sid, today, uid, sid, today)).fetchone()
        open_shift = db.execute(
            "SELECT id,clock_in FROM shop_shifts WHERE owner_id=%s AND staff_id=%s AND clock_out IS NULL "
            "ORDER BY id DESC LIMIT 1", (uid, sid)).fetchone()
        shifts = db.execute(
            f"SELECT id,clock_in,clock_out,in_lat,in_lng,out_lat,out_lng,"
            f" CASE WHEN in_photo IS NOT NULL AND in_photo!='' THEN 1 ELSE 0 END AS has_in_photo,"
            f" CASE WHEN out_photo IS NOT NULL AND out_photo!='' THEN 1 ELSE 0 END AS has_out_photo"
            f" FROM shop_shifts WHERE owner_id=%s AND staff_id=%s "
            f"AND {sql_date('clock_in')}>=%s AND {sql_date('clock_in')}<=%s ORDER BY id",
            (uid, sid, _iso_add_days(from_d, -1), _iso_add_days(to_d, 1))).fetchall()
        work_days = set()
        for r in db.execute(
            f"SELECT {sql_date('created_at')} AS d FROM sales_bills WHERE user_id=%s AND staff_id=%s "
            f"AND {sql_date('created_at')}>=%s AND {sql_date('created_at')}<=%s",
            (uid, sid, from_d, to_d)).fetchall():
            if r['d']:
                work_days.add(str(r['d'])[:10])
        for r in db.execute(
            f"SELECT {sql_date('created_at')} AS d FROM repair_jobs WHERE user_id=%s AND staff_id=%s "
            f"AND {sql_date('created_at')}>=%s AND {sql_date('created_at')}<=%s",
            (uid, sid, from_d, to_d)).fetchall():
            if r['d']:
                work_days.add(str(r['d'])[:10])
        attend = []
        y, mo = int(from_d[:4]), int(from_d[5:7])
        last = calendar.monthrange(y, mo)[1]
        shift_by_day = {}
        for sh in shifts:
            d0 = _ist_date_str(sh['clock_in'])
            if not d0 or d0 < from_d or d0 > to_d:
                continue
            shift_by_day.setdefault(d0, []).append(sh)
        lunch_by_day = {}
        for lb in staff_lunch_breaks(db, uid, sid, from_d=from_d, to_d=to_d):
            d0 = _ist_date_str(lb['lunch_out'])
            if d0:
                lunch_by_day.setdefault(d0, []).append(lb)
        open_lunch = staff_open_lunch(db, uid, sid)
        lunch_today = staff_lunch_breaks(db, uid, sid, day=today)
        for day in range(1, last + 1):
            ds = f'{y:04d}-{mo:02d}-{day:02d}'
            shs = shift_by_day.get(ds) or []
            lbs = lunch_by_day.get(ds) or []
            hours = sum(_shift_hours(x['clock_in'], x['clock_out'] or _now_str()) for x in shs)
            hours = max(0.0, round(hours - sum(
                _shift_hours(x['lunch_out'], x['lunch_in'] or _now_str()) for x in lbs), 2))
            cin = _ist_hm(shs[0]['clock_in']) if shs else ''
            cout = ''
            if shs and shs[-1]['clock_out']:
                cout = _ist_hm(shs[-1]['clock_out'])
            punches = []
            for x in shs:
                punches.append({
                    'id': x['id'],
                    'in': _ist_hm(x['clock_in']),
                    'out': _ist_hm(x['clock_out']) if x['clock_out'] else '',
                    'in_lat': x['in_lat'], 'in_lng': x['in_lng'],
                    'out_lat': x['out_lat'], 'out_lng': x['out_lng'],
                    'has_in_photo': int(x['has_in_photo'] or 0),
                    'has_out_photo': int(x['has_out_photo'] or 0),
                })
            attend.append({
                'd': ds, 'n': day, 'in': cin, 'out': cout, 'hours': hours,
                'work': ds in work_days, 'shift': bool(shs),
                'punches': punches,
                'lunches': [{
                    'out': _ist_hm(x['lunch_out']),
                    'in': _ist_hm(x['lunch_in']) if x['lunch_in'] else '',
                } for x in lbs],
            })
        logs = [dict(r) for r in db.execute(
            "SELECT action,ref_type,ref_id,detail,created_at,staff_name FROM shop_staff_log "
            "WHERE owner_id=%s AND staff_id=%s ORDER BY id DESC LIMIT 40", (uid, sid)).fetchall()]
        recent_sales = db.execute(
            "SELECT id,bill_no,customer_name,total,paid_status,created_at FROM sales_bills "
            "WHERE user_id=%s AND staff_id=%s ORDER BY id DESC LIMIT 30", (uid, sid)).fetchall()
        recent_jobs = db.execute(
            "SELECT id,customer_name,device_model,status,cost,created_at FROM repair_jobs "
            "WHERE user_id=%s AND staff_id=%s ORDER BY id DESC LIMIT 30", (uid, sid)).fetchall()
        sdevs = [dict(d) for d in devices if d['staff_id'] == sid]
        tgt_s = flags['target_sales']
        tgt_j = flags['target_jobs']
        coll = month['sales_collected']
        month['target_sales'] = tgt_s
        month['target_jobs'] = tgt_j
        month['sales_pct'] = round(min(100.0, (coll / tgt_s) * 100), 1) if tgt_s > 0 else None
        month['jobs_pct'] = round(min(100.0, (month['jobs_n'] / tgt_j) * 100), 1) if tgt_j > 0 else None
        pay = staff_salary_calc(
            staff_salary_cfg(flags),
            sum(1 for a in attend if a.get('shift')),
            sum(float(a.get('hours') or 0) for a in attend if a.get('shift')),
        )
        out.append({
            'id': sid, 'name': s['name'], 'phone': s['phone'], 'email': s['email'],
            'enabled': s['enabled'], 'created_at': s['created_at'],
            'job_kind': flags['job_kind'],
            'can_sale': flags['can_sale'], 'can_collect': flags['can_collect'], 'can_jobs': flags['can_jobs'],
            'target_sales': tgt_s, 'target_jobs': tgt_j, 'commission_pct': flags['commission_pct'],
            'salary_monthly': pay['salary_monthly'],
            'salary_days': pay['salary_days'],
            'salary_hours': pay['salary_hours'],
            'daily_rate': pay['daily_rate'],
            'hourly_rate': pay['hourly_rate'],
            'present_days': pay['present_days'],
            'hours_worked': pay['hours_worked'],
            'salary_due': pay['salary_due'],
            'pay_by_hours': pay['pay_by_hours'],
            'devices': sdevs,
            'sales_n': month['sales_n'], 'sales_amt': month['sales_amt'],
            'jobs_n': month['jobs_n'], 'inv_n': month['inv_n'], 'inv_amt': month['inv_amt'],
            'month': month,
            'today_sales_n': int(today_row['n'] or 0),
            'today_sales_amt': float(today_row['amt'] or 0),
            'today_jobs_n': int(today_jobs['n'] or 0),
            'first_at': _rg(first_last, 'first_at') or '',
            'last_at': _rg(first_last, 'last_at') or '',
            'open_shift': dict(open_shift) if open_shift else None,
            'open_lunch': open_lunch,
            'lunch_today': lunch_today,
            'clocked_today': bool(db.execute(
                f"SELECT 1 FROM shop_shifts WHERE owner_id=%s AND staff_id=%s AND {sql_date('clock_in')}=%s LIMIT 1",
                (uid, sid, today)).fetchone()),
            'attend': attend,
            'logs': logs,
            'recent_sales': [dict(r) for r in recent_sales],
            'recent_jobs': [dict(j) for j in recent_jobs],
        })
    return out

def staff_attendance_today(db, uid, staff_panel=None, user=None):
    """Enabled staff with no clock-in today are absent."""
    today = _today_str()
    absent, on_shift, present = [], [], []
    if user is None:
        user = db.execute(
            "SELECT shop_open_time, shop_close_time FROM users WHERE id=%s", (uid,)).fetchone()
    auto_close_overdue_shifts(db, uid, user)
    open_t = (_rg(user, 'shop_open_time') or '09:00')[:5]
    close_t = (_rg(user, 'shop_close_time') or '21:00')[:5]
    shop_open_now = shop_is_open_now(open_t, close_t)
    if staff_panel is not None:
        rows = [s for s in staff_panel if s.get('enabled')]
    else:
        rows = [dict(r) for r in db.execute(
            "SELECT id, name, phone, job_kind, enabled FROM shop_staff "
            "WHERE owner_id=%s AND enabled=1 ORDER BY name", (uid,)).fetchall()]
    for s in rows:
        sid = s['id']
        if staff_panel is None:
            open_sh = db.execute(
                "SELECT clock_in FROM shop_shifts WHERE owner_id=%s AND staff_id=%s AND clock_out IS NULL "
                "ORDER BY id DESC LIMIT 1", (uid, sid)).fetchone()
            clocked = db.execute(
                f"SELECT 1 FROM shop_shifts WHERE owner_id=%s AND staff_id=%s AND {sql_date('clock_in')}=%s LIMIT 1",
                (uid, sid, today)).fetchone()
            item = {'id': sid, 'name': s['name'], 'phone': s['phone'], 'job_kind': s.get('job_kind')}
            is_clocked = bool(clocked)
            if open_sh:
                item['clock_in'] = open_sh['clock_in']
                on_shift.append(item)
        else:
            item = {'id': sid, 'name': s['name'], 'phone': s['phone'], 'job_kind': s.get('job_kind')}
            is_clocked = bool(s.get('clocked_today'))
            if s.get('open_shift'):
                item['clock_in'] = s['open_shift'].get('clock_in')
                on_shift.append(item)
        if is_clocked:
            present.append(item)
        else:
            absent.append(item)
    return {
        'today': today,
        'total_enabled': len(rows),
        'absent': absent,
        'on_shift': on_shift,
        'present': present,
        'shop_open_time': open_t,
        'shop_close_time': close_t,
        'shop_open_now': shop_open_now,
        'show_absent_alert': shop_open_now,
    }

def parse_perf_month(raw):
    today = date.today()
    if raw and re.match(r'^\d{4}-\d{2}$', str(raw)):
        y, m = int(raw[:4]), int(raw[5:7])
        if 2020 <= y <= today.year + 1 and 1 <= m <= 12:
            return y, m
    return today.year, today.month

def month_span(y, m):
    last = calendar.monthrange(y, m)[1]
    return date(y, m, 1).isoformat(), date(y, m, last).isoformat()

def _iso_add_days(iso, n):
    d = datetime.strptime(str(iso)[:10], '%Y-%m-%d').date() + timedelta(days=int(n))
    return d.isoformat()

def load_staff_month_sheet(db, uid, sid, from_d, to_d, with_lines=True, commission_pct=0):
    date_sql = f" AND {sql_date('created_at')}>=%s AND {sql_date('created_at')}<=%s"
    stamp, extra = _staff_stamp_sql(sid)
    params = tuple([uid] + extra + [from_d, to_d])
    live = " AND paid_status!='Returned'"
    collected = "COALESCE(paid_amount, CASE WHEN paid_status='Paid' THEN total ELSE 0 END)"
    sales = db.execute(
        f"SELECT COUNT(*) AS n, COALESCE(SUM(total),0) AS amt, COALESCE(SUM({collected}),0) AS collected "
        f"FROM sales_bills WHERE user_id=%s{stamp}{live}{date_sql}", params).fetchone()
    credit = db.execute(
        f"SELECT COALESCE(SUM(total - {collected}),0) AS amt FROM sales_bills "
        f"WHERE user_id=%s{stamp}{live} AND paid_status IN ('Unpaid','Partial'){date_sql}", params).fetchone()
    ret = db.execute(
        f"SELECT COUNT(*) AS n, COALESCE(SUM(COALESCE(refund_amount,total)),0) AS amt FROM sales_bills "
        f"WHERE user_id=%s{stamp} AND paid_status='Returned'{date_sql}", params).fetchone()
    jobs = db.execute(
        f"SELECT COUNT(*) AS n, COALESCE(SUM(cost),0) AS amt FROM repair_jobs "
        f"WHERE user_id=%s{stamp}{date_sql}", params).fetchone()
    invs = db.execute(
        f"SELECT COUNT(*) AS n, COALESCE(SUM(total),0) AS amt FROM invoices "
        f"WHERE user_id=%s{stamp}{date_sql}", params).fetchone()
    coll = float(sales['collected'] or 0)
    pct = float(commission_pct or 0)
    out = {
        'sales_n': int(sales['n'] or 0), 'sales_amt': float(sales['amt'] or 0),
        'sales_collected': coll,
        'credit_amt': float(credit['amt'] or 0),
        'returns_n': int(ret['n'] or 0), 'returns_amt': float(ret['amt'] or 0),
        'jobs_n': int(jobs['n'] or 0), 'jobs_amt': float(jobs['amt'] or 0),
        'inv_n': int(invs['n'] or 0), 'inv_amt': float(invs['amt'] or 0),
        'commission_pct': pct,
        'commission_amt': round(coll * pct / 100.0, 2) if pct else 0.0,
        'sales': [], 'jobs': [], 'invoices': [],
    }
    if with_lines:
        out['sales'] = [dict(r) for r in db.execute(
            f"SELECT id,bill_no,customer_name,customer_phone,total,paid_status,pay_method,paid_amount,created_at "
            f"FROM sales_bills WHERE user_id=%s{stamp}{date_sql} ORDER BY id", params).fetchall()]
        out['jobs'] = [dict(r) for r in db.execute(
            f"SELECT id,customer_name,customer_phone,device_model,status,cost,created_at "
            f"FROM repair_jobs WHERE user_id=%s{stamp}{date_sql} ORDER BY id", params).fetchall()]
        out['invoices'] = [dict(r) for r in db.execute(
            f"SELECT id,job_id,customer_name,total,paid,pay_method,created_at "
            f"FROM invoices WHERE user_id=%s{stamp}{date_sql} ORDER BY id", params).fetchall()]
    return out

def _perf_csv_rows(staff_name, sheet, month_label):
    rows = [
        ['Sales person performance sheet'],
        ['Name', staff_name],
        ['Month', month_label],
        [],
        ['Summary'],
        ['Sales bills', sheet['sales_n'], 'Sales amount', f"{sheet['sales_amt']:.2f}",
         'Collected', f"{sheet['sales_collected']:.2f}"],
        ['Credit due', f"{sheet.get('credit_amt', 0):.2f}", 'Returns', sheet.get('returns_n') or 0,
         'Return amount', f"{sheet.get('returns_amt', 0):.2f}"],
        ['Commission %', sheet.get('commission_pct') or 0, 'Commission', f"{sheet.get('commission_amt', 0):.2f}"],
        ['Monthly salary', f"{sheet.get('salary_monthly', 0):.2f}", 'Present days', sheet.get('present_days') or 0,
         'Salary due', f"{sheet.get('salary_due', 0):.2f}"],
        ['Hours worked', sheet.get('hours_worked') or 0, 'Daily rate', f"{sheet.get('daily_rate', 0):.2f}"],
        ['Jobs', sheet['jobs_n'], 'Job value', f"{sheet['jobs_amt']:.2f}"],
        ['Invoices', sheet['inv_n'], 'Invoice amount', f"{sheet['inv_amt']:.2f}"],
        [],
        ['Sales bills'],
        ['Bill', 'Date', 'Customer', 'Phone', 'Amount', 'Collected', 'Status', 'Pay method'],
    ]
    for b in sheet.get('sales') or []:
        rows.append([
            f"SAL-{int(b.get('bill_no') or 0):04d}", str(b.get('created_at') or '')[:10],
            b.get('customer_name') or '', b.get('customer_phone') or '',
            f"{float(b.get('total') or 0):.2f}", f"{float(b.get('paid_amount') or 0):.2f}",
            b.get('paid_status') or '', b.get('pay_method') or '',
        ])
    if not sheet.get('sales'):
        rows.append(['No sales this month'])
    rows += [[], ['Service jobs'], ['Job', 'Date', 'Customer', 'Phone', 'Device', 'Status', 'Cost']]
    for j in sheet.get('jobs') or []:
        rows.append([
            f"#{j.get('id')}", str(j.get('created_at') or '')[:10],
            j.get('customer_name') or '', j.get('customer_phone') or '',
            j.get('device_model') or '', j.get('status') or '',
            f"{float(j.get('cost') or 0):.2f}" if j.get('cost') is not None else '',
        ])
    if not sheet.get('jobs'):
        rows.append(['No jobs this month'])
    rows += [[], ['Service invoices'], ['Invoice', 'Job', 'Date', 'Customer', 'Amount', 'Paid', 'Pay method']]
    for i in sheet.get('invoices') or []:
        rows.append([
            f"INV-{i.get('id')}", f"#{i.get('job_id') or ''}", str(i.get('created_at') or '')[:10],
            i.get('customer_name') or '', f"{float(i.get('total') or 0):.2f}",
            i.get('paid') or '', i.get('pay_method') or '',
        ])
    if not sheet.get('invoices'):
        rows.append(['No invoices this month'])
    return rows

def attach_salary_to_sheet(db, uid, sid, from_d, to_d, sheet, staff_row):
    cfg = staff_salary_cfg(staff_row)
    shifts = db.execute(
        f"SELECT clock_in,clock_out FROM shop_shifts WHERE owner_id=%s AND staff_id=%s "
        f"AND {sql_date('clock_in')}>=%s AND {sql_date('clock_in')}<=%s",
        (uid, sid, _iso_add_days(from_d, -1), _iso_add_days(to_d, 1))).fetchall()
    days = set()
    hours = 0.0
    for sh in shifts:
        d0 = _ist_date_str(sh['clock_in'])
        if not d0 or d0 < from_d or d0 > to_d:
            continue
        days.add(d0)
        hours += _shift_hours(sh['clock_in'], sh['clock_out'] or _now_str())
    hours = max(0.0, round(hours - _lunch_hours_for_range(db, uid, sid, from_d, to_d), 2))
    sheet.update(staff_salary_calc(cfg, len(days), hours))
    return sheet

def _csv_file(filename, rows):
    buf = io.StringIO()
    csv.writer(buf).writerows(rows)
    safe = re.sub(r'[^A-Za-z0-9._-]+', '_', filename)
    return Response(
        buf.getvalue().encode('utf-8-sig'), mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename="{safe}"'})

def _staff_for_owner(db, uid, sid):
    return db.execute(
        "SELECT * FROM shop_staff WHERE id=%s AND owner_id=%s", (sid, uid)).fetchone()

def hash_pw(p):
    return generate_password_hash(str(p or ''), method='pbkdf2:sha256', salt_length=16)

def _is_modern_hash(stored):
    s = str(stored or '')
    return s.startswith(('pbkdf2:', 'scrypt:', 'argon2:'))

def verify_pw(stored, p):
    stored = str(stored or '')
    p = str(p or '')
    if not stored or not p:
        return False
    if _is_modern_hash(stored):
        try:
            return check_password_hash(stored, p)
        except Exception:
            return False
    digest = hashlib.sha256(p.encode('utf-8')).hexdigest()
    if len(stored) == 64 and hmac.compare_digest(stored.lower(), digest):
        return True
    return False

def _upgrade_pw_if_needed(db, table, row_id, stored, plain):
    if not stored or _is_modern_hash(stored):
        return
    if table == 'users':
        db.execute("UPDATE users SET password=%s WHERE id=%s", (hash_pw(plain), row_id))
    elif table == 'shop_staff':
        db.execute("UPDATE shop_staff SET password=%s WHERE id=%s", (hash_pw(plain), row_id))

def clean_imei_serial(s):
    return re.sub(r'[^A-Z0-9]', '', (s or '').strip().upper())

def clean_aadhaar(s):
    return re.sub(r'\D', '', s or '')

def valid_aadhaar(s):
    return bool(re.fullmatch(r'\d{12}', clean_aadhaar(s)))

def job_needs_aadhaar(job):
    """FRP / flashing jobs require Aadhaar at deliver and invoice — not at create."""
    if job is None:
        return False
    issue = job['issue'] or ''
    quotes = ''
    try:
        quotes = job['quote_items'] or ''
    except (KeyError, IndexError):
        pass
    blob = f"{issue} {quotes}".lower()
    return 'frp' in blob or 'flash' in blob


def _request_password():
    data = request.get_json(silent=True) or {}
    return (data.get('password') or request.form.get('password') or request.form.get('admin_password') or '').strip()

def admin_password_ok(db, pw):
    if not pw:
        return False
    admin = db.execute(
        "SELECT password FROM users WHERE id=%s AND role='admin'",
        (session.get('user_id'),)).fetchone()
    return bool(admin and verify_pw(admin['password'], pw))

INV_CATEGORIES = ('Accessory', 'Spare', 'Phone', 'Other')
WARR_REASONS = ('DOA', 'Defective', 'Warranty claim', 'Wrong item', 'Other')

def db_insert_id(db, sql, params):
    if USE_PG:
        row = db.execute(sql + ' RETURNING id', params).fetchone()
        return int(row['id'] if row['id'] is not None else 0)
    db.execute(sql, params)
    return int(db._cur.lastrowid)

def next_sale_bill_no(db, uid):
    row = db.execute("SELECT COALESCE(MAX(bill_no),0) FROM sales_bills WHERE user_id=%s", (uid,)).fetchone()
    return int(row[0] or 0) + 1

def clean_txn_id(pay_method, raw):
    if pay_method not in ('UPI', 'Card', 'Bank'):
        return None
    t = re.sub(r'\s+', ' ', (raw or '').strip())[:80]
    return t or None

def load_sales_stats(db, uid, from_d=None, to_d=None):
    today = _today_str()
    date_sql = ''
    params = (uid,)
    if from_d and to_d:
        date_sql = f" AND {sql_date('created_at')}>=%s AND {sql_date('created_at')}<=%s"
        params = (uid, from_d, to_d)
    live = " AND paid_status!='Returned'"
    collected = "COALESCE(paid_amount, CASE WHEN paid_status='Paid' THEN total ELSE 0 END)"
    bills = db.execute(
        f"SELECT COUNT(*) FROM sales_bills WHERE user_id=%s{live}{date_sql}", params).fetchone()[0]
    amt = db.execute(
        f"SELECT COALESCE(SUM({collected}),0) FROM sales_bills WHERE user_id=%s{live}{date_sql}", params).fetchone()[0]
    today_n = db.execute(
        f"SELECT COUNT(*) FROM sales_bills WHERE user_id=%s{live} AND {sql_date('created_at')}=%s",
        (uid, today)).fetchone()[0]
    today_amt = db.execute(
        f"SELECT COALESCE(SUM({collected}),0) FROM sales_bills WHERE user_id=%s{live} AND {sql_date('created_at')}=%s",
        (uid, today)).fetchone()[0]
    credit = db.execute(
        f"SELECT COALESCE(SUM(total - {collected}),0) FROM sales_bills"
        f" WHERE user_id=%s{live} AND total > {collected} + 0.009", (uid,)).fetchone()[0]
    low = db.execute(
        "SELECT COUNT(*) FROM inventory_items WHERE user_id=%s AND active=1 AND reorder_qty > 0 AND qty<=reorder_qty",
        (uid,)).fetchone()[0]
    by_method = db.execute(
        f"SELECT pay_method, COUNT(*) AS n, COALESCE(SUM({collected}),0) AS amt FROM sales_bills"
        f" WHERE user_id=%s{live}{date_sql} GROUP BY pay_method", params).fetchall()
    return {
        'bills': int(bills or 0),
        'amount': float(amt or 0),
        'today_n': int(today_n or 0),
        'today_amt': float(today_amt or 0),
        'credit_due': float(credit or 0),
        'low_stock': int(low or 0),
        'by_method': [{'method': r['pay_method'] or 'Cash', 'n': int(r['n'] or 0), 'amt': float(r['amt'] or 0)} for r in by_method],
    }

LOYALTY_EARN_PER_100 = 10
LOYALTY_POINTS_PER_RUPEE = 4
LOYALTY_MIN = {'service': 250, 'sales': 300}

def loyalty_phone(phone):
    return re.sub(r'\D', '', str(phone or ''))[-10:]

def loyalty_earn_pts(rupees):
    return int(max(0.0, float(rupees or 0)) // 10)

def loyalty_rupees_from_pts(pts):
    return round(int(pts or 0) / float(LOYALTY_POINTS_PER_RUPEE), 2)

def loyalty_map(db, uid):
    try:
        rows = db.execute("SELECT phone, points FROM loyalty_accounts WHERE user_id=%s", (uid,)).fetchall()
    except Exception:
        db.rollback()
        return {}
    out = {}
    for r in rows:
        out[loyalty_phone(r['phone'])] = int(r['points'] or 0)
    return out

def loyalty_get(db, uid, phone):
    ph = loyalty_phone(phone)
    if len(ph) != 10:
        return {'phone': ph, 'points': 0}
    try:
        row = db.execute(
            "SELECT points FROM loyalty_accounts WHERE user_id=%s AND phone=%s", (uid, ph)).fetchone()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return {'phone': ph, 'points': 0}
    return {'phone': ph, 'points': int(row['points'] if row else 0)}

def loyalty_add(db, uid, phone, delta, area, kind, rupees, ref_type, ref_id, note=''):
    ph = loyalty_phone(phone)
    delta = int(delta or 0)
    if len(ph) != 10 or delta == 0:
        return loyalty_get(db, uid, phone)
    acc = db.execute(
        "SELECT id, points FROM loyalty_accounts WHERE user_id=%s AND phone=%s", (uid, ph)).fetchone()
    new_pts = max(0, int(acc['points'] if acc else 0) + delta)
    if acc:
        db.execute("UPDATE loyalty_accounts SET points=%s, updated_at=%s WHERE id=%s",
                   (new_pts, _now_str(), acc['id']))
    else:
        db.execute(
            "INSERT INTO loyalty_accounts (user_id,phone,points,updated_at) VALUES (%s,%s,%s,%s)",
            (uid, ph, new_pts, _now_str()))
    db.execute(
        '''INSERT INTO loyalty_ledger
           (user_id,phone,area,kind,points,rupees,ref_type,ref_id,note,created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (uid, ph, area, kind, delta, float(rupees or 0), ref_type, ref_id, note, _now_str()))
    return {'phone': ph, 'points': new_pts}

def loyalty_earn(db, uid, phone, area, rupees, ref_type, ref_id, note=''):
    pts = loyalty_earn_pts(rupees)
    if pts <= 0:
        return loyalty_get(db, uid, phone)
    return loyalty_add(db, uid, phone, pts, area, 'earn', rupees, ref_type, ref_id, note or f'Earn {pts} pts')

def loyalty_redeem(db, uid, phone, area, points, ref_type, ref_id):
    min_pts = LOYALTY_MIN.get(area) or 250
    pts = int(points or 0)
    if pts <= 0:
        return {'points': 0, 'rupees': 0.0, **loyalty_get(db, uid, phone)}, None
    if pts < min_pts:
        return None, f'Minimum redemption is {min_pts} points'
    acc = loyalty_get(db, uid, phone)
    if acc['points'] < pts:
        return None, f'Only {acc["points"]} points available'
    rupees = loyalty_rupees_from_pts(pts)
    bal = loyalty_add(db, uid, phone, -pts, area, 'redeem', rupees, ref_type, ref_id, f'Redeem {pts} pts')
    return {'points': pts, 'rupees': rupees, **bal}, None

def loyalty_info(db, uid, phone):
    acc = loyalty_get(db, uid, phone)
    pts = acc['points']
    return {
        'phone': acc['phone'],
        'points': pts,
        'value': loyalty_rupees_from_pts(pts),
        'earn': '₹100 = 10 points',
        'redeem': '4 points = ₹1',
        'min_service': LOYALTY_MIN['service'],
        'min_sales': LOYALTY_MIN['sales'],
        'can_service': pts >= LOYALTY_MIN['service'],
        'can_sales': pts >= LOYALTY_MIN['sales'],
    }

def sale_pay_status(total, paid_amount):
    if float(paid_amount or 0) + 0.009 >= float(total or 0):
        return 'Paid'
    if float(paid_amount or 0) > 0.009:
        return 'Partial'
    return 'Unpaid'

def sale_collected(bill):
    paid = float(bill['paid_amount'] or 0) if bill['paid_amount'] is not None else 0
    # Legacy rows: Paid with no paid_amount meant fully collected.
    # Do not treat a positive short payment as fully paid.
    if paid <= 0.009 and (bill['paid_status'] or '') == 'Paid':
        return float(bill['total'] or 0)
    return paid

def sale_balance(bill):
    if (bill['paid_status'] or '') == 'Returned':
        return 0.0
    return max(0.0, float(bill['total'] or 0) - sale_collected(bill))

def sale_live_status(bill):
    """Status from money received, not a stale paid_status flag."""
    if (bill['paid_status'] or '') == 'Returned':
        return 'Returned'
    return sale_pay_status(float(bill['total'] or 0), sale_collected(bill))

def enrich_sale_bill(bill):
    d = dict(bill)
    d['collected'] = sale_collected(bill)
    d['balance'] = sale_balance(bill)
    d['paid_status'] = sale_live_status(bill)
    return d

def _norm_phone(phone):
    return re.sub(r'\D', '', str(phone or ''))[-10:]

def _staff_can_collect():
    if session.get('shop_role') != 'salesperson':
        return True
    return int(session.get('can_collect') or 0) == 1

def _apply_sale_collect(db, uid, bill, amount, pay_method, due_date, txn_id):
    amount = round(float(amount or 0), 2)
    if amount <= 0.009:
        return None, 'Enter an amount greater than 0'
    if (bill['paid_status'] or '') == 'Returned':
        return None, 'Returned bills cannot be collected'
    new_paid = round(sale_collected(bill) + amount, 2)
    total = float(bill['total'] or 0)
    if new_paid > total + 0.005:
        return None, 'Amount cannot exceed balance due'
    status = sale_pay_status(total, new_paid)
    balance = max(0.0, total - new_paid)
    if balance > 0.01 and not valid_credit_due_date(due_date):
        return None, 'Due date must be within 15 days from today.'
    txn_id = clean_txn_id(pay_method, txn_id)
    if txn_id:
        db.execute(
            "UPDATE sales_bills SET paid_amount=%s,pay_method=%s,paid_status=%s,due_date=%s,txn_id=%s WHERE id=%s AND user_id=%s",
            (new_paid, pay_method, status, due_date or None, txn_id, bill['id'], uid))
    else:
        db.execute(
            "UPDATE sales_bills SET paid_amount=%s,pay_method=%s,paid_status=%s,due_date=%s WHERE id=%s AND user_id=%s",
            (new_paid, pay_method, status, due_date or None, bill['id'], uid))
    log_staff(db, uid, 'collect', 'sale', bill['id'], f'{amount:.2f} {pay_method}')
    loyalty_earn(db, uid, bill['customer_phone'], 'sales', amount, 'sale', bill['id'], 'Sales collect')
    return {
        'id': bill['id'], 'bill_no': bill['bill_no'], 'amount': amount,
        'paid': status, 'balance': balance, 'label': f"SAL-{int(bill['bill_no'] or 0):04d}",
    }, None

def _apply_invoice_collect(db, uid, inv, amount, pay_method, due_date):
    amount = round(float(amount or 0), 2)
    if amount <= 0.009:
        return None, 'Enter an amount to collect.'
    new_adv = round(float(inv['advance_amount'] or 0) + amount, 2)
    total = float(inv['total'] or 0)
    if new_adv > total + 0.005:
        return None, 'Amount cannot exceed balance due'
    balance = max(0.0, total - new_adv)
    paid = 'Paid' if balance < 0.01 else ('Partial' if new_adv > 0 else 'Unpaid')
    if balance > 0.01 and not valid_credit_due_date(due_date):
        return None, 'Due date must be within 15 days from today.'
    hist = parse_pay_hist(inv['payment_history'] if 'payment_history' in inv.keys() else None)
    if not hist and inv['job_id']:
        job_row = db.execute(
            "SELECT advance_history FROM repair_jobs WHERE id=%s AND user_id=%s",
            (inv['job_id'], uid)).fetchone()
        if job_row:
            hist = parse_pay_hist(job_row['advance_history'])
    hist = append_pay(hist, amount, pay_method, 'Due collected')
    pay_json = json.dumps(hist)
    db.execute(
        "UPDATE invoices SET advance_amount=%s,pay_method=%s,paid=%s,due_date=%s,payment_history=%s WHERE id=%s",
        (new_adv, pay_method, paid, due_date or None, pay_json, inv['id']))
    if inv['job_id']:
        db.execute(
            "UPDATE repair_jobs SET paid_status=%s,advance_amount=%s,advance_history=%s WHERE id=%s AND user_id=%s",
            (paid, new_adv, pay_json, inv['job_id'], uid))
    loyalty_earn(db, uid, inv['customer_phone'], 'service', amount, 'invoice', inv['id'], 'Service collect')
    return {
        'id': inv['id'], 'amount': amount, 'paid': paid, 'balance': balance,
        'label': f"INV-{int(inv['id']):04d}",
    }, None

def _apply_job_advance(db, uid, job, amount, pay_method):
    amount = round(float(amount or 0), 2)
    cost = float(job['cost'] or 0)
    old_adv = float(job['advance_amount'] or 0)
    due = max(0.0, cost - old_adv)
    if amount <= 0.009 or due <= 0.009:
        return None, 'Nothing due on this job'
    take = min(amount, due)
    new_adv = round(old_adv + take, 2)
    paid = 'Paid' if new_adv + 0.009 >= cost else ('Partial' if new_adv > 0.009 else 'Unpaid')
    hist = parse_pay_hist(job['advance_history'] if 'advance_history' in job.keys() else None)
    hist = append_pay(hist, take, pay_method, 'Due collected')
    pay_json = json.dumps(hist)
    db.execute(
        "UPDATE repair_jobs SET paid_status=%s,advance_amount=%s,advance_history=%s WHERE id=%s AND user_id=%s",
        (paid, new_adv, pay_json, job['id'], uid))
    loyalty_earn(db, uid, job['customer_phone'], 'service', take, 'job', job['id'], 'Job due collect')
    return {
        'id': job['id'], 'amount': take, 'paid': paid, 'balance': max(0.0, cost - new_adv),
        'label': f"JOB-{int(job['id'])}",
    }, None

def repair_sale_statuses(db, uid=None):
    """Fix Paid flags when paid_amount is less than the bill total."""
    extra = ''
    params = []
    if uid is not None:
        extra = ' AND user_id=%s'
        params.append(uid)
    db.execute(
        "UPDATE sales_bills SET paid_status=CASE "
        "WHEN COALESCE(paid_amount,0)+0.009>=COALESCE(total,0) THEN 'Paid' "
        "WHEN COALESCE(paid_amount,0)>0.009 THEN 'Partial' "
        "ELSE 'Unpaid' END "
        "WHERE COALESCE(paid_status,'')!='Returned'"
        " AND NOT (COALESCE(paid_status,'')='Paid' AND COALESCE(paid_amount,0)<=0.009)"
        + extra,
        tuple(params))

def _pay_now():
    return datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')

def parse_pay_hist(raw):
    try:
        hist = json.loads(raw or '[]')
        return [x for x in hist if isinstance(x, dict)] if isinstance(hist, list) else []
    except Exception:
        return []

def append_pay(hist, amount, method, note=''):
    hist = list(hist or [])
    amt = round(float(amount or 0), 2)
    if amt <= 0.009:
        return hist
    hist.append({
        'amount': amt,
        'method': (method or 'Cash') or 'Cash',
        'date': _pay_now(),
        'note': note or '',
    })
    return hist

def _sale_lines(bill):
    try:
        lines = json.loads(bill['items'] or '[]')
        return lines if isinstance(lines, list) else []
    except Exception:
        return []

def line_remain_qty(ln):
    return max(0.0, float(ln.get('qty') or 0) - float(ln.get('returned_qty') or 0))

def sale_orig_discount(bill):
    od = _inv_row_get(bill, 'orig_discount')
    if od is None:
        return float(bill['discount'] or 0)
    return float(od or 0)

def sale_remaining_figures(bill, lines=None):
    lines = lines if lines is not None else _sale_lines(bill)
    orig_sub = sum(float(ln.get('qty') or 0) * float(ln.get('price') or 0) for ln in lines)
    remain_sub = round(sum(line_remain_qty(ln) * float(ln.get('price') or 0) for ln in lines), 2)
    orig_disc = sale_orig_discount(bill)
    remain_disc = round(orig_disc * remain_sub / orig_sub, 2) if orig_sub > 0.0001 else 0.0
    remain_total = round(max(0.0, remain_sub - remain_disc), 2)
    fully = all(line_remain_qty(ln) <= 0.0001 for ln in lines) if lines else True
    return remain_sub, remain_disc, remain_total, fully

def find_sales_customer(db, uid, phone):
    phone = re.sub(r'\D', '', phone or '')[-10:]
    if len(phone) != 10:
        return None
    row = db.execute(
        "SELECT name, phone FROM sales_customers WHERE user_id=%s AND phone=%s",
        (uid, phone)).fetchone()
    if row:
        return {'name': (row['name'] or '').strip().upper(), 'phone': phone}
    row = db.execute(
        "SELECT customer_name AS name, customer_phone AS phone FROM sales_bills"
        " WHERE user_id=%s AND customer_phone LIKE %s ORDER BY id DESC LIMIT 1",
        (uid, f'%{phone}')).fetchone()
    if row:
        return {'name': (row['name'] or '').strip().upper(), 'phone': phone}
    return None

def upsert_sales_customer(db, uid, name, phone):
    phone = re.sub(r'\D', '', phone or '')[-10:]
    name = (name or '').strip().upper()
    if len(phone) != 10 or not name:
        return False
    row = db.execute(
        "SELECT id FROM sales_customers WHERE user_id=%s AND phone=%s", (uid, phone)).fetchone()
    if row:
        db.execute("UPDATE sales_customers SET name=%s WHERE id=%s AND user_id=%s", (name, row['id'], uid))
    else:
        db.execute(
            "INSERT INTO sales_customers (user_id,name,phone,created_at) VALUES (%s,%s,%s,%s)",
            (uid, name, phone, _now_str()))
    return True

def _qty_s(n):
    n = float(n or 0)
    if abs(n - round(n)) < 1e-9:
        return str(int(round(n)))
    return f'{n:.2f}'

def log_inventory_change(db, uid, item_id, kind, summary, qty_before=None, qty_after=None):
    db.execute(
        '''INSERT INTO inventory_item_logs
           (user_id,item_id,kind,summary,qty_before,qty_after,created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s)''',
        (uid, item_id, kind, summary or '', qty_before, qty_after, _now_str()))

def inventory_item_diff(old, new_vals):
    changes = []
    def _n(v):
        try:
            return float(v or 0)
        except (TypeError, ValueError):
            return 0.0
    if (str(old['name'] or '').strip()) != (str(new_vals['name'] or '').strip()):
        changes.append(f"Name {old['name'] or '—'} → {new_vals['name'] or '—'}")
    if (str(old['sku'] or '').strip()) != (str(new_vals['sku'] or '').strip()):
        changes.append(f"SKU {old['sku'] or '—'} → {new_vals['sku'] or '—'}")
    old_hsn = ''
    try:
        old_hsn = str(_rg(old, 'hsn_code') or '').strip()
    except Exception:
        old_hsn = ''
    if old_hsn != (str(new_vals.get('hsn_code') or '').strip()):
        changes.append(f"HSN {old_hsn or '—'} → {new_vals.get('hsn_code') or '—'}")
    old_gst = 0.0
    try:
        old_gst = float(_rg(old, 'gst_rate') or 0)
    except Exception:
        old_gst = 0.0
    new_gst = float(new_vals.get('gst_rate') or 0)
    if abs(old_gst - new_gst) >= 0.01:
        changes.append(f"GST {old_gst:g}% → {new_gst:g}%")
    old_sn = ''
    try:
        old_sn = str(old['serial_no'] or '').strip()
    except Exception:
        old_sn = ''
    if old_sn != (str(new_vals.get('serial_no') or '').strip()):
        changes.append(f"Serial Number {old_sn or '—'} → {new_vals.get('serial_no') or '—'}")
    if (str(old['category'] or '').strip()) != (str(new_vals['category'] or '').strip()):
        changes.append(f"Category {old['category'] or '—'} → {new_vals['category'] or '—'}")
    try:
        old_sc = str(old.get('sub_category') or '').strip()
    except Exception:
        old_sc = ''
    if old_sc != (str(new_vals.get('sub_category') or '').strip()):
        changes.append(f"Sub-category {old_sc or '—'} → {new_vals.get('sub_category') or '—'}")
    if (str(old['unit'] or '').strip()) != (str(new_vals['unit'] or '').strip()):
        changes.append(f"Unit {old['unit'] or '—'} → {new_vals['unit'] or '—'}")
    if abs(_n(old['reorder_qty']) - _n(new_vals['reorder_qty'])) >= 0.0001:
        changes.append(f"Low stock reminder {_qty_s(old['reorder_qty'])} → {_qty_s(new_vals['reorder_qty'])}")
    if abs(_n(old['cost_price']) - _n(new_vals['cost_price'])) >= 0.0001:
        changes.append(f"Cost ₹{_n(old['cost_price']):.2f} → ₹{_n(new_vals['cost_price']):.2f}")
    if abs(_n(old['sell_price']) - _n(new_vals['sell_price'])) >= 0.0001:
        changes.append(f"Sell ₹{_n(old['sell_price']):.2f} → ₹{_n(new_vals['sell_price']):.2f}")
    if int(old['active'] or 0) != int(new_vals['active'] or 0):
        changes.append('Set active' if new_vals['active'] else 'Set inactive')
    return changes

def apply_stock_change(db, uid, item_id, delta, mov_type, ref_type, ref_id, note=''):
    item = db.execute(
        "SELECT * FROM inventory_items WHERE id=%s AND user_id=%s", (item_id, uid)).fetchone()
    if not item:
        return False, 'Item not found'
    old_qty = float(item['qty'] or 0)
    delta = float(delta)
    new_qty = old_qty + delta
    if new_qty < -0.0001:
        return False, 'Not enough stock for ' + (item['name'] or 'item')
    db.execute(
        "UPDATE inventory_items SET qty=%s, updated_at=%s WHERE id=%s AND user_id=%s",
        (new_qty, _now_str(), item_id, uid))
    db.execute(
        '''INSERT INTO stock_movements
           (user_id,item_id,type,qty,ref_type,ref_id,note,created_at,qty_before,qty_after)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (uid, item_id, mov_type, delta, ref_type, ref_id, note or '', _now_str(), old_qty, new_qty))
    return True, new_qty

def _parse_dt(s):
    if not s: return None
    return datetime.fromisoformat(str(s)[:19]).replace(tzinfo=timezone.utc)

def _ist_date_str(s):
    dt = _parse_dt(s)
    if not dt:
        return str(s or '')[:10]
    return dt.astimezone(IST).strftime('%Y-%m-%d')

def _ist_hm(s):
    dt = _parse_dt(s)
    if not dt:
        return str(s or '')[11:16]
    return dt.astimezone(IST).strftime('%H:%M')

def _trial_end(trial_start):
    if not trial_start: return None
    ist_start = trial_start.astimezone(IST)
    end_day = (ist_start + timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return end_day.astimezone(timezone.utc)

def subscription_status(user):
    if not user['enabled']: return 'inactive'
    now = datetime.now(timezone.utc)
    trial_start = _parse_dt(user['trial_start'])
    sub_end = _parse_dt(user['subscription_end'])
    if sub_end and now < sub_end: return 'active'
    if trial_start and now < _trial_end(trial_start): return 'trial'
    if sub_end and now >= sub_end: return 'expired'
    if trial_start and now >= _trial_end(trial_start): return 'trial_expired'
    return 'trial'

def days_left(user):
    now = datetime.now(IST)
    sub_end = _parse_dt(user['subscription_end'])
    trial_start = _parse_dt(user['trial_start'])
    if sub_end:
        return max(0, (sub_end.astimezone(IST) - now).days)
    if trial_start:
        return max(0, (_trial_end(trial_start).astimezone(IST) - now).days)
    return 0

PLAN_TIERS = {
    'diamond': {'staff': 1, 'devices': 4, 'label': 'Diamond'},
    'platinum': {'staff': 2, 'devices': 5, 'label': 'Platinum'},
    'enterprise': {'staff': 3, 'devices': 10, 'label': 'Enterprise'},
}
PLAN_DURATIONS = {
    '1mo': {'days': 30, 'label': 'Monthly'},
    '30d': {'days': 30, 'label': '30 Days'},
    '1y': {'days': 365, 'label': '1 Year'},
    '2y': {'days': 730, 'label': '2 Years'},
    '3y': {'days': 1095, 'label': '3 Years'},
}
LEGACY_PLAN_TIER = {
    '30d': None,
    '1y': 'diamond',
    '2y': 'platinum',
    '3y': 'enterprise',
}

def parse_plan_code(plan):
    plan = (plan or '').strip().lower()
    if not plan or plan == 'trial':
        return None, None
    if '_' in plan:
        tier, dur = plan.split('_', 1)
        if tier in PLAN_TIERS and dur in PLAN_DURATIONS:
            return tier, dur
    if plan in PLAN_DURATIONS:
        return LEGACY_PLAN_TIER.get(plan), plan
    return None, None

def plan_spec(plan):
    tier, dur = parse_plan_code(plan)
    if not dur:
        return {'devices': 1, 'staff': 0, 'label': 'Trial', 'days': 0, 'tier': None, 'duration': None}
    d = PLAN_DURATIONS[dur]
    staff = int(PLAN_TIERS[tier]['staff']) if tier else 0
    devices = int(PLAN_TIERS[tier]['devices']) if tier else 1
    label = f"{PLAN_TIERS[tier]['label']} · {d['label']}" if tier else d['label']
    return {
        'devices': devices,
        'staff': staff,
        'label': label,
        'days': int(d['days']),
        'tier': tier,
        'duration': dur,
    }

def plan_display_name(plan):
    if not plan:
        return 'Free Trial'
    return plan_spec(plan)['label']

OWNER_ONLY_ENDPOINTS = {
    'settings', 'setup_2fa', 'verify_2fa_setup', 'disable_2fa',
    'reports', 'reports_print', 'inventory', 'inventory_item_log', 'print_inventory_log',
    'return_sale', 'delete_job', 'cancel_job_route', 'record_refund',
    'shop_team', 'shop_staff_add', 'shop_staff_update', 'shop_device_remove',
    'shop_staff_perf_print', 'shop_staff_perf_csv', 'shop_staff_perf_all_print', 'shop_staff_perf_all_csv',
    'shop_staff_perf_owner_print', 'shop_staff_perf_owner_csv',
    'shop_staff_reassign', 'shop_staff_alerts_seen', 'dashboard_hide_absent',
}

def plan_limits(user):
    st = subscription_status(user) if user else 'trial'
    if st == 'trial':
        base = {'devices': 1, 'staff': 0, 'label': 'Trial', 'tier': None, 'duration': None}
    else:
        base = dict(plan_spec(_rg(user, 'subscription_plan')))
    extra = 0
    try:
        extra = int(_rg(user, 'extra_staff') or 0)
    except (TypeError, ValueError):
        extra = 0
    plan_staff = int(base.get('staff') or 0)
    base['plan_staff'] = plan_staff
    base['extra_staff'] = extra
    base['staff'] = max(0, min(20, plan_staff + extra))
    # Extra device slots granted by admin
    extra_dev = 0
    try:
        extra_dev = int(_rg(user, 'extra_devices') or 0)
    except (TypeError, ValueError):
        extra_dev = 0
    plan_devices = int(base.get('devices') or 1)
    base['plan_devices'] = plan_devices
    base['extra_devices'] = extra_dev
    base['devices'] = max(1, min(20, plan_devices + extra_dev))
    return base

def is_shop_owner():
    return session.get('role') != 'admin' and session.get('shop_role') != 'salesperson'

def _device_token_from_request():
    raw = (request.form.get('device_token') or request.cookies.get('mfp_device') or '').strip().lower()
    if re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', raw):
        return raw
    return ''

def _device_label(ua):
    ua = ua or ''
    if 'iPhone' in ua: return 'iPhone'
    if 'iPad' in ua: return 'iPad'
    if 'Android' in ua: return 'Android'
    if 'Windows' in ua: return 'Windows PC'
    if 'Mac OS' in ua or 'Macintosh' in ua: return 'Mac'
    if 'Linux' in ua: return 'Linux'
    return 'Browser'

def _claim_device(db, owner, token, staff_id=None):
    if not token:
        return 'This browser could not be identified. Allow cookies and try again.'
    limits = plan_limits(owner)
    cap = int(limits['devices'] or 1)
    ua = (request.headers.get('User-Agent') or '')[:300]
    ip = (request.headers.get('X-Forwarded-For') or request.remote_addr or '')[:80]
    now = _now_str()
    row = db.execute(
        "SELECT id FROM shop_devices WHERE owner_id=%s AND token=%s",
        (owner['id'], token)).fetchone()
    rows = db.execute(
        "SELECT id, token FROM shop_devices WHERE owner_id=%s ORDER BY last_seen DESC NULLS LAST, id DESC"
        if USE_PG else
        "SELECT id, token FROM shop_devices WHERE owner_id=%s ORDER BY last_seen DESC, id DESC",
        (owner['id'],)).fetchall()
    allowed = {r['token'] for r in rows[:cap]}
    if row:
        if token not in allowed and len(rows) > cap:
            return (
                f'This shop is over the {cap} device limit on the {limits["label"]} plan. '
                'Remove an unused device in Team, or upgrade.'
            )
        db.execute(
            "UPDATE shop_devices SET last_seen=%s,user_agent=%s,ip_address=%s,staff_id=%s,label=%s WHERE id=%s",
            (now, ua, ip, staff_id, _device_label(ua), row['id']))
        return None
    if len(rows) >= cap:
        return (
            f'This shop already uses {len(rows)} of {cap} login devices on the {limits["label"]} plan. '
            'Ask the owner to remove a device in Team, or upgrade.'
        )
    db.execute(
        '''INSERT INTO shop_devices (owner_id,token,label,user_agent,ip_address,staff_id,last_seen,created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
        (owner['id'], token, _device_label(ua), ua, ip, staff_id, now, now))
    if staff_id:
        nm = db.execute("SELECT name FROM shop_staff WHERE id=%s", (staff_id,)).fetchone()
        sname = (nm['name'] if nm else 'SALES')
        try:
            db.execute(
                '''INSERT INTO shop_login_alerts (owner_id,staff_id,staff_name,device_label,ip_address,seen,created_at)
                   VALUES (%s,%s,%s,%s,%s,0,%s)''',
                (owner['id'], staff_id, sname, _device_label(ua), ip, now))
            log_staff(db, owner['id'], 'login_device', 'device', None,
                      f'New device {_device_label(ua)}', staff_id, sname)
        except Exception:
            pass
    return None

def _staff_within_plan(db, owner, staff):
    lim = int(plan_limits(owner)['staff'] or 0)
    if lim <= 0:
        return False
    ids = [r['id'] for r in db.execute(
        "SELECT id FROM shop_staff WHERE owner_id=%s AND enabled=1 ORDER BY id",
        (owner['id'],)).fetchall()]
    return staff['id'] in ids[:lim]

def _apply_shop_session(owner, staff=None, device_token=''):
    session.permanent = True
    session.pop('pending_2fa_uid', None)
    session.pop('pending_device_token', None)
    session['user_id'] = owner['id']
    session['role'] = owner['role'] or 'user'
    session['shop_name'] = owner['shop_name'] or 'My Shop'
    session['device_token'] = device_token
    if staff:
        session['shop_role'] = 'salesperson'
        session['staff_id'] = staff['id']
        session['staff_name'] = (staff['name'] or 'SALES').strip().upper()
        flags = staff_flags(staff)
        session['can_sale'] = flags['can_sale']
        session['can_collect'] = flags['can_collect']
        session['can_jobs'] = flags['can_jobs']
        session['staff_kind'] = flags['job_kind']
    else:
        session['shop_role'] = 'owner'
        session['staff_id'] = None
        session['staff_name'] = 'OWNER'
        session['can_sale'] = 1
        session['can_collect'] = 1
        session['can_jobs'] = 1
        session['staff_kind'] = 'owner'

def _login_redirect(owner):
    dest = url_for('admin_dashboard') if owner['role'] == 'admin' else url_for('dashboard')
    resp = redirect(dest)
    token = session.get('device_token')
    if token and owner['role'] != 'admin':
        resp.set_cookie('mfp_device', token, max_age=60 * 60 * 24 * 400, samesite='Lax')
    return resp

def _owner_denied():
    if request.is_json or request.headers.get('X-Requested-With') or request.method == 'POST':
        return jsonify({'error': 'Only the shop owner can do this'}), 403
    flash('Only the shop owner can do this.', 'error')
    return redirect(url_for('dashboard'))

def _staff_perm_denied(what):
    msg = f'The shop owner has not allowed {what} for this login.'
    if request.is_json or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return jsonify({'error': msg}), 403
    flash(msg, 'error')
    return redirect(url_for('dashboard'))

# ── Decorators ─────────────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session: return redirect(url_for('login'))
        db = get_db()
        user = db.execute("SELECT id FROM users WHERE id=%s", (session['user_id'],)).fetchone()
        db.close()
        if not user:
            session.clear()
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('role') != 'admin':
            if request.method == 'POST' or request.headers.get('X-Requested-With') or request.is_json:
                return jsonify({'error': 'Unauthorized'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def active_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            if request.is_json or request.headers.get('X-Requested-With'):
                return jsonify({'error': 'Not logged in'}), 401
            return redirect(url_for('login'))
        if session.get('role') == 'admin': return f(*args, **kwargs)
        if session.get('impersonator_id'):
            return f(*args, **kwargs)
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
        db.close()
        if not user:
            session.clear()
            if request.is_json or request.headers.get('X-Requested-With'):
                return jsonify({'error': 'Session expired'}), 401
            return redirect(url_for('login'))
        status = subscription_status(user)
        if status in ('inactive', 'trial_expired', 'expired'):
            if request.is_json or request.headers.get('X-Requested-With'):
                return jsonify({'error': 'Subscription expired'}), 403
            reason = 'disabled' if status == 'inactive' else status
            return redirect(url_for('subscription_page', reason=reason))
        return f(*args, **kwargs)
    return decorated

# ── Routes ─────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('admin_dashboard') if session.get('role') == 'admin' else url_for('dashboard'))
    return redirect(url_for('login'))

def _device_from_ua(ua):
    u = (ua or '')
    ul = u.lower()
    if 'ipad' in ul or ('tablet' in ul) or ('android' in ul and 'mobile' not in ul):
        kind = 'Tablet'
    elif 'mobile' in ul or 'iphone' in ul or 'android' in ul:
        kind = 'Mobile'
    else:
        kind = 'Desktop'
    if 'edg/' in ul or 'edge/' in ul:
        browser = 'Edge'
    elif 'opr/' in ul or 'opera' in ul:
        browser = 'Opera'
    elif 'chrome' in ul and 'edg' not in ul:
        browser = 'Chrome'
    elif 'firefox' in ul:
        browser = 'Firefox'
    elif 'safari' in ul and 'chrome' not in ul:
        browser = 'Safari'
    else:
        browser = 'Browser'
    return f'{kind} · {browser}'

_GEO_CACHE = {}

def _ip_is_local(ip):
    ip = (ip or '').strip()
    if not ip or ip in ('unknown', '::1', '127.0.0.1'):
        return True
    if ip.startswith('10.') or ip.startswith('192.168.') or ip.startswith('172.'):
        return True
    if ip.startswith('fe80:') or ip.startswith('fc') or ip.startswith('fd'):
        return True
    return False

def _lookup_location(ip):
    ip = (ip or '').strip()
    if _ip_is_local(ip):
        return 'Local / LAN'
    header_cc = (request.headers.get('CF-IPCountry') or request.headers.get('X-AppEngine-Country') or '').strip()
    if ip in _GEO_CACHE:
        return _GEO_CACHE[ip]
    loc = ''
    try:
        url = f'http://ip-api.com/json/{ip}?fields=status,city,regionName,country,countryCode'
        with urllib.request.urlopen(url, timeout=1.5) as resp:
            data = json.loads(resp.read().decode('utf-8', 'ignore') or '{}')
        if data.get('status') == 'success':
            parts = [p for p in [data.get('city'), data.get('regionName'), data.get('countryCode') or data.get('country')] if p]
            loc = ', '.join(dict.fromkeys(parts))
    except Exception:
        loc = ''
    if not loc and header_cc and header_cc.upper() not in ('XX', 'T1'):
        loc = header_cc.upper()
    loc = loc or 'Unknown'
    _GEO_CACHE[ip] = loc
    if len(_GEO_CACHE) > 400:
        _GEO_CACHE.clear()
    return loc

def _log_login(db, user_id, identifier, status, reason=None):
    ip = (request.headers.get('X-Forwarded-For') or request.remote_addr or 'unknown').split(',')[0].strip()
    ua = (request.headers.get('User-Agent') or '')[:300]
    now_ist = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    device = _device_from_ua(ua)
    location = _lookup_location(ip)
    if status == 'success':
        reason = reason or ''
    elif not reason:
        reason = {
            'failed': 'Wrong phone/email or password',
            'blocked': 'Account disabled',
            'device_limit': 'Device limit reached',
            'staff_limit': 'Sales person limit exceeded',
        }.get(status, status)
    db.execute(
        '''INSERT INTO login_logs
           (user_id, identifier, ip_address, user_agent, status, created_at, fail_reason, device_type, location)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (user_id, identifier, ip, ua, status, now_ist, reason or None, device, location)
    )
    db.commit()

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        identifier = request.form.get('identifier', '').strip()
        password = request.form.get('password', '')
        device_token = _device_token_from_request()
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE phone=%s OR email=%s", (identifier, identifier)).fetchone()
        if user and verify_pw(user['password'], password):
            if not user['enabled'] and user['role'] != 'admin':
                _log_login(db, user['id'], identifier, 'blocked', 'Shop account disabled')
                db.close()
                flash('Your account has been disabled. Contact support.', 'error')
                return render_template('login.html')
            if user['role'] != 'admin' and not user['totp_enabled']:
                err = _claim_device(db, user, device_token, None)
                if err:
                    _log_login(db, user['id'], identifier, 'device_limit', 'Device limit reached')
                    db.commit()
                    db.close()
                    flash(err, 'error')
                    return render_template('login.html')
            _upgrade_pw_if_needed(db, 'users', user['id'], user['password'], password)
            _log_login(db, user['id'], identifier, 'success')
            db.commit()
            if user['totp_enabled']:
                session['pending_2fa_uid'] = user['id']
                session['pending_device_token'] = device_token
                db.close()
                return redirect(url_for('login_2fa'))
            _apply_shop_session(user, None, device_token)
            db.close()
            return _login_redirect(user)
        staff = db.execute(
            "SELECT * FROM shop_staff WHERE phone=%s OR LOWER(COALESCE(email,''))=%s",
            (identifier, identifier.lower())).fetchone()
        if staff and verify_pw(staff['password'], password):
            owner = db.execute("SELECT * FROM users WHERE id=%s", (staff['owner_id'],)).fetchone()
            if not owner or not owner['enabled']:
                _log_login(db, staff['owner_id'], identifier, 'blocked', 'Shop account disabled')
                db.close()
                flash('This shop account is disabled. Contact support.', 'error')
                return render_template('login.html')
            if int(staff['enabled'] or 0) != 1:
                _log_login(db, owner['id'], identifier, 'blocked', 'Sales login disabled')
                db.close()
                flash('This sales login is disabled. Ask the shop owner.', 'error')
                return render_template('login.html')
            if not _staff_within_plan(db, owner, staff):
                lim = plan_limits(owner)
                _log_login(db, owner['id'], identifier, 'staff_limit', 'Sales person limit exceeded')
                db.commit()
                db.close()
                flash(
                    f'The {lim["label"]} plan allows {lim["staff"]} sales person login'
                    f'{"s" if lim["staff"] != 1 else ""}. Ask the owner to upgrade or disable extra logins.',
                    'error')
                return render_template('login.html')
            err = _claim_device(db, owner, device_token, staff['id'])
            if err:
                _log_login(db, owner['id'], identifier, 'device_limit', 'Device limit reached')
                db.commit()
                db.close()
                flash(err, 'error')
                return render_template('login.html')
            _upgrade_pw_if_needed(db, 'shop_staff', staff['id'], staff['password'], password)
            _log_login(db, owner['id'], identifier, 'success')
            db.commit()
            _apply_shop_session(owner, staff, device_token)
            db.close()
            return _login_redirect(owner)
        uid = user['id'] if user else (staff['owner_id'] if staff else None)
        if user:
            fail_why = 'Wrong password'
        elif staff:
            fail_why = 'Wrong password'
        else:
            fail_why = 'Unknown phone/email'
        _log_login(db, uid, identifier, 'failed', fail_why)
        db.close()
        flash('Invalid phone/email or password.', 'error')
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        phone = request.form.get('phone', '').strip()
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        shop_name = request.form.get('shop_name', '').strip().upper()
        door_no   = request.form.get('door_no', '').strip().upper()
        street    = request.form.get('street', '').strip().upper()
        city      = request.form.get('city', '').strip().upper()
        pincode   = request.form.get('pincode', '').strip().upper()
        addr_parts = [p for p in [door_no, street] if p]
        addr_line1 = ', '.join(addr_parts)
        addr_line2 = city + (' - ' + pincode if pincode else '')
        address = '\n'.join([l for l in [addr_line1, addr_line2] if l])
        if not re.match(r'^\d{10}$', phone):
            flash('Phone must be exactly 10 digits.', 'error'); return render_template('register.html')
        if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
            flash('Invalid email address.', 'error'); return render_template('register.html')
        if len(password) < 6:
            flash('Password must be at least 6 characters.', 'error'); return render_template('register.html')
        db = get_db()
        if db.execute("SELECT id FROM users WHERE phone=%s OR email=%s", (phone, email)).fetchone():
            db.close()
            flash('Phone or email already registered.', 'error'); return render_template('register.html')
        db.execute('INSERT INTO users (phone,email,password,shop_name,address,door_no,street,city,pincode,trial_start) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                   (phone, email, hash_pw(password), shop_name, address, door_no, street, city, pincode, _now_str()))
        db.commit()
        new_user = db.execute("SELECT * FROM users WHERE phone=%s", (phone,)).fetchone()
        token = _device_token_from_request()
        if token:
            _claim_device(db, new_user, token, None)
            db.commit()
        db.close()
        _apply_shop_session(new_user, None, token)
        flash('Welcome! Your 30-day free trial has started. Owner login: 1 device, owner only.', 'success')
        return _login_redirect(new_user)
    return render_template('register.html')

@app.route('/logout')
def logout():
    device_token = session.get('device_token')
    if device_token and not session.get('impersonator_id'):
        db = get_db()
        db.execute("DELETE FROM shop_devices WHERE token=%s", (device_token,))
        db.commit()
        db.close()
    session.clear()
    return redirect(url_for('login'))

@app.route('/subscription')
@login_required
def subscription_page():
    reason = request.args.get('reason', '')
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    db.close()
    return render_template('subscription.html', user=user, status=subscription_status(user),
                           days_left=days_left(user), reason=reason)

@app.route('/dashboard')
@active_required
def dashboard():
    db = get_db()
    uid = session['user_id']
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    attendance_today = None
    hide_absent_today = False
    if session.get('shop_role') != 'salesperson':
        attendance_today = staff_attendance_today(db, uid, user=dict(user))
        hide_absent_today = str(_rg(user, 'absent_alert_hide_date') or '')[:10] == _today_str()
    db.close()
    return render_template('dashboard.html', user=user, status=subscription_status(user),
                           days_left=days_left(user), attendance_today=attendance_today,
                           hide_absent_today=hide_absent_today)

@app.route('/dashboard/hide-absent', methods=['POST'])
@login_required
@active_required
def dashboard_hide_absent():
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    db = get_db()
    db.execute(
        "UPDATE users SET absent_alert_hide_date=%s WHERE id=%s",
        (_today_str(), session['user_id']))
    db.commit()
    db.close()
    return redirect(url_for('dashboard'))

@app.route('/profile')
@active_required
def shop_profile():
    if session.get('shop_role') != 'salesperson' or not session.get('staff_id'):
        flash('Profile is for sales persons.', 'error')
        return redirect(url_for('dashboard'))
    db = get_db()
    uid = session['user_id']
    sid = int(session['staff_id'])
    today = _today_str()
    y, m = date.today().year, date.today().month
    from_d, to_d = month_span(y, m)
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    auto_close_overdue_shifts(db, uid, user)
    staff = db.execute("SELECT * FROM shop_staff WHERE id=%s AND owner_id=%s", (sid, uid)).fetchone()
    if not staff:
        db.close()
        flash('Profile not found.', 'error')
        return redirect(url_for('dashboard'))
    flags = staff_flags(staff)
    open_shift = db.execute(
        "SELECT id,clock_in FROM shop_shifts WHERE owner_id=%s AND staff_id=%s AND clock_out IS NULL "
        "ORDER BY id DESC LIMIT 1", (uid, sid)).fetchone()
    open_lunch = staff_open_lunch(db, uid, sid)
    lunch_today = staff_lunch_breaks(db, uid, sid, day=today)
    collected = "COALESCE(paid_amount, CASE WHEN paid_status='Paid' THEN total ELSE 0 END)"
    today_row = db.execute(
        f"SELECT COUNT(*) AS n, COALESCE(SUM({collected}),0) AS amt FROM sales_bills "
        f"WHERE user_id=%s AND staff_id=%s AND paid_status!='Returned' AND {sql_date('created_at')}=%s",
        (uid, sid, today)).fetchone()
    today_jobs = db.execute(
        f"SELECT COUNT(*) AS n FROM repair_jobs WHERE user_id=%s AND staff_id=%s AND {sql_date('created_at')}=%s",
        (uid, sid, today)).fetchone()
    pay = {}
    attach_salary_to_sheet(db, uid, sid, from_d, to_d, pay, staff)
    db.close()
    return render_template(
        'profile.html', user=user, staff=dict(staff), flags=flags,
        status=subscription_status(user), days_left=days_left(user),
        open_shift=dict(open_shift) if open_shift else None,
        open_lunch=open_lunch,
        lunch_today=lunch_today,
        pay=pay, today=today, perf_label=date(y, m, 1).strftime('%B %Y'),
        today_sales_n=int(today_row['n'] or 0) if today_row else 0,
        today_sales_amt=float(today_row['amt'] or 0) if today_row else 0,
        today_jobs_n=int(today_jobs['n'] or 0) if today_jobs else 0)

@app.route('/attendance')
@active_required
def shop_attendance():
    if session.get('shop_role') != 'salesperson' or not session.get('staff_id'):
        flash('Attendance is for sales persons.', 'error')
        return redirect(url_for('dashboard'))
    db = get_db()
    uid = session['user_id']
    sid = int(session['staff_id'])
    today = _today_str()
    y, m = date.today().year, date.today().month
    from_d, to_d = month_span(y, m)
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    auto_close_overdue_shifts(db, uid, user)
    open_shift = db.execute(
        "SELECT id,clock_in,in_lat,in_lng FROM shop_shifts WHERE owner_id=%s AND staff_id=%s AND clock_out IS NULL "
        "ORDER BY id DESC LIMIT 1", (uid, sid)).fetchone()
    open_lunch = staff_open_lunch(db, uid, sid)
    lunch_today = staff_lunch_breaks(db, uid, sid, day=today)
    staff = db.execute(
        "SELECT * FROM shop_staff WHERE id=%s AND owner_id=%s", (sid, uid)).fetchone()
    rows = db.execute(
        f"SELECT id,clock_in,clock_out,in_lat,in_lng,out_lat,out_lng,out_source,"
        f" CASE WHEN in_photo IS NOT NULL AND in_photo!='' THEN 1 ELSE 0 END AS has_in_photo,"
        f" CASE WHEN out_photo IS NOT NULL AND out_photo!='' THEN 1 ELSE 0 END AS has_out_photo"
        f" FROM shop_shifts WHERE owner_id=%s AND staff_id=%s "
        f"AND {sql_date('clock_in')}>=%s AND {sql_date('clock_in')}<=%s ORDER BY id DESC",
        (uid, sid, from_d, to_d)).fetchall()
    pay = {'salary_monthly': 0, 'present_days': 0, 'hours_worked': 0, 'salary_due': 0, 'daily_rate': 0, 'salary_days': 26}
    if staff:
        sheet = {}
        attach_salary_to_sheet(db, uid, sid, from_d, to_d, sheet, staff)
        pay = sheet
    db.close()
    return render_template(
        'attendance.html', user=user, status=subscription_status(user), days_left=days_left(user),
        open_shift=dict(open_shift) if open_shift else None,
        open_lunch=open_lunch,
        lunch_today=lunch_today,
        server_now=_now_str(), today=today,
        shifts=[dict(r) for r in rows], pay=pay)

@app.route('/team/shift-photo/<int:shift_id>')
@login_required
@active_required
def shop_shift_photo(shift_id):
    db = get_db()
    uid = session['user_id']
    row = db.execute(
        "SELECT id,staff_id,in_photo,out_photo FROM shop_shifts WHERE id=%s AND owner_id=%s",
        (shift_id, uid)).fetchone()
    db.close()
    if not row:
        return jsonify(ok=False, error='Not found'), 404
    if session.get('shop_role') == 'salesperson':
        if int(row['staff_id'] or 0) != int(session.get('staff_id') or 0):
            return jsonify(ok=False, error='Not allowed'), 403
    which = (request.args.get('which') or 'in').strip().lower()
    photo = row['out_photo'] if which == 'out' else row['in_photo']
    if not photo:
        return jsonify(ok=False, error='No photo'), 404
    return jsonify(ok=True, photo=photo)

@app.route('/service')
@active_required
def service_hub():
    db = get_db()
    uid = session['user_id']
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    job_count = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=%s", (uid,)).fetchone()[0]
    pending = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=%s AND status NOT IN ('Delivered','Cancelled')", (uid,)).fetchone()[0]
    delivered = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=%s AND status='Delivered'", (uid,)).fetchone()[0]
    recent_jobs = db.execute("SELECT * FROM repair_jobs WHERE user_id=%s ORDER BY created_at DESC LIMIT 6", (uid,)).fetchall()
    db.close()
    return render_template(
        'service.html', user=user, status=subscription_status(user), days_left=days_left(user),
        job_count=job_count, pending=pending, delivered=delivered, recent_jobs=recent_jobs)

@app.route('/jobs')
@active_required
def jobs():
    db = get_db()
    uid = session['user_id']
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    all_jobs = db.execute("SELECT * FROM repair_jobs WHERE user_id=%s ORDER BY created_at DESC", (uid,)).fetchall()
    all_invs = db.execute(
        "SELECT id, job_id, due_date, total, advance_amount FROM invoices WHERE user_id=%s", (uid,)).fetchall()
    db.close()
    inv_by_job = {r['job_id']: r for r in all_invs}
    jobs_list = []
    for j in all_jobs:
        d = dict(j)
        inv = inv_by_job.get(j['id'])
        d['inv_id'] = inv['id'] if inv else None
        d['inv_due_date'] = inv['due_date'] if inv else None
        d['inv_balance'] = (max(0.0, float(inv['total'] or 0) - float(inv['advance_amount'] or 0)) if inv else 0)
        jobs_list.append(d)
    today = _today_str()
    overdue_jobs = [j for j in jobs_list if j.get('expected_return') and j['expected_return'] < today
                    and j['status'] not in ('Delivered', 'Cancelled')]
    partial_jobs = [j for j in jobs_list if j['status'] == 'Delivered' and float(j.get('inv_balance') or 0) > 0.01]
    return render_template('jobs.html', jobs=jobs_list, jobs_json=json.dumps(jobs_list),
                           user=user, status=subscription_status(user), days_left=days_left(user),
                           overdue_jobs=overdue_jobs, partial_jobs=partial_jobs,
                           imei_skip=int(user['imei_skip'] or 0))

@app.route('/jobs/add', methods=['GET', 'POST'])
@active_required
def add_job():
    if request.method == 'POST':
        db = get_db()
        adv  = float(request.form.get('advance_amount') or 0)
        cost = float(request.form.get('cost') or 0)
        paid_status = 'Paid' if adv > 0 and cost > 0 and adv >= cost else ('Partial' if adv > 0 else 'Unpaid')
        happy_code = generate_happy_code()
        er = (request.form.get('expected_return') or '').strip()[:10]
        book = datetime.now(IST).date()
        try:
            er_d = datetime.strptime(er, '%Y-%m-%d').date()
        except ValueError:
            db.close()
            flash('Please select a valid expected return date.', 'error')
            return redirect(url_for('add_job'))
        if er_d < book or er_d > book + timedelta(days=15):
            db.close()
            flash('Expected return must be within 15 days of the booking date.', 'error')
            return redirect(url_for('add_job'))
        aadhar = clean_aadhaar(request.form.get('aadhar_number', ''))
        if aadhar and not valid_aadhaar(aadhar):
            db.close()
            flash('Aadhaar number must be 12 digits if entered.', 'error')
            return redirect(url_for('add_job'))
        staff_id, staff_name = session_staff_stamp()
        db.execute('''INSERT INTO repair_jobs
                      (user_id,customer_name,customer_phone,device_model,imei,imei_billing,
                       issue,aadhar_number,received_without,cost,notes,advance_amount,advance_method,
                       paid_status,expected_return,happy_code,staff_id,staff_name,created_at,updated_at)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                   (session['user_id'],
                    request.form.get('customer_name', '').upper(),
                    request.form.get('customer_phone', ''),
                    (request.form.get('device_model', '') or
                     (request.form.get('device_brand', '') + ' ' + request.form.get('device_model_only', '')).strip()).upper(),
                    clean_imei_serial(request.form.get('imei', '')),
                    clean_imei_serial(request.form.get('imei_billing', '')),
                    request.form.get('issue', '').upper(),
                    aadhar,
                    request.form.get('received_without_val', ''),
                    cost, request.form.get('notes', '').upper(),
                    adv, request.form.get('advance_method', ''), paid_status,
                    er,
                    happy_code, staff_id, staff_name, _now_str(), _now_str()))
        log_staff(db, session['user_id'], 'add_job', 'job', None,
                  (request.form.get('customer_name') or '')[:80], staff_id, staff_name)
        job_row = db.execute("SELECT id FROM repair_jobs WHERE happy_code=%s AND user_id=%s ORDER BY id DESC LIMIT 1", (happy_code, session['user_id'])).fetchone()
        job_id = job_row['id'] if job_row else None
        db.commit(); db.close()
        flash('Repair job added successfully!', 'success')
        if job_id:
            return redirect(url_for('jobs', print_barcode=job_id))
        return redirect(url_for('jobs'))
    db = get_db()
    user = db.execute("SELECT imei_skip FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    imei_skip = int(user['imei_skip'] or 0) if user else 0
    db.close()
    return render_template('add_job.html', imei_skip=imei_skip)

@app.route('/jobs/<int:job_id>/barcode')
@active_required
def print_barcode(job_id):
    db = get_db()
    uid = session['user_id']
    job = db.execute("SELECT * FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, uid)).fetchone()
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    db.close()
    if not job:
        flash('Job not found.', 'error')
        return redirect(url_for('jobs'))
    return render_template('print_barcode.html', job=job, user=user)

@app.route('/jobs/<int:job_id>/print-card')
@active_required
def print_job_card(job_id):
    db = get_db()
    uid = session['user_id']
    job = db.execute("SELECT * FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, uid)).fetchone()
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    if not job:
        db.close()
        flash('Job not found.', 'error')
        return redirect(url_for('jobs'))
    if (job['status'] or '') in ('Delivered', 'Cancelled'):
        db.close()
        flash('Job card print is not available for delivered or cancelled jobs.', 'error')
        return redirect(url_for('jobs'))
    db.close()
    return render_template('print_job_card.html', job=job, user=user)

@app.route('/jobs/<int:job_id>/update', methods=['POST'])
@active_required
def update_job(job_id):
    db = get_db()
    adv_str = request.form.get('advance_amount')
    new_adv = float(adv_str or 0) if adv_str is not None else 0
    existing = db.execute("SELECT advance_amount, advance_history, status, diagnosis_history FROM repair_jobs WHERE id=%s AND user_id=%s",
                          (job_id, session['user_id'])).fetchone()
    has_cost = 'cost' in request.form
    _ts = sql_now()
    new_status = request.form.get('status')
    set_parts = ['status=%s', f'diagnosed_at=COALESCE(diagnosed_at,{_ts})', f'updated_at={_ts}']
    params = [new_status]
    
    if has_cost:
        new_cost = float(request.form.get('cost') or 0)
        set_parts.insert(1, 'cost=%s')
        set_parts.insert(2, 'notes=%s')
        set_parts.insert(3, 'quote_items=%s')
        params.insert(1, new_cost)
        params.insert(2, request.form.get('notes', ''))
        params.insert(3, request.form.get('quote_items', ''))
        if adv_str is None or new_adv == 0:
            row = db.execute("SELECT advance_amount FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, session['user_id'])).fetchone()
            cur_adv = float(row['advance_amount'] or 0) if row else 0
            recalc_paid = 'Paid' if new_cost > 0 and cur_adv >= new_cost else ('Partial' if cur_adv > 0 else 'Unpaid')
            set_parts.insert(4, 'paid_status=%s')
            params.insert(4, recalc_paid)
            
    if existing and existing['status'] != new_status:
        diag_hist = json.loads(existing['diagnosis_history'] or '[]')
        diag_hist.append({'status': new_status, 'time': datetime.now(IST).strftime('%Y-%m-%d %I:%M %p')})
        set_parts.append('diagnosis_history=%s')
        params.append(json.dumps(diag_hist))
    if adv_str is not None and new_adv > 0:
        adv_method = request.form.get('advance_method', '')
        old_total = float(existing['advance_amount'] or 0) if existing else 0
        history = json.loads(existing['advance_history'] or '[]') if existing else []
        history.append({'amount': new_adv, 'method': adv_method,
                        'date': datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'), 'note': 'Advance'})
        total_adv = old_total + new_adv
        cost_val = float(request.form.get('cost') or 0) if has_cost else None
        if cost_val is None:
            row = db.execute("SELECT cost FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, session['user_id'])).fetchone()
            cost_val = float(row['cost'] or 0) if row and row['cost'] else 0
        new_paid = 'Paid' if cost_val > 0 and total_adv >= cost_val else ('Partial' if total_adv > 0 else 'Unpaid')
        set_parts.insert(-2, 'advance_amount=%s')
        set_parts.insert(-2, 'advance_method=%s')
        set_parts.insert(-2, 'advance_history=%s')
        set_parts.insert(-2, 'paid_status=%s')
        params += [total_adv, adv_method, json.dumps(history), new_paid]
    reason = request.form.get('cancel_reason')
    if reason is not None:
        set_parts.append('cancel_reason=%s')
        params.append(reason)
    params += [job_id, session['user_id']]
    try:
        db.execute(f"UPDATE repair_jobs SET {', '.join(set_parts)} WHERE id=%s AND user_id=%s", tuple(params))
        db.commit()
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})
    finally:
        db.close()
    return jsonify({'ok': True})

@app.route('/jobs/<int:job_id>/advance', methods=['POST'])
@active_required
def add_advance_payment(job_id):
    db = get_db()
    new_adv = float(request.form.get('advance_amount') or 0)
    adv_method = request.form.get('advance_method', 'Cash')
    
    if new_adv <= 0:
        db.close()
        return jsonify({'ok': False, 'error': 'Invalid advance amount'})
        
    existing = db.execute("SELECT cost, advance_amount, advance_history FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, session['user_id'])).fetchone()
    if not existing:
        db.close()
        return jsonify({'ok': False, 'error': 'Job not found'}), 404
        
    old_total = float(existing['advance_amount'] or 0)
    history = json.loads(existing['advance_history'] or '[]') if existing['advance_history'] else []
    history.append({
        'amount': new_adv,
        'method': adv_method,
        'date': datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
        'note': 'Advance'
    })
    
    total_adv = old_total + new_adv
    cost = float(existing['cost'] or 0)
    new_paid = 'Paid' if cost > 0 and total_adv >= cost else ('Partial' if total_adv > 0 else 'Unpaid')
    
    db.execute("UPDATE repair_jobs SET advance_amount=%s, advance_method=%s, advance_history=%s, paid_status=%s WHERE id=%s AND user_id=%s",
               (total_adv, adv_method, json.dumps(history), new_paid, job_id, session['user_id']))
    db.commit()
    db.close()
    return jsonify({'ok': True})

@app.route('/jobs/<int:job_id>/advance/print')
@active_required
def print_advance(job_id):
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    job = db.execute("SELECT * FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, session['user_id'])).fetchone()
    db.close()
    if not job:
        flash('Job not found.', 'error')
        return redirect(url_for('jobs'))
        
    # Extract advance history to show on receipt
    adv_history = parse_pay_hist(job['advance_history']) if job and job['advance_history'] else []
    
    return render_template('print_advance.html', user=user, job=job, adv_history=adv_history)

@app.route('/jobs/<int:job_id>/verify_happy', methods=['POST'])
@active_required
def verify_happy_code(job_id):
    code = request.json.get('code', '').strip()
    db = get_db()
    job = db.execute("SELECT happy_code,status FROM repair_jobs WHERE id=%s AND user_id=%s",
                     (job_id, session['user_id'])).fetchone()
    db.close()
    if not job: return jsonify({'ok': False, 'error': 'Job not found'}), 404
    if job['happy_code'] == code: return jsonify({'ok': True})
    return jsonify({'ok': False, 'error': 'Invalid Happy Code'})

@app.route('/jobs/<int:job_id>/set_reminder', methods=['GET', 'POST'])
@active_required
def set_reminder(job_id):
    db = get_db()
    if request.method == 'POST':
        reminder_date = request.form.get('reminder_date', '')
        db.execute("UPDATE repair_jobs SET reminder_date=%s WHERE id=%s AND user_id=%s",
                   (reminder_date, job_id, session['user_id']))
        db.commit(); db.close()
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'ok': True})
        flash('Reminder set!', 'success')
        return redirect(url_for('jobs'))
    job = db.execute("SELECT * FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, session['user_id'])).fetchone()
    db.close()
    if not job: return redirect(url_for('jobs'))
    return render_template('set_reminder.html', job=job)

@app.route('/jobs/<int:job_id>/deliver', methods=['POST'])
@active_required
def deliver_job(job_id):
    db = get_db()
    job = db.execute("SELECT * FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, session['user_id'])).fetchone()
    if not job:
        db.close()
        return jsonify({'error': 'Job not found'}), 404
    has_quote = job['quote_items'] and job['quote_items'] not in ('null', '[]', '')
    has_cost  = float(job['cost'] or 0) > 0
    if not has_quote and not has_cost:
        db.close()
        return jsonify({'error': 'Please diagnose and quote this job before delivering.'}), 400
    data = request.get_json(force=True)
    aadhar_in = clean_aadhaar(data.get('aadhar_number') or data.get('aadhar') or '')
    stored_aadhar = clean_aadhaar(job['aadhar_number'] if job['aadhar_number'] else '')
    aadhar = aadhar_in or stored_aadhar
    if aadhar_in and not valid_aadhaar(aadhar_in):
        db.close()
        return jsonify({'error': 'Aadhaar Card Number must be 12 digits.'}), 400
    if job_needs_aadhaar(job) and not valid_aadhaar(aadhar):
        db.close()
        return jsonify({'error': 'Aadhaar Card Number (12 digits) is mandatory before delivery and invoicing for FRP / Flash jobs.'}), 400
    total          = float(data.get('total', 0))
    advance        = float(data.get('advance', 0))
    amount_paid_now= float(data.get('amountPaidNow', 0))
    discount       = float(data.get('discount', 0))
    redeem_pts     = int(data.get('redeem_points') or 0)
    uid = session['user_id']
    loy_pts, loy_rs = 0, 0.0
    if redeem_pts:
        red, err = loyalty_redeem(db, uid, job['customer_phone'], 'service', redeem_pts, 'job', job_id)
        if err:
            db.close()
            return jsonify({'error': err}), 400
        loy_pts, loy_rs = red['points'], red['rupees']
        total = max(0.0, round(total - loy_rs, 2))
        discount = round(discount + loy_rs, 2)
    pay_method     = data.get('payMethod', 'Cash')
    credit_due_date= (data.get('creditDueDate') or '')[:10] or None
    items_str      = json.dumps(data.get('items', []))
    total_collected= advance + amount_paid_now
    balance        = max(0, total - total_collected)
    paid = 'Paid' if balance < 0.01 else ('Partial' if total_collected > 0 else 'Unpaid')
    if balance > 0.01 and not valid_credit_due_date(credit_due_date):
        db.rollback()
        db.close()
        return jsonify({'error': 'Due date must be within 15 days from today.'}), 400
    delivery_date  = datetime.now(IST).strftime('%Y-%m-%d')
    imei = clean_imei_serial(data.get('imei', ''))
    if imei:
        db.execute("UPDATE repair_jobs SET imei_billing=%s WHERE id=%s AND user_id=%s AND (imei_billing IS NULL OR imei_billing='')",
                   (imei, job_id, session['user_id']))
    if aadhar_in and valid_aadhaar(aadhar_in):
        db.execute("UPDATE repair_jobs SET aadhar_number=%s WHERE id=%s AND user_id=%s",
                   (aadhar_in, job_id, session['user_id']))
    pay_hist = parse_pay_hist(job['advance_history'] if 'advance_history' in job.keys() else None)
    if amount_paid_now > 0.009:
        pay_hist = append_pay(pay_hist, amount_paid_now, pay_method, 'At Delivery')
    pay_json = json.dumps(pay_hist)
    db.execute(
        "UPDATE repair_jobs SET status='Delivered',cost=%s,delivery_date=%s,paid_status=%s,"
        "advance_amount=%s,advance_history=%s,updated_at=%s WHERE id=%s AND user_id=%s",
        (total, delivery_date, paid, total_collected, pay_json, _now_str(), job_id, session['user_id']))
    staff_id, staff_name = session_staff_stamp()
    db.execute(
        '''INSERT INTO invoices (user_id,job_id,customer_name,customer_phone,items,total,advance_amount,discount,
           pay_method,paid,due_date,staff_id,staff_name,loyalty_points,loyalty_rupees,payment_history,created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (session['user_id'], job_id, job['customer_name'], job['customer_phone'], items_str, total,
         total_collected, discount, pay_method, paid, credit_due_date, staff_id, staff_name,
         loy_pts, loy_rs, pay_json, _now_str()))
    log_staff(db, session['user_id'], 'deliver', 'job', job_id, f'INV total {total:.2f}', staff_id, staff_name)
    inv = db.execute("SELECT id FROM invoices WHERE job_id=%s ORDER BY id DESC LIMIT 1", (job_id,)).fetchone()
    inv_id = inv['id'] if inv else None
    earned = loyalty_earn(db, uid, job['customer_phone'], 'service', total_collected, 'invoice', inv_id, 'Service payment')
    db.commit()
    db.close()
    return jsonify({'ok': True, 'inv_id': inv_id, 'loyalty': earned})

@app.route('/jobs/<int:job_id>/cancel', methods=['POST'])
@active_required
def cancel_job_route(job_id):
    reason = request.form.get('reason', 'Cancelled')
    db = get_db()
    job = db.execute("SELECT status FROM repair_jobs WHERE id=%s AND user_id=%s",
                     (job_id, session['user_id'])).fetchone()
    if not job:
        db.close()
        return jsonify({'error': 'Job not found'}), 404
    if job['status'] != 'Not Ready':
        db.close()
        return jsonify({'error': 'Cancel is only allowed for Return Mobiles jobs.'}), 400
    db.execute("UPDATE repair_jobs SET status='Cancelled',cancel_reason=%s,updated_at=%s WHERE id=%s AND user_id=%s",
               (reason, _now_str(), job_id, session['user_id']))
    db.commit(); db.close()
    return jsonify({'ok': True})

@app.route('/jobs/<int:job_id>/record_refund', methods=['POST'])
@active_required
def record_refund(job_id):
    amount = float(request.form.get('amount', 0))
    method = request.form.get('method', 'Cash')
    date   = _now_str()
    db = get_db()
    db.execute("UPDATE repair_jobs SET refund_amount=%s,refund_method=%s,refund_date=%s WHERE id=%s AND user_id=%s",
               (amount, method, date, job_id, session['user_id']))
    db.commit(); db.close()
    return jsonify({'ok': True})

@app.route('/jobs/<int:job_id>/rework', methods=['POST'])
@active_required
def rework_job(job_id):
    details = request.form.get('details', '')
    db = get_db()
    orig = db.execute("SELECT * FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, session['user_id'])).fetchone()
    if not orig: db.close(); return jsonify({'error': 'Not found'}), 404
    happy_code = generate_happy_code()
    orig_notes = orig['notes'] or ''
    lock_m = re.search(r'SCREEN LOCK:\s*([^|]+)', orig_notes, re.I)
    lock_bit = f" | SCREEN LOCK: {lock_m.group(1).strip()}" if lock_m else ''
    notes = f"Original Job: #{job_id} | {details}{lock_bit}"
    staff_id, staff_name = session_staff_stamp()
    db.execute('''INSERT INTO repair_jobs
                  (user_id,customer_name,customer_phone,device_model,imei,imei_billing,
                   issue,aadhar_number,received_without,cost,notes,status,happy_code,
                   rework_details,original_job_id,staff_id,staff_name,created_at,updated_at)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Rework',%s,%s,%s,%s,%s,%s,%s)''',
               (session['user_id'], orig['customer_name'], orig['customer_phone'],
                orig['device_model'], orig['imei'], orig['imei_billing'],
                f"REWORK: {details}", orig['aadhar_number'], orig['received_without'],
                0, notes, happy_code, details, job_id, staff_id, staff_name, _now_str(), _now_str()))
    db.commit(); db.close()
    return jsonify({'ok': True})

@app.route('/jobs/<int:job_id>/delete', methods=['POST'])
@active_required
def delete_job(job_id):
    db = get_db()
    db.execute("DELETE FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, session['user_id']))
    db.commit(); db.close()
    flash('Job deleted.', 'success')
    return redirect(url_for('jobs'))

@app.route('/invoices')
@active_required
def invoices():
    db = get_db()
    user    = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    all_inv = db.execute("SELECT * FROM invoices WHERE user_id=%s ORDER BY created_at DESC", (session['user_id'],)).fetchall()
    db.close()
    inv_list = [dict(r) for r in all_inv]
    return render_template('invoices.html', invoices=all_inv, invoices_json=json.dumps(inv_list),
                           user=user, status=subscription_status(user), days_left=days_left(user))

@app.route('/invoices/<int:inv_id>/print')
@active_required
def print_invoice(inv_id):
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    inv  = db.execute("SELECT * FROM invoices WHERE id=%s AND user_id=%s", (inv_id, session['user_id'])).fetchone()
    if not inv:
        db.close(); flash('Invoice not found.', 'error')
        return redirect(url_for('invoices'))
    job = db.execute("SELECT * FROM repair_jobs WHERE id=%s AND user_id=%s", (inv['job_id'], session['user_id'])).fetchone() if inv['job_id'] else None
    loy = loyalty_info(db, session['user_id'], inv['customer_phone'])
    pay_log = parse_pay_hist(inv['payment_history'] if inv and 'payment_history' in inv.keys() else None)
    if not pay_log and job:
        pay_log = parse_pay_hist(job['advance_history'] if 'advance_history' in job.keys() else None)
    if not pay_log and float(inv['advance_amount'] or 0) > 0.009:
        pay_log = [{
            'amount': float(inv['advance_amount'] or 0),
            'method': inv['pay_method'] or 'Cash',
            'date': str(inv['created_at'] or '')[:19],
            'note': 'Paid',
        }]
    db.close()
    return render_template('print_invoice.html', inv=inv, user=user, job=job, loyalty=loy, pay_log=pay_log)

@app.route('/invoices/<int:inv_id>/mark_paid', methods=['POST'])
@active_required
def mark_invoice_paid(inv_id):
    amount_received = float(request.form.get('amount_received', 0))
    pay_method = request.form.get('pay_method', 'Cash')
    due_date   = (request.form.get('due_date') or '')[:10]
    db = get_db()
    inv = db.execute("SELECT * FROM invoices WHERE id=%s AND user_id=%s", (inv_id, session['user_id'])).fetchone()
    if not inv: db.close(); return jsonify({'error': 'Not found'}), 404
    if amount_received <= 0.009:
        db.close(); return jsonify({'error': 'Enter an amount to collect.'}), 400
    new_adv = float(inv['advance_amount'] or 0) + amount_received
    total   = float(inv['total'] or 0)
    balance = max(0, total - new_adv)
    paid    = 'Paid' if balance < 0.01 else ('Partial' if new_adv > 0 else 'Unpaid')
    if balance > 0.01 and not valid_credit_due_date(due_date):
        db.close(); return jsonify({'error': 'Due date must be within 15 days from today.'}), 400
    hist = parse_pay_hist(inv['payment_history'] if 'payment_history' in inv.keys() else None)
    if not hist and inv['job_id']:
        job_row = db.execute("SELECT advance_history FROM repair_jobs WHERE id=%s AND user_id=%s",
                             (inv['job_id'], session['user_id'])).fetchone()
        if job_row:
            hist = parse_pay_hist(job_row['advance_history'])
    hist = append_pay(hist, amount_received, pay_method, 'Due Collected')
    pay_json = json.dumps(hist)
    db.execute(
        "UPDATE invoices SET advance_amount=%s,pay_method=%s,paid=%s,due_date=%s,payment_history=%s WHERE id=%s",
        (new_adv, pay_method, paid, due_date or None, pay_json, inv_id))
    if inv['job_id']:
        db.execute(
            "UPDATE repair_jobs SET paid_status=%s,advance_amount=%s,advance_history=%s WHERE id=%s AND user_id=%s",
            (paid, new_adv, pay_json, inv['job_id'], session['user_id']))
    earned = loyalty_earn(db, session['user_id'], inv['customer_phone'], 'service', amount_received, 'invoice', inv_id, 'Service collect')
    db.commit(); db.close()
    return jsonify({'ok': True, 'paid': paid, 'advance': new_adv, 'balance': balance, 'loyalty': earned, 'payment_history': hist})

@app.route('/customers/collect-all', methods=['POST'])
@active_required
def collect_all_service():
    if not _staff_can_collect():
        return jsonify({'error': 'Collect is not allowed for this login'}), 403
    phone = _norm_phone(request.form.get('phone'))
    pay_method = request.form.get('pay_method', 'Cash')
    if pay_method not in ('Cash', 'UPI', 'Card', 'Bank'):
        pay_method = 'Cash'
    due_date = (request.form.get('due_date') or '').strip()[:10]
    try:
        amount = float(request.form.get('amount_received', 0) or 0)
    except (TypeError, ValueError):
        return jsonify({'error': 'Enter a valid amount'}), 400
    if len(phone) != 10:
        return jsonify({'error': 'Customer phone is required'}), 400
    if amount <= 0.009:
        return jsonify({'error': 'Enter an amount greater than 0'}), 400
    db = get_db()
    uid = session['user_id']
    dues = []
    invs = db.execute(
        "SELECT * FROM invoices WHERE user_id=%s ORDER BY created_at ASC, id ASC", (uid,)).fetchall()
    billed_jobs = set()
    for inv in invs:
        if _norm_phone(inv['customer_phone']) != phone:
            continue
        bal = max(0.0, float(inv['total'] or 0) - float(inv['advance_amount'] or 0))
        if bal > 0.01:
            dues.append(('inv', inv, bal))
        if inv['job_id']:
            billed_jobs.add(int(inv['job_id']))
    jobs = db.execute(
        "SELECT * FROM repair_jobs WHERE user_id=%s ORDER BY created_at ASC, id ASC", (uid,)).fetchall()
    for job in jobs:
        if _norm_phone(job['customer_phone']) != phone:
            continue
        if (job['status'] or '') in ('Delivered', 'Cancelled'):
            continue
        if int(job['id']) in billed_jobs:
            continue
        cost = float(job['cost'] or 0)
        if cost <= 0.009:
            continue
        bal = max(0.0, cost - float(job['advance_amount'] or 0))
        if bal > 0.01:
            dues.append(('job', job, bal))
    total_due = sum(x[2] for x in dues)
    if not dues:
        db.close()
        return jsonify({'error': 'No due amount on this customer'}), 400
    if amount > total_due + 0.009:
        db.close()
        return jsonify({'error': f'Amount cannot exceed total due ₹{total_due:.2f}'}), 400
    leftover = round(total_due - amount, 2)
    if leftover > 0.01 and not valid_credit_due_date(due_date):
        db.close()
        return jsonify({'error': 'Due date must be within 15 days from today.'}), 400
    remaining = amount
    applied = []
    name = ''
    try:
        for kind, rec, bal in dues:
            if remaining <= 0.009:
                break
            take = round(min(remaining, bal), 2)
            if kind == 'inv':
                name = name or (rec['customer_name'] or '')
                row, err = _apply_invoice_collect(db, uid, rec, take, pay_method, due_date)
            else:
                name = name or (rec['customer_name'] or '')
                row, err = _apply_job_advance(db, uid, rec, take, pay_method)
            if err:
                db.rollback()
                db.close()
                return jsonify({'error': err}), 400
            applied.append(row)
            remaining = round(remaining - take, 2)
        db.commit()
    except Exception as e:
        db.rollback()
        db.close()
        return jsonify({'error': str(e)}), 400
    db.close()
    collected = round(amount - remaining, 2)
    return jsonify({
        'ok': True, 'collected': collected, 'remaining_due': max(0.0, leftover),
        'count': len(applied), 'items': applied, 'customer': name, 'phone': phone,
    })

def _shop_ctx(db, uid):
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    return user, subscription_status(user), days_left(user)

@app.route('/sales')
@active_required
def sales_hub():
    db = get_db()
    uid = session['user_id']
    user, st, dl = _shop_ctx(db, uid)
    stats = load_sales_stats(db, uid)
    q = (request.args.get('q') or '').strip()
    date_f = (request.args.get('date') or '').strip()[:10]
    sql = "SELECT * FROM sales_bills WHERE user_id=%s"
    params = [uid]
    if date_f:
        sql += f" AND {sql_date('created_at')}=%s"
        params.append(date_f)
    sql += " ORDER BY created_at DESC LIMIT 200"
    try:
        repair_sale_statuses(db, uid)
        db.commit()
    except Exception:
        db.rollback()
    bills = db.execute(sql, tuple(params)).fetchall()
    db.close()
    bills_list = [enrich_sale_bill(r) for r in bills]
    return render_template(
        'sales.html', user=user, status=st, days_left=dl, stats=stats,
        bills=bills_list, bills_json=json.dumps(bills_list, default=str), q=q, date_f=date_f)

@app.route('/sales/new', methods=['GET', 'POST'])
@active_required
def sale_new():
    db = get_db()
    uid = session['user_id']
    user, st, dl = _shop_ctx(db, uid)
    if request.method == 'POST':
        try:
            payload = request.get_json(silent=True) or {}
            name = (payload.get('customer_name') or '').strip().upper()
            phone = re.sub(r'\D', '', payload.get('customer_phone') or '')[-10:]
            if len(phone) != 10:
                db.close()
                return jsonify({'error': 'Enter a 10-digit mobile number'}), 400
            known = find_sales_customer(db, uid, phone)
            if known and known.get('name'):
                name = known['name']
            if not name:
                db.close()
                return jsonify({'error': 'Enter customer name'}), 400
            discount = float(payload.get('discount') or 0)
            pay_method = payload.get('pay_method') or 'Cash'
            if pay_method not in ('Cash', 'UPI', 'Card', 'Bank', 'Credit'):
                pay_method = 'Cash'
            lines = payload.get('items') or []
            if not lines:
                db.close()
                return jsonify({'error': 'Add at least one item'}), 400
            built = []
            subtotal = 0.0
            for ln in lines:
                item_id = int(ln.get('item_id') or 0)
                qty = float(ln.get('qty') or 0)
                if qty <= 0:
                    db.close()
                    return jsonify({'error': 'Each item needs a quantity greater than 0'}), 400
                item = db.execute(
                    "SELECT * FROM inventory_items WHERE id=%s AND user_id=%s AND active=1",
                    (item_id, uid)).fetchone()
                if not item:
                    db.close()
                    return jsonify({'error': 'Item not found or inactive'}), 400
                price = float(item['sell_price'] or 0)
                if price <= 0:
                    db.close()
                    return jsonify({'error': f'Set a sell price in inventory for {item["name"]}'}), 400
                if float(item['qty'] or 0) + 0.0001 < qty:
                    db.close()
                    return jsonify({'error': f'Not enough stock for {item["name"]}'}), 400
                amt = round(qty * price, 2)
                subtotal += amt
                hsn = _hsn_code(ln.get('hsn') or ln.get('hsn_code') or _rg(item, 'hsn_code'))
                built.append({
                    'item_id': item_id, 'name': item['name'], 'sku': item['sku'] or '',
                    'hsn': hsn, 'qty': qty, 'price': price, 'amount': amt
                })
            if discount < 0 or discount > subtotal + 0.005:
                db.close()
                return jsonify({'error': 'Discount cannot exceed subtotal'}), 400
            redeem_pts = int(payload.get('redeem_points') or 0)
            loy_pts, loy_rs = 0, 0.0
            if redeem_pts:
                min_pts = LOYALTY_MIN.get('sales') or 300
                if redeem_pts < min_pts:
                    db.close()
                    return jsonify({'error': f'Minimum redemption is {min_pts} points'}), 400
                acc = loyalty_get(db, uid, phone)
                if acc['points'] < redeem_pts:
                    db.close()
                    return jsonify({'error': f'Only {acc["points"]} points available'}), 400
                loy_pts = redeem_pts
                loy_rs = loyalty_rupees_from_pts(redeem_pts)
            if discount + loy_rs > subtotal + 0.005:
                db.close()
                return jsonify({'error': 'Discount and points cannot exceed subtotal'}), 400
            total = round(subtotal - discount - loy_rs, 2)
            if total < -0.009:
                db.close()
                return jsonify({'error': 'Sale total cannot be negative'}), 400
            paid_now = float(payload.get('paid_amount') if payload.get('paid_amount') is not None else total)
            if paid_now < 0:
                db.close()
                return jsonify({'error': 'Amount received cannot be negative'}), 400
            due_date = (payload.get('due_date') or '').strip()[:10] or None
            txn_id = clean_txn_id(pay_method, payload.get('txn_id'))
            # --- GST ---
            is_gst = 1 if payload.get('is_gst') else 0
            gst_rate = float(payload.get('gst_rate') or 0)
            customer_gstin = (payload.get('customer_gstin') or '').strip().upper()
            billing_state = (payload.get('billing_state') or '').strip().upper()
            auto_bill_state = _gst_state_from_gstin(customer_gstin)
            if auto_bill_state:
                billing_state = auto_bill_state
            customer_address = (payload.get('customer_address') or '').strip()
            sale_cgst = sale_sgst = sale_igst = 0.0
            taxable_value = round(subtotal - discount - loy_rs, 2)  # base without tax
            if is_gst:
                missing = next((ln['name'] for ln in built if not ln.get('hsn')), None)
                if missing:
                    db.close()
                    return jsonify({'error': f'Enter HSN / SAC code (4–8 digits) for {missing}'}), 400
                for ln in built:
                    if ln.get('hsn') and ln.get('item_id'):
                        try:
                            db.execute(
                                "UPDATE inventory_items SET hsn_code=%s WHERE id=%s AND user_id=%s"
                                " AND (hsn_code IS NULL OR hsn_code='')",
                                (ln['hsn'], ln['item_id'], uid))
                        except Exception:
                            pass
            if is_gst and gst_rate > 0:
                gst_exclusive = 1 if payload.get('gst_exclusive') else 0
                base = round(total, 2)
                shop_user = db.execute('SELECT state FROM users WHERE id=?', (uid,)).fetchone()
                shop_state = (shop_user['state'] if shop_user and shop_user['state'] else '').strip().upper()
                if gst_exclusive:
                    # Tax exclusive: tax is added ON TOP of the base total
                    taxable_value = base
                    tax_amount = round(base * gst_rate / 100, 2)
                    total = round(base + tax_amount, 2)  # recalculate total to include tax
                else:
                    # Tax inclusive: extract tax backward from total
                    taxable_value = round(base / (1 + gst_rate / 100), 2)
                    tax_amount = round(base - taxable_value, 2)
                if billing_state and shop_state and billing_state != shop_state:
                    sale_igst = tax_amount
                else:
                    sale_cgst = round(tax_amount / 2, 2)
                    sale_sgst = round(tax_amount - sale_cgst, 2)
            # Validate paid_now AFTER total may have been adjusted by GST exclusive
            if paid_now > total + 0.005:
                db.close()
                return jsonify({'error': 'Amount received cannot exceed total'}), 400
            status = sale_pay_status(total, paid_now)
            if status != 'Paid':
                if not valid_credit_due_date(due_date):
                    db.close()
                    return jsonify({'error': 'Due date must be within 15 days from today.'}), 400
            bill_no = next_sale_bill_no(db, uid)
            staff_id, staff_name = session_staff_stamp()
            bill_id = db_insert_id(db, '''INSERT INTO sales_bills
                (user_id,bill_no,customer_name,customer_phone,items,subtotal,discount,total,pay_method,paid_status,paid_amount,due_date,txn_id,orig_discount,staff_id,staff_name,loyalty_points,loyalty_rupees,is_gst,gst_rate,customer_gstin,billing_state,customer_address,cgst,sgst,igst,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (uid, bill_no, name, phone, json.dumps(built), subtotal, discount, total, pay_method, status, paid_now, due_date, txn_id, discount, staff_id, staff_name, loy_pts, loy_rs, is_gst, gst_rate, customer_gstin, billing_state, customer_address, sale_cgst, sale_sgst, sale_igst, _now_str()))
            for ln in built:
                ok, msg = apply_stock_change(
                    db, uid, ln['item_id'], -ln['qty'], 'out', 'sale', bill_id, f'SAL-{bill_no:04d}')
                if not ok:
                    db.rollback()
                    db.close()
                    return jsonify({'error': msg}), 400
            if redeem_pts:
                red, err = loyalty_redeem(db, uid, phone, 'sales', redeem_pts, 'sale', bill_id)
                if err:
                    db.rollback()
                    db.close()
                    return jsonify({'error': err}), 400
            if len(phone) == 10:
                upsert_sales_customer(db, uid, name, phone)
                if is_gst and (customer_gstin or billing_state or customer_address):
                    try:
                        db.execute('UPDATE sales_customers SET gstin=?, state=?, address=? WHERE user_id=? AND phone=?',
                            (customer_gstin or None, billing_state or None, customer_address or None, uid, phone))
                    except Exception:
                        pass
            log_staff(db, uid, 'sale', 'sale', bill_id, f'SAL-{bill_no:04d}', staff_id, staff_name)
            earned = loyalty_earn(db, uid, phone, 'sales', paid_now, 'sale', bill_id, f'SAL-{bill_no:04d}')
            db.commit()
            db.close()
            return jsonify({'ok': True, 'id': bill_id, 'bill_no': bill_no, 'loyalty': earned})
        except Exception as e:
            try:
                db.rollback()
            except Exception:
                pass
            db.close()
            return jsonify({'error': 'Could not save sale'}), 500
    items = db.execute(
        "SELECT id,sku,name,category,qty,sell_price,serial_no,hsn_code,gst_rate FROM inventory_items WHERE user_id=%s AND active=1 ORDER BY name",
        (uid,)).fetchall()
    db.close()
    items_json = json.dumps([dict(r) for r in items], default=str)
    prefill_phone = re.sub(r'\D', '', request.args.get('phone') or '')[-10:]
    prefill_name = (request.args.get('name') or '').strip().upper()
    return render_template(
        'sale_new.html', user=user, status=st, days_left=dl, items_json=items_json,
        prefill_phone=prefill_phone, prefill_name=prefill_name)

@app.route('/sales/<int:bill_id>/print')
@active_required
def print_sale(bill_id):
    db = get_db()
    uid = session['user_id']
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    bill = db.execute("SELECT * FROM sales_bills WHERE id=%s AND user_id=%s", (bill_id, uid)).fetchone()
    if not bill:
        db.close()
        flash('Bill not found.', 'error')
        return redirect(url_for('sales_hub'))
    try:
        lines = json.loads(bill['items'] or '[]')
    except Exception:
        lines = []
    loy = loyalty_info(db, uid, bill['customer_phone'])
    db.close()
    return render_template(
        'print_sale.html', bill=bill, user=user, lines=lines, is_refund=False,
        collected=sale_collected(bill), balance=sale_balance(bill), loyalty=loy)

@app.route('/sales/<int:bill_id>/print-refund')
@active_required
def print_sale_refund(bill_id):
    db = get_db()
    uid = session['user_id']
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    bill = db.execute("SELECT * FROM sales_bills WHERE id=%s AND user_id=%s", (bill_id, uid)).fetchone()
    db.close()
    if not bill:
        flash('Bill not found.', 'error')
        return redirect(url_for('sales_hub'))
    try:
        lines = json.loads(bill['items'] or '[]')
        if not isinstance(lines, list):
            lines = []
    except Exception:
        lines = []
    has_return = any(float(ln.get('returned_qty') or 0) > 0.0001 for ln in lines)
    if (bill['paid_status'] or '') != 'Returned' and float(bill['refund_amount'] or 0) <= 0.009 and not has_return:
        flash('Refund receipt is available after a return.', 'error')
        return redirect(url_for('sales_hub'))
    return render_template(
        'print_sale.html', bill=bill, user=user, lines=lines, is_refund=True,
        collected=sale_collected(bill), balance=0)

@app.route('/sales/<int:bill_id>/collect', methods=['POST'])
@active_required
def collect_sale(bill_id):
    amount_received = float(request.form.get('amount_received', 0) or 0)
    pay_method = request.form.get('pay_method', 'Cash')
    if pay_method not in ('Cash', 'UPI', 'Card', 'Bank'):
        pay_method = 'Cash'
    due_date = (request.form.get('due_date') or '').strip()[:10]
    txn_id = clean_txn_id(pay_method, request.form.get('txn_id'))
    db = get_db()
    uid = session['user_id']
    bill = db.execute("SELECT * FROM sales_bills WHERE id=%s AND user_id=%s", (bill_id, uid)).fetchone()
    if not bill:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    if bill['paid_status'] == 'Returned':
        db.close()
        return jsonify({'error': 'Returned bills cannot be collected'}), 400
    if amount_received <= 0:
        db.close()
        return jsonify({'error': 'Enter an amount greater than 0'}), 400
    new_paid = sale_collected(bill) + amount_received
    total = float(bill['total'] or 0)
    if new_paid > total + 0.005:
        db.close()
        return jsonify({'error': 'Amount cannot exceed balance due'}), 400
    status = sale_pay_status(total, new_paid)
    balance = max(0.0, total - new_paid)
    if balance > 0.01 and not valid_credit_due_date(due_date):
        db.close()
        return jsonify({'error': 'Due date must be within 15 days from today.'}), 400
    if txn_id:
        db.execute(
            "UPDATE sales_bills SET paid_amount=%s,pay_method=%s,paid_status=%s,due_date=%s,txn_id=%s WHERE id=%s AND user_id=%s",
            (new_paid, pay_method, status, due_date or None, txn_id, bill_id, uid))
    else:
        db.execute(
            "UPDATE sales_bills SET paid_amount=%s,pay_method=%s,paid_status=%s,due_date=%s WHERE id=%s AND user_id=%s",
            (new_paid, pay_method, status, due_date or None, bill_id, uid))
    log_staff(db, uid, 'collect', 'sale', bill_id, f'{amount_received:.2f} {pay_method}')
    earned = loyalty_earn(db, uid, bill['customer_phone'], 'sales', amount_received, 'sale', bill_id, 'Sales collect')
    db.commit()
    db.close()
    return jsonify({'ok': True, 'paid': status, 'paid_amount': new_paid, 'balance': balance, 'loyalty': earned})

@app.route('/sales/collect-all', methods=['POST'])
@active_required
def collect_all_sales():
    if not _staff_can_collect():
        return jsonify({'error': 'Collect is not allowed for this login'}), 403
    phone = _norm_phone(request.form.get('phone'))
    pay_method = request.form.get('pay_method', 'Cash')
    if pay_method not in ('Cash', 'UPI', 'Card', 'Bank'):
        pay_method = 'Cash'
    due_date = (request.form.get('due_date') or '').strip()[:10]
    txn_id = request.form.get('txn_id')
    try:
        amount = float(request.form.get('amount_received', 0) or 0)
    except (TypeError, ValueError):
        return jsonify({'error': 'Enter a valid amount'}), 400
    if len(phone) != 10:
        return jsonify({'error': 'Customer phone is required'}), 400
    if amount <= 0.009:
        return jsonify({'error': 'Enter an amount greater than 0'}), 400
    db = get_db()
    uid = session['user_id']
    repair_sale_statuses(db, uid)
    rows = db.execute(
        "SELECT * FROM sales_bills WHERE user_id=%s ORDER BY created_at ASC, id ASC", (uid,)).fetchall()
    due_bills = []
    total_due = 0.0
    for b in rows:
        if _norm_phone(b['customer_phone']) != phone:
            continue
        if (b['paid_status'] or '') == 'Returned':
            continue
        bal = sale_balance(b)
        if bal > 0.01:
            due_bills.append((b, bal))
            total_due += bal
    if not due_bills:
        db.close()
        return jsonify({'error': 'No due amount on this customer'}), 400
    if amount > total_due + 0.009:
        db.close()
        return jsonify({'error': f'Amount cannot exceed total due ₹{total_due:.2f}'}), 400
    leftover = round(total_due - amount, 2)
    if leftover > 0.01 and not valid_credit_due_date(due_date):
        db.close()
        return jsonify({'error': 'Due date must be within 15 days from today.'}), 400
    remaining = amount
    applied = []
    name = due_bills[0][0]['customer_name'] or ''
    try:
        for bill, bal in due_bills:
            if remaining <= 0.009:
                break
            take = round(min(remaining, bal), 2)
            row, err = _apply_sale_collect(db, uid, bill, take, pay_method, due_date, txn_id)
            if err:
                db.rollback()
                db.close()
                return jsonify({'error': err}), 400
            applied.append(row)
            remaining = round(remaining - take, 2)
        db.commit()
    except Exception as e:
        db.rollback()
        db.close()
        return jsonify({'error': str(e)}), 400
    db.close()
    collected = round(amount - remaining, 2)
    return jsonify({
        'ok': True, 'collected': collected, 'remaining_due': max(0.0, leftover),
        'count': len(applied), 'items': applied, 'customer': name, 'phone': phone,
    })

@app.route('/sales/<int:bill_id>/return', methods=['POST'])
@active_required
def return_sale(bill_id):
    payload = request.get_json(silent=True) or {}
    reason = (payload.get('reason') or request.form.get('reason') or '').strip()
    method = payload.get('refund_method') or request.form.get('refund_method') or ''
    if method not in ('Cash', 'UPI', 'Card', 'Bank'):
        return jsonify({'error': 'Select refund method: Cash, UPI, Card, or Bank'}), 400
    db = get_db()
    uid = session['user_id']
    bill = db.execute("SELECT * FROM sales_bills WHERE id=%s AND user_id=%s", (bill_id, uid)).fetchone()
    if not bill:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    if bill['paid_status'] == 'Returned':
        db.close()
        return jsonify({'error': 'Already returned'}), 400
    if len(reason) < 3:
        db.close()
        return jsonify({'error': 'Enter a return reason'}), 400
    collected = sale_collected(bill)
    try:
        lines = json.loads(bill['items'] or '[]')
        if not isinstance(lines, list):
            lines = []
    except Exception:
        lines = []
    req = payload.get('lines')
    this_qty = []
    if isinstance(req, list) and req:
        for i, ln in enumerate(lines):
            avail = line_remain_qty(ln)
            q = 0.0
            for spec in req:
                try:
                    if int(spec.get('index')) == i:
                        q = float(spec.get('qty') or 0)
                        break
                except (TypeError, ValueError):
                    continue
            if q < -0.0001 or q > avail + 0.0001:
                db.close()
                return jsonify({'error': 'Return quantity is not valid'}), 400
            this_qty.append(round(q, 3))
    else:
        this_qty = [line_remain_qty(ln) for ln in lines]
    if not any(q > 0.0001 for q in this_qty):
        db.close()
        return jsonify({'error': 'Select a quantity to return'}), 400
    old_sub, old_disc, old_total, _ = sale_remaining_figures(bill, lines)
    for i, ln in enumerate(lines):
        q = this_qty[i]
        if q <= 0.0001:
            continue
        item_id = int(ln.get('item_id') or 0)
        if item_id:
            ok, msg = apply_stock_change(
                db, uid, item_id, q, 'in', 'sale', bill_id,
                f'Return SAL-{int(bill["bill_no"] or 0):04d}')
            if not ok:
                db.rollback()
                db.close()
                return jsonify({'error': msg}), 400
        ln['returned_qty'] = round(float(ln.get('returned_qty') or 0) + q, 3)
    remain_sub, remain_disc, remain_total, fully = sale_remaining_figures(bill, lines)
    returned_value = round(max(0.0, old_total - remain_total), 2)
    try:
        refund_amt = float(payload.get('refund_amount') if payload.get('refund_amount') is not None
                           else request.form.get('refund_amount', min(collected, returned_value)))
    except (TypeError, ValueError):
        db.close()
        return jsonify({'error': 'Enter a valid refund amount'}), 400
    max_refund = min(collected, returned_value)
    if refund_amt < -0.0001:
        db.close()
        return jsonify({'error': 'Refund cannot be negative'}), 400
    if refund_amt > max_refund + 0.005:
        db.close()
        return jsonify({'error': 'Refund cannot exceed the returned value or amount collected'}), 400
    if max_refund > 0.01:
        refund_amt = round(max_refund, 2)
    else:
        refund_amt = 0.0
    new_paid = round(max(0.0, collected - refund_amt), 2)
    prev_refund = float(bill['refund_amount'] or 0)
    status = 'Returned' if fully else sale_pay_status(remain_total, new_paid)
    db.execute(
        '''UPDATE sales_bills SET items=%s,subtotal=%s,discount=%s,total=%s,paid_amount=%s,paid_status=%s,
           return_reason=%s,refund_amount=%s,refund_method=%s,refund_date=%s WHERE id=%s AND user_id=%s''',
        (json.dumps(lines), remain_sub, remain_disc, remain_total, new_paid, status,
         reason, round(prev_refund + refund_amt, 2), method or None, _now_str(), bill_id, uid))
    if refund_amt > 0.009:
        claw = loyalty_earn_pts(refund_amt)
        if claw:
            loyalty_add(db, uid, bill['customer_phone'], -claw, 'sales', 'adjust', refund_amt, 'sale', bill_id, 'Return clawback')
    db.commit()
    db.close()
    return jsonify({'ok': True, 'refund_amount': refund_amt, 'refund_method': method, 'reason': reason, 'full': fully})

@app.route('/inventory', methods=['GET', 'POST'])
@active_required
def inventory():
    db = get_db()
    uid = session['user_id']
    user, st, dl = _shop_ctx(db, uid)
    if request.method == 'POST':
        action = request.form.get('action') or 'save'
        if action == 'save':
            item_id = request.form.get('item_id') or ''
            name = (request.form.get('name') or '').strip().upper()
            sku = (request.form.get('sku') or '').strip().upper()
            hsn_raw = (request.form.get('hsn_code') or request.form.get('hsn') or '').strip()
            hsn_code = _hsn_code(hsn_raw)
            if hsn_raw and not hsn_code:
                db.close()
                flash('HSN / SAC must be 4 to 8 digits.', 'error')
                return redirect(url_for('inventory'))
            try:
                gst_rate = float(request.form.get('gst_rate') or 0)
            except ValueError:
                gst_rate = 0
            if gst_rate not in (0, 5, 12, 18, 28):
                gst_rate = 0
            serial_no = clean_imei_serial(request.form.get('serial_no'))
            category = request.form.get('category') or 'Accessory'
            sub_category = request.form.get('sub_category') or ''
            unit = (request.form.get('unit') or 'PCS').strip().upper() or 'PCS'
            try:
                reorder = float(
                    request.form.get('low_stock_qty') or request.form.get('reorder_qty') or 0)
                cost = float(request.form.get('cost_price') or 0)
                sell = float(request.form.get('sell_price') or 0)
            except ValueError:
                db.close()
                flash('Enter valid numbers for price and low stock reminder qty.', 'error')
                return redirect(url_for('inventory'))
            active = 1 if request.form.get('active') == '1' else 0
            if not name:
                db.close()
                flash('Item name is required.', 'error')
                return redirect(url_for('inventory'))
            if item_id:
                old = db.execute(
                    "SELECT * FROM inventory_items WHERE id=%s AND user_id=%s",
                    (int(item_id), uid)).fetchone()
                if not old:
                    db.close()
                    flash('Item not found.', 'error')
                    return redirect(url_for('inventory'))
                db.execute(
                    '''UPDATE inventory_items SET sku=%s,name=%s,category=%s,sub_category=%s,unit=%s,reorder_qty=%s,
                       cost_price=%s,sell_price=%s,active=%s,serial_no=%s,hsn_code=%s,gst_rate=%s,updated_at=%s WHERE id=%s AND user_id=%s''',
                    (sku, name, category, sub_category, unit, reorder, cost, sell, active, serial_no or None, hsn_code or None, gst_rate, _now_str(), int(item_id), uid))
                diffs = inventory_item_diff(old, {
                    'name': name, 'sku': sku, 'category': category, 'sub_category': sub_category, 'unit': unit,
                    'reorder_qty': reorder, 'cost_price': cost, 'sell_price': sell, 'active': active,
                    'serial_no': serial_no, 'hsn_code': hsn_code, 'gst_rate': gst_rate
                })
                if diffs:
                    q = float(old['qty'] or 0)
                    log_inventory_change(db, uid, int(item_id), 'update', '; '.join(diffs), q, q)
                flash('Item updated.', 'success')
            else:
                opening = float(request.form.get('qty') or 0)
                if opening < 0:
                    opening = 0
                iid = db_insert_id(db, '''INSERT INTO inventory_items
                    (user_id,sku,name,category,sub_category,unit,qty,reorder_qty,cost_price,sell_price,active,serial_no,hsn_code,gst_rate,created_at,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    (uid, sku, name, category, sub_category, unit, 0, reorder, cost, sell, 1, serial_no or None, hsn_code or None, gst_rate, _now_str(), _now_str()))
                log_inventory_change(db, uid, iid, 'create', 'Item added', 0, 0)
                if opening > 0:
                    apply_stock_change(db, uid, iid, opening, 'in', 'purchase', iid, 'Opening stock')
                flash('Item added.', 'success')
            db.commit()
        elif action == 'stock_in':
            try:
                item_id = int(request.form.get('item_id') or 0)
                qty = float(request.form.get('qty') or 0)
            except ValueError:
                db.close()
                flash('Enter a valid quantity.', 'error')
                return redirect(url_for('inventory'))
            note = (request.form.get('note') or '').strip()
            if qty <= 0:
                db.close()
                flash('Stock in quantity must be greater than 0.', 'error')
                return redirect(url_for('inventory'))
            ok, msg = apply_stock_change(db, uid, item_id, qty, 'in', 'purchase', item_id, note or 'Stock in')
            if not ok:
                db.close()
                flash(msg, 'error')
                return redirect(url_for('inventory'))
            db.commit()
            flash('Stock updated.', 'success')
        elif action == 'warranty_take':
            try:
                item_id = int(request.form.get('item_id') or 0)
                qty = float(request.form.get('qty') or 0)
            except ValueError:
                db.close()
                flash('Enter a valid quantity.', 'error')
                return redirect(url_for('inventory'))
            supplier = (request.form.get('supplier') or '').strip().upper()
            reason = (request.form.get('reason') or '').strip()
            reason_other = (request.form.get('reason_other') or '').strip()
            faulty_serial = clean_imei_serial(request.form.get('faulty_serial'))
            claim_no = (request.form.get('claim_no') or '').strip().upper()
            note = (request.form.get('note') or '').strip()
            if qty < 1:
                db.close()
                flash('Quantity taken must be at least 1.', 'error')
                return redirect(url_for('inventory'))
            if not supplier:
                db.close()
                flash('Supplier name is required.', 'error')
                return redirect(url_for('inventory'))
            if reason not in WARR_REASONS:
                db.close()
                flash('Select a warranty reason.', 'error')
                return redirect(url_for('inventory'))
            if reason == 'Other' and not reason_other:
                db.close()
                flash('Enter the other reason.', 'error')
                return redirect(url_for('inventory'))
            item = db.execute(
                "SELECT * FROM inventory_items WHERE id=%s AND user_id=%s",
                (item_id, uid)).fetchone()
            if not item:
                db.close()
                flash('Item not found.', 'error')
                return redirect(url_for('inventory'))
            on_hand = float(item['qty'] or 0)
            if qty > on_hand + 0.0001:
                db.close()
                flash('Quantity cannot be more than stock on hand.', 'error')
                return redirect(url_for('inventory'))
            reason_txt = reason_other if reason == 'Other' else reason
            now = _now_str()
            wid = db_insert_id(db, '''INSERT INTO warranty_replacements
                (user_id,item_id,qty,supplier,reason,reason_other,faulty_serial,claim_no,note,status,taken_at,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'taken',%s,%s)''',
                (uid, item_id, qty, supplier, reason, reason_other or None,
                 faulty_serial or None, claim_no or None, note or None, now, now))
            parts = [f'Supplier: {supplier}', reason_txt]
            if claim_no:
                parts.append('Claim ' + claim_no)
            if faulty_serial:
                parts.append('Faulty SN ' + faulty_serial)
            if note:
                parts.append(note)
            ok, msg = apply_stock_change(
                db, uid, item_id, -qty, 'warranty', 'take', wid,
                f'{_qty_s(qty)} taken · ' + ' · '.join(parts))
            if not ok:
                db.rollback()
                db.close()
                flash(msg, 'error')
                return redirect(url_for('inventory'))
            db.commit()
            flash('Taken for replacement. Mark replaced when the product is received.', 'success')
        elif action == 'warranty_receive':
            try:
                claim_id = int(request.form.get('claim_id') or 0)
            except ValueError:
                db.close()
                flash('Replacement record not found.', 'error')
                return redirect(url_for('inventory'))
            new_serial = clean_imei_serial(request.form.get('new_serial'))
            note = (request.form.get('note') or '').strip()
            claim = db.execute(
                "SELECT * FROM warranty_replacements WHERE id=%s AND user_id=%s",
                (claim_id, uid)).fetchone()
            if not claim or (claim['status'] or '') != 'taken':
                db.close()
                flash('No pending replacement to mark.', 'error')
                return redirect(url_for('inventory'))
            item = db.execute(
                "SELECT * FROM inventory_items WHERE id=%s AND user_id=%s",
                (claim['item_id'], uid)).fetchone()
            if not item:
                db.close()
                flash('Item not found.', 'error')
                return redirect(url_for('inventory'))
            need_serial = bool((item['serial_no'] or '').strip()) or bool(claim['faulty_serial'] or '') or (item['category'] or '') == 'Phone'
            if need_serial and not new_serial:
                db.close()
                flash('Replacement serial number is required.', 'error')
                return redirect(url_for('inventory'))
            now = _now_str()
            qty = float(claim['qty'] or 0)
            db.execute(
                '''UPDATE warranty_replacements SET status='replaced', new_serial=%s, replaced_at=%s
                   WHERE id=%s AND user_id=%s''',
                (new_serial or None, now, claim_id, uid))
            parts = [f'Supplier: {claim["supplier"]}', 'Received']
            if claim['claim_no']:
                parts.append('Claim ' + claim['claim_no'])
            if new_serial:
                parts.append('New SN ' + new_serial)
            if note:
                parts.append(note)
            ok, msg = apply_stock_change(
                db, uid, claim['item_id'], qty, 'warranty', 'receive', claim_id,
                f'{_qty_s(qty)} replaced · ' + ' · '.join(parts))
            if not ok:
                db.rollback()
                db.close()
                flash(msg, 'error')
                return redirect(url_for('inventory'))
            if new_serial and new_serial != (item['serial_no'] or '').strip().upper():
                db.execute(
                    "UPDATE inventory_items SET serial_no=%s, updated_at=%s WHERE id=%s AND user_id=%s",
                    (new_serial, now, claim['item_id'], uid))
                log_inventory_change(
                    db, uid, claim['item_id'], 'update',
                    f"Serial Number {(item['serial_no'] or '—')} → {new_serial}",
                    float(item['qty'] or 0) + qty, float(item['qty'] or 0) + qty)
            db.commit()
            flash('Marked replaced. Date and time recorded.', 'success')
        db.close()
        return redirect(url_for('inventory'))
    show_inactive = request.args.get('inactive') == '1'
    q = (request.args.get('q') or '').strip().upper()
    sql = "SELECT * FROM inventory_items WHERE user_id=%s"
    params = [uid]
    if not show_inactive:
        sql += " AND active=1"
    if q:
        sql += " AND (UPPER(name) LIKE %s OR UPPER(COALESCE(sku,'')) LIKE %s OR UPPER(COALESCE(serial_no,'')) LIKE %s)"
        params.extend([f'%{q}%', f'%{q}%', f'%{q}%'])
    sql += " ORDER BY name"
    items = [dict(r) for r in db.execute(sql, tuple(params)).fetchall()]
    pending_map = {}
    ids = [it['id'] for it in items]
    if ids:
        try:
            ph = ','.join(['%s'] * len(ids))
            for row in db.execute(
                f"""SELECT * FROM warranty_replacements
                    WHERE user_id=%s AND item_id IN ({ph}) AND status='taken'
                    ORDER BY id""",
                (uid, *ids)
            ).fetchall():
                d = dict(row)
                pending_map.setdefault(d['item_id'], []).append({
                    'id': d['id'],
                    'qty': float(d['qty'] or 0),
                    'supplier': d['supplier'] or '',
                    'reason': d['reason_other'] if d['reason'] == 'Other' and d.get('reason_other') else (d['reason'] or ''),
                    'faulty_serial': d.get('faulty_serial') or '',
                    'claim_no': d.get('claim_no') or '',
                    'taken_at': d.get('taken_at') or '',
                })
        except Exception:
            db.rollback()
    for it in items:
        it['warr_pending'] = pending_map.get(it['id']) or []
    stats = load_sales_stats(db, uid)
    
    dyn_cats = [r['name'] for r in db.execute('SELECT name FROM inventory_categories WHERE user_id=%s ORDER BY name', (uid,)).fetchall()]
    all_sub_cats = db.execute('''
        SELECT c.name as cat_name, s.name as sub_name 
        FROM inventory_subcategories s
        JOIN inventory_categories c ON s.category_id = c.id
        WHERE s.user_id=%s
    ''', (uid,)).fetchall()
    sub_map = {}
    for r in all_sub_cats:
        sub_map.setdefault(r['cat_name'], []).append(r['sub_name'])

    db.close()
    return render_template(
        'inventory.html', user=user, status=st, days_left=dl, items=items,
        q=q, show_inactive=show_inactive, categories=dyn_cats, sub_map=json.dumps(sub_map),
        warranty_reasons=WARR_REASONS, low_stock=stats['low_stock'])

def load_inventory_item_log(db, uid, item_id):
    item = db.execute(
        "SELECT * FROM inventory_items WHERE id=%s AND user_id=%s",
        (item_id, uid)).fetchone()
    if not item:
        return None, []
    moves = db.execute(
        "SELECT * FROM stock_movements WHERE user_id=%s AND item_id=%s ORDER BY id DESC",
        (uid, item_id)).fetchall()
    edits = db.execute(
        "SELECT * FROM inventory_item_logs WHERE user_id=%s AND item_id=%s ORDER BY id DESC",
        (uid, item_id)).fetchall()
    events = [_stock_movement_event(m) for m in moves]
    for e in edits:
        kind = e['kind'] or 'update'
        title = 'Item added' if kind == 'create' else 'Item edited'
        events.append({
            'kind': kind, 'title': title, 'summary': e['summary'] or title,
            'qty_delta': None,
            'qty_before': None if e['qty_before'] is None else float(e['qty_before']),
            'qty_after': None if e['qty_after'] is None else float(e['qty_after']),
            'created_at': e['created_at'],
            '_k': f"{e['created_at'] or ''}-{e['id']:010d}-e"
        })
    events.sort(key=lambda x: x['_k'], reverse=True)
    for ev in events:
        ev.pop('_k', None)
    return item, events

def _inv_row_get(row, key, default=None):
    try:
        if key in row.keys():
            val = row[key]
            return default if val is None else val
    except Exception:
        pass
    return default

def _stock_movement_event(m):
    t = (m['type'] or '')
    ref = (m['ref_type'] or '')
    note = (m['note'] or '').strip()
    qty = float(m['qty'] or 0)
    qb = _inv_row_get(m, 'qty_before')
    qa = _inv_row_get(m, 'qty_after')
    if qb is None:
        signed = -abs(qty) if t == 'out' else abs(qty)
    else:
        signed = qty
    if t == 'out':
        kind, title = 'sale', 'Sale'
    elif t == 'in' and ref == 'sale':
        kind, title = 'return', 'Sale return'
    elif t == 'adjust':
        kind, title = 'adjust', 'Adjust'
        if qb is None:
            signed = qty
    elif t == 'warranty':
        if ref == 'take':
            kind, title = 'warranty', 'Taken for replacement'
        else:
            kind, title = 'warranty_in', 'Replaced'
    else:
        kind, title = 'stock_in', 'Stock in'
    sign = '+' if signed > 0 else ''
    delta_txt = f'{sign}{_qty_s(signed)}' if abs(signed) >= 0.0001 or t == 'adjust' else ''
    summary = note or title
    if delta_txt:
        summary = f'{summary} · {delta_txt}'
    if qa is not None:
        summary = f'{summary} · on hand {_qty_s(qa)}'
    return {
        'kind': kind, 'title': title, 'summary': summary,
        'qty_delta': signed,
        'qty_before': None if qb is None else float(qb),
        'qty_after': None if qa is None else float(qa),
        'created_at': m['created_at'],
        '_k': f"{m['created_at'] or ''}-{m['id']:010d}-m"
    }

@app.route('/api/inventory/<int:item_id>/log')
@active_required
def inventory_item_log(item_id):
    db = get_db()
    uid = session['user_id']
    item, events = load_inventory_item_log(db, uid, item_id)
    db.close()
    if not item:
        return jsonify({'error': 'Item not found'}), 404
    return jsonify({
        'item': {'id': item['id'], 'name': item['name'], 'sku': item['sku'] or '', 'qty': item['qty']},
        'events': events
    })

@app.route('/inventory/<int:item_id>/log/print')
@active_required
def print_inventory_log(item_id):
    db = get_db()
    uid = session['user_id']
    user, st, dl = _shop_ctx(db, uid)
    item, events = load_inventory_item_log(db, uid, item_id)
    db.close()
    if not item:
        flash('Item not found.', 'error')
        return redirect(url_for('inventory'))
    return render_template(
        'print_item_log.html', user=user, item=item, events=events, generated_at=_now_str())

@app.route('/api/inventory/search')
@active_required
def inventory_search():
    q = (request.args.get('q') or '').strip().upper()
    db = get_db()
    uid = session['user_id']
    if q:
        rows = db.execute(
            "SELECT id,sku,name,category,qty,sell_price,serial_no,hsn_code,gst_rate FROM inventory_items"
            " WHERE user_id=%s AND active=1 AND (UPPER(name) LIKE %s OR UPPER(COALESCE(sku,'')) LIKE %s OR UPPER(COALESCE(serial_no,'')) LIKE %s)"
            " ORDER BY name LIMIT 20",
            (uid, f'%{q}%', f'%{q}%', f'%{q}%')).fetchall()
    else:
        rows = db.execute(
            "SELECT id,sku,name,category,qty,sell_price,serial_no,hsn_code,gst_rate FROM inventory_items"
            " WHERE user_id=%s AND active=1 ORDER BY name LIMIT 20", (uid,)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/purchases/<int:pid>/delete', methods=['POST'])
@active_required
def api_purchases_delete(pid):
    db = get_db()
    uid = session['user_id']
    # Check if exists
    row = db.execute("SELECT * FROM purchase_bills WHERE id=? AND user_id=?", (pid, uid)).fetchone()
    if row:
        db.execute("DELETE FROM purchase_bills WHERE id=?", (pid,))
        db.commit()
    db.close()
    return jsonify({'ok': True})

@app.route('/api/sales/customers/search')
@active_required
def sales_customer_search():
    q = request.args.get('q', '').strip().upper()
    if len(q) < 2:
        return jsonify([])
    db = get_db()
    rows = db.execute(
        "SELECT DISTINCT name AS customer_name, phone AS customer_phone FROM sales_customers"
        " WHERE user_id=%s AND (UPPER(name) LIKE %s OR phone LIKE %s)"
        " ORDER BY name LIMIT 10",
        (session['user_id'], f'%{q}%', f'%{q}%')).fetchall()
    if len(rows) < 10:
        extra = db.execute(
            "SELECT DISTINCT customer_name, customer_phone FROM sales_bills"
            " WHERE user_id=%s AND LENGTH(customer_phone)>=10"
            " AND (UPPER(customer_name) LIKE %s OR customer_phone LIKE %s)"
            " ORDER BY customer_name LIMIT 10",
            (session['user_id'], f'%{q}%', f'%{q}%')).fetchall()
        seen = {(r['customer_phone'] or '')[-10:] for r in rows}
        for r in extra:
            ph = (r['customer_phone'] or '')[-10:]
            if ph in seen:
                continue
            rows.append(r)
            seen.add(ph)
            if len(rows) >= 10:
                break
    loy_map = loyalty_map(db, session['user_id'])
    db.close()
    out = []
    for r in rows:
        ph = re.sub(r'\D', '', r['customer_phone'] or '')[-10:]
        pts = int(loy_map.get(ph, 0))
        out.append({
            'name': r['customer_name'],
            'phone': r['customer_phone'],
            'loyalty': {'points': pts, 'value': round(pts / 4.0, 2)},
        })
    return jsonify(out)

@app.route('/api/sales/customers/by-phone')
@active_required
def sales_customer_by_phone():
    phone = re.sub(r'\D', '', request.args.get('phone') or '')[-10:]
    db = get_db()
    found = find_sales_customer(db, session['user_id'], phone)
    info = loyalty_info(db, session['user_id'], phone)
    db.close()
    if not found:
        return jsonify({'name': '', 'phone': phone, 'loyalty': info})
    found = dict(found)
    found['loyalty'] = info
    return jsonify(found)

@app.route('/sales/customers', methods=['GET', 'POST'])
@active_required
def sales_customers():
    db = get_db()
    uid = session['user_id']
    user, st, dl = _shop_ctx(db, uid)
    if request.method == 'POST':
        name = (request.form.get('name') or '').strip().upper()
        phone = re.sub(r'\D', '', request.form.get('phone') or '')[-10:]
        if not name:
            db.close()
            flash('Customer name is required.', 'error')
            return redirect(url_for('sales_customers'))
        if len(phone) != 10:
            db.close()
            flash('Enter a valid 10-digit mobile number.', 'error')
            return redirect(url_for('sales_customers'))
        existing = db.execute(
            "SELECT id FROM sales_customers WHERE user_id=%s AND phone=%s", (uid, phone)).fetchone()
        upsert_sales_customer(db, uid, name, phone)
        db.commit()
        db.close()
        flash('Customer updated.' if existing else 'Customer added.', 'success')
        return redirect(url_for('sales_customers'))
    saved = db.execute(
        "SELECT name, phone FROM sales_customers WHERE user_id=%s ORDER BY name", (uid,)).fetchall()
    bills = db.execute(
        "SELECT * FROM sales_bills WHERE user_id=%s ORDER BY created_at DESC", (uid,)).fetchall()
    try:
        repair_sale_statuses(db, uid)
        db.commit()
        bills = db.execute(
            "SELECT * FROM sales_bills WHERE user_id=%s ORDER BY created_at DESC", (uid,)).fetchall()
    except Exception:
        db.rollback()
    loy_map = loyalty_map(db, uid)
    db.close()
    cmap = {}
    for r in saved:
        ph = re.sub(r'\D', '', r['phone'] or '')[-10:]
        if len(ph) != 10:
            continue
        cmap[ph] = {
            'name': r['name'] or 'CASH', 'phone': ph,
            'bills': [], 'total_business': 0.0, 'total_due': 0.0, 'total_paid': 0.0
        }
    for b in bills:
        ph = re.sub(r'\D', '', b['customer_phone'] or '')[-10:]
        if len(ph) != 10:
            continue
        if ph not in cmap:
            cmap[ph] = {
                'name': b['customer_name'] or 'CASH', 'phone': ph,
                'bills': [], 'total_business': 0.0, 'total_due': 0.0, 'total_paid': 0.0
            }
        bd = enrich_sale_bill(b)
        try:
            lines = json.loads(b['items'] or '[]')
            bd['lines'] = lines if isinstance(lines, list) else []
        except Exception:
            bd['lines'] = []
        cmap[ph]['bills'].append(bd)
        if bd['paid_status'] != 'Returned':
            cmap[ph]['total_business'] += float(b['total'] or 0)
            cmap[ph]['total_paid'] += bd['collected']
            cmap[ph]['total_due'] += bd['balance']
        if b['customer_name'] and not cmap[ph]['name']:
            cmap[ph]['name'] = b['customer_name']
    customers = sorted(cmap.values(), key=lambda x: (-x['total_due'], x['name'] or ''))
    for c in customers:
        c['points'] = int(loy_map.get(c['phone'], 0))
    return render_template(
        'sales_customers.html', customers=customers, user=user, status=st, days_left=dl,
        bills_json=json.dumps([dict(b) for c in customers for b in c['bills']], default=str))

@app.route('/api/loyalty')
@active_required
def api_loyalty():
    phone = loyalty_phone(request.args.get('phone') or '')
    db = get_db()
    info = loyalty_info(db, session['user_id'], phone)
    db.close()
    return jsonify(info)

@app.route('/api/customers/search')
@active_required
def customer_search():
    q = request.args.get('q', '').strip().upper()
    if len(q) < 2: return jsonify([])
    db = get_db()
    rows = db.execute(
        "SELECT DISTINCT customer_name, customer_phone FROM repair_jobs"
        " WHERE user_id=%s AND (UPPER(customer_name) LIKE %s OR customer_phone LIKE %s)"
        " ORDER BY customer_name LIMIT 10",
        (session['user_id'], f'%{q}%', f'%{q}%')).fetchall()
    db.close()
    return jsonify([{'name': r['customer_name'], 'phone': r['customer_phone']} for r in rows])

@app.route('/customers')
@active_required
def customers():
    db = get_db()
    user     = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    all_jobs = db.execute("SELECT * FROM repair_jobs WHERE user_id=%s ORDER BY created_at DESC", (session['user_id'],)).fetchall()
    all_invs = db.execute("SELECT id, job_id, total, advance_amount, paid, due_date FROM invoices WHERE user_id=%s", (session['user_id'],)).fetchall()
    loy_map = loyalty_map(db, session['user_id'])
    db.close()
    inv_map     = {r['job_id']: r['id'] for r in all_invs}
    inv_balance = {r['job_id']: {'balance': max(0, float(r['total'] or 0) - float(r['advance_amount'] or 0)),
                                  'due_date': r['due_date'], 'paid': r['paid']} for r in all_invs}
    customer_map = {}
    for job in all_jobs:
        ph = job['customer_phone']
        if ph not in customer_map:
            customer_map[ph] = {'name': job['customer_name'], 'phone': ph, 'jobs': [], 'total_business': 0, 'total_due': 0}
        job_dict = dict(job)
        job_dict['inv_id'] = inv_map.get(job['id'])
        inv_info = inv_balance.get(job['id'])
        job_dict['inv_balance'] = inv_info['balance'] if inv_info else 0
        job_dict['inv_due_date'] = inv_info['due_date'] if inv_info else None
        customer_map[ph]['jobs'].append(job_dict)
        if job['status'] == 'Delivered' and job['cost']:
            customer_map[ph]['total_business'] += float(job['cost'])
        if job['status'] not in ('Delivered', 'Cancelled') and job['cost']:
            due = float(job['cost']) - float(job['advance_amount'] or 0)
            if due > 0: customer_map[ph]['total_due'] += due
        if job['status'] == 'Delivered' and inv_info and inv_info['balance'] > 0.01:
            customer_map[ph]['total_due'] += inv_info['balance']
    for c in customer_map.values():
        c['points'] = int(loy_map.get(loyalty_phone(c['phone']), 0))
    return render_template('customers.html',
                           customers=sorted(customer_map.values(), key=lambda x: x['total_business'], reverse=True),
                           user=user, status=subscription_status(user), days_left=days_left(user))

REPORT_STATUSES = ('All', 'Received', 'Diagnosing', 'Repairing', 'Ready', 'Not Ready', 'Delivered', 'Cancelled', 'Rework')

def _parse_ymd(s):
    try:
        return datetime.strptime((s or '')[:10], '%Y-%m-%d').date()
    except ValueError:
        return None

def load_shop_report(db, uid, from_d, to_d, job_status):
    from_s, to_s = from_d.isoformat(), to_d.isoformat()
    created = f"AND {sql_date('created_at')}>=%s AND {sql_date('created_at')}<=%s"
    created_p = (from_s, to_s)
    st_sql, st_p = ('', ())
    if job_status and job_status != 'All':
        st_sql, st_p = (' AND status=%s', (job_status,))

    def count(where, params=()):
        return db.execute(
            f"SELECT COUNT(*) FROM repair_jobs WHERE user_id=%s {where}", (uid,) + params).fetchone()[0]

    def revenue(where, params=()):
        r = db.execute(
            f"SELECT SUM(cost) FROM repair_jobs WHERE user_id=%s {where}", (uid,) + params).fetchone()[0]
        return float(r or 0)

    base = created + st_sql
    base_p = created_p + st_p
    today_str = datetime.now(IST).strftime('%Y-%m-%d')
    deliv = (
        f"(CASE WHEN delivery_date IS NOT NULL AND delivery_date!='' "
        f"THEN {sql_date('delivery_date')} ELSE {sql_date('created_at')} END)"
    )
    stats = {
        'total':     count(base, base_p),
        'pending':   count(base + " AND status NOT IN ('Delivered','Cancelled')", base_p),
        'delivered': count(base + " AND status='Delivered'", base_p),
        'cancelled': count(base + " AND status='Cancelled'", base_p),
        'rework':    count(base + " AND status='Rework'", base_p),
        'ready':     count(base + " AND status='Ready'", base_p),
        'partial':   count(base + " AND paid_status='Partial'", base_p),
        'overdue':   count(
            base + " AND status NOT IN ('Delivered','Cancelled') AND expected_return IS NOT NULL AND expected_return!='' AND expected_return<%s",
            base_p + (today_str,)),
        'period_revenue': revenue(
            f"AND status='Delivered' AND {deliv}>=%s AND {deliv}<=%s" + st_sql,
            (from_s, to_s) + st_p),
        'booked_value': revenue(base, base_p),
    }
    jobs = db.execute(
        f"SELECT * FROM repair_jobs WHERE user_id=%s {base} ORDER BY created_at DESC LIMIT 200",
        (uid,) + base_p).fetchall()
    return stats, jobs

def report_filter_from_request():
    today = datetime.now(IST).date()
    from_d = _parse_ymd(request.args.get('from', ''))
    to_d = _parse_ymd(request.args.get('to', ''))
    job_status = (request.args.get('job_status') or 'All').strip()
    if job_status not in REPORT_STATUSES:
        job_status = 'All'
    missing = not from_d or not to_d
    if not from_d:
        if session.get('app_area') == 'sales':
            from_d = today
        else:
            from_d = today.replace(day=1)
    if not to_d:
        to_d = today
    if from_d > to_d:
        from_d, to_d = to_d, from_d
    return from_d, to_d, job_status, missing

@app.route('/reports')
@active_required
def reports():
    db = get_db()
    uid = session['user_id']
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    from_d, to_d, job_status, missing = report_filter_from_request()
    if missing and request.args:
        flash('From date and To date are required.', 'error')
    stats, jobs = load_shop_report(db, uid, from_d, to_d, job_status)
    sales = load_sales_stats(db, uid, from_d.isoformat(), to_d.isoformat())
    db.close()
    today = datetime.now(IST).date()
    week_start = today - timedelta(days=today.weekday())
    return render_template(
        'reports.html', user=user, stats=stats, report_jobs=jobs, sales=sales,
        from_date=from_d.isoformat(), to_date=to_d.isoformat(), job_status=job_status,
        statuses=REPORT_STATUSES, today=today.isoformat(),
        week_start=week_start.isoformat(), month_start=today.replace(day=1).isoformat(),
        year_start=today.replace(month=1, day=1).isoformat(),
        status=subscription_status(user), days_left=days_left(user))

@app.route('/reports/print')
@active_required
def reports_print():
    from_d, to_d, job_status, missing = report_filter_from_request()
    if missing:
        flash('From date and To date are required to download the report.', 'error')
        return redirect(url_for('reports'))
    db = get_db()
    uid = session['user_id']
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    stats, jobs = load_shop_report(db, uid, from_d, to_d, job_status)
    sales = load_sales_stats(db, uid, from_d.isoformat(), to_d.isoformat())
    db.close()
    return render_template(
        'print_report.html', user=user, stats=stats, report_jobs=jobs, sales=sales,
        from_date=from_d.isoformat(), to_date=to_d.isoformat(), job_status=job_status)

@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    if request.method == 'POST':
        confirm_pw = request.form.get('settings_password', '') or request.form.get('current_password', '')
        if not confirm_pw or not verify_pw(user['password'], confirm_pw):
            db.close()
            flash('Password is incorrect. Settings were not saved.' if confirm_pw else 'Enter your current password to save settings.', 'error')
            return redirect(url_for('settings'))
        shop_name          = request.form.get('shop_name', '').strip().upper()
        door_no            = request.form.get('door_no', '').strip().upper()
        street             = request.form.get('street', '').strip().upper()
        city               = request.form.get('city', '').strip().upper()
        pincode            = request.form.get('pincode', '').strip().upper()
        gstin              = request.form.get('gstin', '').strip().upper()
        state              = request.form.get('state', '').strip().upper()
        auto_state = _gst_state_from_gstin(gstin)
        if auto_state:
            state = auto_state
        addr_parts = [p for p in [door_no, street] if p]
        addr_line1 = ', '.join(addr_parts)
        addr_line2 = city + (' - ' + pincode if pincode else '')
        address = '\n'.join([l for l in [addr_line1, addr_line2] if l])
        google_review_link = request.form.get('google_review_link', '').strip()
        shop_open_time = (request.form.get('shop_open_time') or '09:00').strip()[:5]
        shop_close_time = (request.form.get('shop_close_time') or '21:00').strip()[:5]
        if not re.match(r'^\d{2}:\d{2}$', shop_open_time):
            shop_open_time = '09:00'
        if not re.match(r'^\d{2}:\d{2}$', shop_close_time):
            shop_close_time = '21:00'
        new_pw             = request.form.get('new_password', '')
        if new_pw and len(new_pw) < 6:
            db.close()
            flash('New password must be at least 6 characters.', 'error')
            return redirect(url_for('settings'))
        logo_data = None
        if 'logo' in request.files:
            f = request.files['logo']
            if f and f.filename:
                mime = f.content_type or 'image/png'
                logo_data = 'data:' + mime + ';base64,' + base64.b64encode(f.read()).decode()
        db.execute("UPDATE users SET shop_name=%s,address=%s,door_no=%s,street=%s,city=%s,pincode=%s,gstin=%s,state=%s,google_review_link=%s,shop_open_time=%s,shop_close_time=%s WHERE id=%s",
                   (shop_name, address, door_no, street, city, pincode, gstin, state, google_review_link, shop_open_time, shop_close_time, session['user_id']))
        if new_pw:
            db.execute("UPDATE users SET password=%s WHERE id=%s", (hash_pw(new_pw), session['user_id']))
        else:
            _upgrade_pw_if_needed(db, 'users', session['user_id'], user['password'], confirm_pw)
        if logo_data:
            db.execute("UPDATE users SET logo=%s WHERE id=%s", (logo_data, session['user_id']))
        db.commit(); db.close()
        session['shop_name'] = shop_name
        flash('Settings saved!', 'success')
        return redirect(url_for('settings'))
    broadcasts = [dict(r) for r in db.execute("SELECT * FROM global_notif_history ORDER BY created_at DESC").fetchall()]
    db.close()
    limits = plan_limits(user)
    return render_template(
        'settings.html', user=user, status=subscription_status(user), days_left=days_left(user),
        limits=limits, broadcasts=broadcasts)

def _load_team_context(db, user, from_d=None, to_d=None):
    staff_rows = db.execute(
        "SELECT * FROM shop_staff WHERE owner_id=%s ORDER BY id",
        (user['id'],)).fetchall()
    devices = [dict(d) for d in db.execute(
        "SELECT id,token,label,ip_address,staff_id,last_seen,created_at FROM shop_devices WHERE owner_id=%s ORDER BY last_seen DESC",
        (user['id'],)).fetchall()]
    staff_panel = load_staff_panel(db, user['id'], staff_rows, devices, from_d, to_d)
    staff_names = {s['id']: s['name'] for s in staff_rows}
    return staff_panel, devices, staff_names

@app.route('/team')
@login_required
def shop_team():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    y, m = parse_perf_month(request.args.get('month'))
    from_d, to_d = month_span(y, m)
    staff_panel, devices, staff_names = _load_team_context(db, user, from_d, to_d)
    ranked = sorted(staff_panel, key=lambda x: -float(((x.get('month') or {}).get('sales_collected') or 0)))
    for i, s in enumerate(ranked, 1):
        s['rank'] = i
    owner_month = load_staff_month_sheet(db, user['id'], None, from_d, to_d, with_lines=False)
    alerts = [dict(r) for r in db.execute(
        "SELECT id,staff_id,staff_name,device_label,ip_address,created_at FROM shop_login_alerts "
        "WHERE owner_id=%s AND COALESCE(seen,0)=0 ORDER BY id DESC LIMIT 20", (user['id'],)).fetchall()]
    shop_logs = [dict(r) for r in db.execute(
        "SELECT staff_name,action,ref_type,ref_id,detail,created_at FROM shop_staff_log "
        "WHERE owner_id=%s ORDER BY id DESC LIMIT 50", (user['id'],)).fetchall()]
    attendance_today = staff_attendance_today(db, user['id'], staff_panel, user=dict(user))
    db.close()
    perf_month = f'{y:04d}-{m:02d}'
    return render_template(
        'team.html', user=user, status=subscription_status(user), days_left=days_left(user),
        staff_rows=staff_panel, devices=devices, staff_names=staff_names, limits=plan_limits(user),
        device_token=session.get('device_token') or '',
        staff_json=json.dumps(staff_panel, default=str),
        perf_month=perf_month, perf_label=date(y, m, 1).strftime('%B %Y'),
        perf_from=from_d, perf_to=to_d, owner_month=owner_month, alerts=alerts, shop_logs=shop_logs,
        attendance_today=attendance_today)

def _owner_staff_month(sid=None, owner_work=False):
    y, m = parse_perf_month(request.args.get('month'))
    from_d, to_d = month_span(y, m)
    db = get_db()
    uid = session['user_id']
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    if owner_work:
        sheet = load_staff_month_sheet(db, uid, None, from_d, to_d, with_lines=True)
        sheets = [{'staff': {'id': 0, 'name': 'OWNER', 'phone': user['phone'], 'email': user['email']}, 'sheet': sheet}]
    elif sid is not None:
        staff = _staff_for_owner(db, uid, sid)
        if not staff:
            db.close()
            flash('Sales person not found.', 'error')
            return None, redirect(url_for('shop_team'))
        flags = staff_flags(staff)
        sheet = load_staff_month_sheet(
            db, uid, sid, from_d, to_d, with_lines=True, commission_pct=flags['commission_pct'])
        attach_salary_to_sheet(db, uid, sid, from_d, to_d, sheet, flags)
        sheets = [{'staff': dict(staff), 'sheet': sheet}]
    else:
        rows = db.execute("SELECT * FROM shop_staff WHERE owner_id=%s ORDER BY id", (uid,)).fetchall()
        sheets = []
        for r in rows:
            flags = staff_flags(r)
            sheet = load_staff_month_sheet(
                db, uid, r['id'], from_d, to_d, with_lines=True, commission_pct=flags['commission_pct'])
            attach_salary_to_sheet(db, uid, r['id'], from_d, to_d, sheet, flags)
            sheets.append({'staff': dict(r), 'sheet': sheet})
    db.close()
    ctx = {
        'user': user, 'sheets': sheets, 'from_date': from_d, 'to_date': to_d,
        'perf_month': f'{y:04d}-{m:02d}', 'perf_label': date(y, m, 1).strftime('%B %Y'),
        'single': sid is not None or owner_work,
    }
    return ctx, None

@app.route('/team/performance/<int:sid>')
@login_required
def shop_staff_perf_print(sid):
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    ctx, err = _owner_staff_month(sid)
    if err:
        return err
    return render_template('print_staff_perf.html', **ctx)

@app.route('/team/performance/<int:sid>/csv')
@login_required
def shop_staff_perf_csv(sid):
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    ctx, err = _owner_staff_month(sid)
    if err:
        return err
    st = ctx['sheets'][0]
    name = st['staff']['name'] or 'Sales'
    return _csv_file(
        f"Perf_{name}_{ctx['perf_month']}.csv",
        _perf_csv_rows(name, st['sheet'], ctx['perf_label']))

@app.route('/team/performance/all')
@login_required
def shop_staff_perf_all_print():
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    ctx, err = _owner_staff_month(None)
    if err:
        return err
    if not ctx['sheets']:
        flash('Add a sales person first to download a performance sheet.', 'error')
        return redirect(url_for('shop_team'))
    return render_template('print_staff_perf.html', **ctx)

@app.route('/team/performance/all/csv')
@login_required
def shop_staff_perf_all_csv():
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    ctx, err = _owner_staff_month(None)
    if err:
        return err
    if not ctx['sheets']:
        flash('Add a sales person first to download a performance sheet.', 'error')
        return redirect(url_for('shop_team'))
    rows = []
    for i, st in enumerate(ctx['sheets']):
        if i:
            rows.append([])
        rows.extend(_perf_csv_rows(st['staff']['name'] or 'Sales', st['sheet'], ctx['perf_label']))
    shop = (ctx['user']['shop_name'] or 'Shop')
    return _csv_file(f"Perf_Team_{shop}_{ctx['perf_month']}.csv", rows)

@app.route('/team/performance/owner')
@login_required
def shop_staff_perf_owner_print():
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    ctx, err = _owner_staff_month(owner_work=True)
    if err:
        return err
    return render_template('print_staff_perf.html', **ctx)

@app.route('/team/performance/owner/csv')
@login_required
def shop_staff_perf_owner_csv():
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    ctx, err = _owner_staff_month(owner_work=True)
    if err:
        return err
    return _csv_file(
        f"Perf_OWNER_{ctx['perf_month']}.csv",
        _perf_csv_rows('OWNER', ctx['sheets'][0]['sheet'], ctx['perf_label']))

@app.route('/team/clock', methods=['POST'])
@login_required
@active_required
def shop_staff_clock():
    db = get_db()
    uid = session['user_id']
    src = _clock_payload()
    try:
        sid = int(src.get('staff_id') or session.get('staff_id') or 0)
    except (TypeError, ValueError):
        sid = 0
    going_in = (src.get('clock') or 'in') != 'out'
    wants_json = _clock_wants_json()

    def fail(msg, code=400):
        db.close()
        if wants_json:
            return jsonify(ok=False, error=msg), code
        flash(msg, 'error')
        return redirect(request.referrer or url_for('dashboard'))

    if session.get('shop_role') == 'salesperson':
        if sid != int(session.get('staff_id') or 0):
            db.close()
            return _owner_denied()
    else:
        row = db.execute("SELECT id FROM shop_staff WHERE id=%s AND owner_id=%s", (sid, uid)).fetchone()
        if not row:
            return fail('Sales person not found.')
    auto_close_overdue_shifts(db, uid)
    open_sh = db.execute(
        "SELECT id,clock_in FROM shop_shifts WHERE owner_id=%s AND staff_id=%s AND clock_out IS NULL ORDER BY id DESC LIMIT 1",
        (uid, sid)).fetchone()
    now = _now_str()
    is_self = session.get('shop_role') == 'salesperson'
    photo = _parse_selfie(src.get('photo'))
    geo = _parse_geo(src.get('lat'), src.get('lng'), src.get('accuracy') or src.get('acc'))
    client_time = (src.get('client_time') or '')[:19]
    if is_self:
        if not photo:
            return fail('Take a camera photo to mark attendance.')
        if not geo:
            return fail('Allow location to mark attendance.')
    source = 'self' if is_self else 'owner'
    if going_in:
        if open_sh:
            return fail('Already clocked in.')
        lat = geo[0] if geo else None
        lng = geo[1] if geo else None
        acc = geo[2] if geo else None
        db.execute(
            "INSERT INTO shop_shifts (owner_id,staff_id,clock_in,created_at,"
            "in_photo,in_lat,in_lng,in_acc,in_client_time,in_source) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (uid, sid, now, now, photo, lat, lng, acc, client_time or None, source))
        log_staff(db, uid, 'clock_in', 'shift', sid, now)
        msg = 'Clocked in.'
    else:
        if not open_sh:
            return fail('Not clocked in.')
        lat = geo[0] if geo else None
        lng = geo[1] if geo else None
        acc = geo[2] if geo else None
        db.execute(
            "UPDATE shop_shifts SET clock_out=%s, out_photo=%s, out_lat=%s, out_lng=%s, out_acc=%s,"
            "out_client_time=%s, out_source=%s WHERE id=%s AND owner_id=%s",
            (now, photo, lat, lng, acc, client_time or None, source, open_sh['id'], uid))
        log_staff(db, uid, 'clock_out', 'shift', sid, now)
        msg = 'Clocked out.'
    db.commit()
    db.close()
    if wants_json:
        return jsonify(ok=True, message=msg, at=now)
    flash(msg, 'success')
    dest = url_for('shop_team') if session.get('shop_role') != 'salesperson' else url_for('shop_attendance')
    return redirect(src.get('next') or dest)

@app.route('/team/lunch', methods=['POST'])
@login_required
@active_required
def shop_staff_lunch():
    db = get_db()
    uid = session['user_id']
    src = _clock_payload()
    try:
        sid = int(src.get('staff_id') or session.get('staff_id') or 0)
    except (TypeError, ValueError):
        sid = 0
    going_out = (src.get('lunch') or 'out') != 'in'
    wants_json = _clock_wants_json()

    def fail(msg, code=400):
        db.close()
        if wants_json:
            return jsonify(ok=False, error=msg), code
        flash(msg, 'error')
        return redirect(request.referrer or url_for('dashboard'))

    if session.get('shop_role') == 'salesperson':
        if sid != int(session.get('staff_id') or 0):
            db.close()
            return _owner_denied()
    else:
        row = db.execute("SELECT id FROM shop_staff WHERE id=%s AND owner_id=%s", (sid, uid)).fetchone()
        if not row:
            return fail('Sales person not found.')
    auto_close_overdue_shifts(db, uid)
    open_sh = db.execute(
        "SELECT id FROM shop_shifts WHERE owner_id=%s AND staff_id=%s AND clock_out IS NULL ORDER BY id DESC LIMIT 1",
        (uid, sid)).fetchone()
    open_lunch = staff_open_lunch(db, uid, sid)
    now = _now_str()
    source = 'self' if session.get('shop_role') == 'salesperson' else 'owner'
    if going_out:
        if not open_sh:
            return fail('Clock in first before lunch.')
        if open_lunch:
            return fail('Already on lunch.')
        db.execute(
            "INSERT INTO shop_lunch_breaks (owner_id,staff_id,shift_id,lunch_out,source,created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (uid, sid, open_sh['id'], now, source, now))
        log_staff(db, uid, 'lunch_out', 'shift', sid, now)
        msg = 'Lunch started.'
    else:
        if not open_lunch:
            return fail('Not on lunch.')
        db.execute(
            "UPDATE shop_lunch_breaks SET lunch_in=%s WHERE id=%s AND owner_id=%s",
            (now, open_lunch['id'], uid))
        log_staff(db, uid, 'lunch_in', 'shift', sid, now)
        msg = 'Back from lunch.'
    db.commit()
    db.close()
    if wants_json:
        return jsonify(ok=True, message=msg, at=now)
    flash(msg, 'success')
    dest = url_for('shop_team') if session.get('shop_role') != 'salesperson' else url_for('shop_attendance')
    return redirect(src.get('next') or dest)

@app.route('/team/reassign', methods=['POST'])
@login_required
@active_required
def shop_staff_reassign():
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    kind = (request.form.get('kind') or '').strip()
    try:
        item_id = int(request.form.get('item_id') or 0)
        new_sid = int(request.form.get('staff_id') or 0)
    except (TypeError, ValueError):
        item_id, new_sid = 0, 0
    db = get_db()
    uid = session['user_id']
    owner = db.execute("SELECT password FROM users WHERE id=%s", (uid,)).fetchone()
    confirm_pw = request.form.get('owner_password') or ''
    if not confirm_pw or not owner or not verify_pw(owner['password'], confirm_pw):
        db.close()
        flash('Owner password is incorrect. No change was made.', 'error')
        return redirect(url_for('shop_team'))
    if kind not in ('sale', 'job', 'invoice') or not item_id:
        db.close()
        flash('Pick a bill or job to reassign.', 'error')
        return redirect(url_for('shop_team'))
    if new_sid:
        st = db.execute("SELECT id,name FROM shop_staff WHERE id=%s AND owner_id=%s", (new_sid, uid)).fetchone()
        if not st:
            db.close()
            flash('Sales person not found.', 'error')
            return redirect(url_for('shop_team'))
        nsid, nname = st['id'], st['name']
    else:
        nsid, nname = None, 'OWNER'
    table = {'sale': 'sales_bills', 'job': 'repair_jobs', 'invoice': 'invoices'}[kind]
    row = db.execute(f"SELECT id FROM {table} WHERE id=%s AND user_id=%s", (item_id, uid)).fetchone()
    if not row:
        db.close()
        flash('Record not found.', 'error')
        return redirect(url_for('shop_team'))
    db.execute(f"UPDATE {table} SET staff_id=%s, staff_name=%s WHERE id=%s AND user_id=%s",
               (nsid, nname, item_id, uid))
    log_staff(db, uid, 'reassign', kind, item_id, nname, None, 'OWNER')
    db.commit()
    db.close()
    flash(f'Assigned to {nname}.', 'success')
    return redirect(url_for('shop_team', month=request.form.get('month') or ''))

@app.route('/team/alerts/seen', methods=['POST'])
@login_required
def shop_staff_alerts_seen():
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    db = get_db()
    db.execute("UPDATE shop_login_alerts SET seen=1 WHERE owner_id=%s", (session['user_id'],))
    db.commit()
    db.close()
    flash('Login alerts cleared.', 'success')
    return redirect(url_for('shop_team'))

@app.route('/settings/staff', methods=['POST'])
@login_required
@active_required
def shop_staff_add():
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    name = (request.form.get('name') or '').strip().upper()
    phone = re.sub(r'\D', '', request.form.get('phone') or '')[-10:]
    email = (request.form.get('email') or '').strip().lower()
    password = request.form.get('password') or ''
    job_kind = 'tech' if request.form.get('job_kind') == 'tech' else 'sales'
    if job_kind == 'tech':
        can_sale, can_collect, can_jobs = 0, 0, 1
    else:
        can_sale = 1 if request.form.get('can_sale') != '0' else 0
        can_collect = 1 if request.form.get('can_collect') != '0' else 0
        can_jobs = 1 if request.form.get('can_jobs') != '0' else 0
        if request.form.get('can_sale') is None:
            can_sale = can_collect = can_jobs = 1
    if not name or len(phone) != 10:
        flash('Sales person needs a name and 10-digit mobile.', 'error')
        return redirect(url_for('shop_team'))
    if email and not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        flash('Enter a valid email or leave it blank.', 'error')
        return redirect(url_for('shop_team'))
    if len(password) < 6:
        flash('Sales person password must be at least 6 characters.', 'error')
        return redirect(url_for('shop_team'))
    db = get_db()
    uid = session['user_id']
    owner = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    lim = plan_limits(owner)
    n = db.execute(
        "SELECT COUNT(*) FROM shop_staff WHERE owner_id=%s AND enabled=1", (uid,)).fetchone()[0]
    if int(n or 0) >= int(lim['staff'] or 0):
        db.close()
        flash(
            f'The {lim["label"]} plan allows {lim["staff"]} sales person login'
            f'{"s" if lim["staff"] != 1 else ""}. Upgrade to add more.', 'error')
        return redirect(url_for('shop_team'))
    if db.execute("SELECT id FROM users WHERE phone=%s", (phone,)).fetchone():
        db.close()
        flash('That mobile is already used by a shop account.', 'error')
        return redirect(url_for('shop_team'))
    if email and db.execute("SELECT id FROM users WHERE email=%s", (email,)).fetchone():
        db.close()
        flash('That email is already used by a shop account.', 'error')
        return redirect(url_for('shop_team'))
    taken = db.execute("SELECT id FROM shop_staff WHERE phone=%s", (phone,)).fetchone()
    if not taken and email:
        taken = db.execute("SELECT id FROM shop_staff WHERE email=%s", (email,)).fetchone()
    if taken:
        db.close()
        flash('That mobile or email is already used by a sales person.', 'error')
        return redirect(url_for('shop_team'))
    db.execute(
        "INSERT INTO shop_staff (owner_id,name,phone,email,password,enabled,job_kind,can_sale,can_collect,can_jobs,created_at) "
        "VALUES (%s,%s,%s,%s,%s,1,%s,%s,%s,%s,%s)",
        (uid, name, phone, email or None, hash_pw(password), job_kind, can_sale, can_collect, can_jobs, _now_str()))
    db.commit()
    db.close()
    flash(('Technician' if job_kind == 'tech' else 'Sales person') + ' added. They can sign in with their mobile and password.', 'success')
    return redirect(url_for('shop_team'))

@app.route('/settings/staff/<int:sid>', methods=['POST'])
@login_required
@active_required
def shop_staff_update(sid):
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    action = request.form.get('action') or ''
    db = get_db()
    uid = session['user_id']
    row = db.execute("SELECT * FROM shop_staff WHERE id=%s AND owner_id=%s", (sid, uid)).fetchone()
    if not row:
        db.close()
        flash('Sales person not found.', 'error')
        return redirect(url_for('shop_team'))
    if action not in ('toggle', 'password', 'delete', 'device', 'profile', 'perms', 'kick', 'salary'):
        db.close()
        return redirect(url_for('shop_team'))
    owner = db.execute("SELECT password FROM users WHERE id=%s", (uid,)).fetchone()
    confirm_pw = request.form.get('owner_password') or ''
    if not confirm_pw or not owner or not verify_pw(owner['password'], confirm_pw):
        db.close()
        flash('Owner password is incorrect. No change was made.', 'error')
        return redirect(url_for('shop_team'))
    if action == 'toggle':
        new_en = 0 if int(row['enabled'] or 0) else 1
        if new_en:
            owner = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
            lim = plan_limits(owner)
            n = db.execute(
                "SELECT COUNT(*) FROM shop_staff WHERE owner_id=%s AND enabled=1", (uid,)).fetchone()[0]
            if int(n or 0) >= int(lim['staff'] or 0):
                db.close()
                flash(f'The {lim["label"]} plan allows {lim["staff"]} active sales person login(s).', 'error')
                return redirect(url_for('shop_team'))
        db.execute("UPDATE shop_staff SET enabled=%s WHERE id=%s AND owner_id=%s", (new_en, sid, uid))
        db.commit()
        flash('Sales person ' + ('enabled' if new_en else 'disabled') + '.', 'success')
    elif action == 'password':
        pw = request.form.get('password') or ''
        if len(pw) < 6:
            db.close()
            flash('Password must be at least 6 characters.', 'error')
            return redirect(url_for('shop_team'))
        db.execute("UPDATE shop_staff SET password=%s WHERE id=%s AND owner_id=%s", (hash_pw(pw), sid, uid))
        db.commit()
        flash('Sales person password updated.', 'success')
    elif action == 'delete':
        db.execute("DELETE FROM shop_devices WHERE staff_id=%s AND owner_id=%s", (sid, uid))
        db.execute("DELETE FROM shop_staff WHERE id=%s AND owner_id=%s", (sid, uid))
        db.commit()
        flash('Sales person removed and their login device was cleared.', 'success')
    elif action == 'device':
        try:
            did = int(request.form.get('device_id') or 0)
        except (TypeError, ValueError):
            did = 0
        drow = db.execute(
            "SELECT id,token FROM shop_devices WHERE id=%s AND owner_id=%s AND staff_id=%s",
            (did, uid, sid)).fetchone()
        if not drow:
            db.close()
            flash('Device not found for this sales person.', 'error')
            return redirect(url_for('shop_team'))
        db.execute("DELETE FROM shop_devices WHERE id=%s AND owner_id=%s", (did, uid))
        db.commit()
        flash('Sales person device removed. They must sign in again on a free slot.', 'success')
    elif action == 'kick':
        db.execute("DELETE FROM shop_devices WHERE staff_id=%s AND owner_id=%s", (sid, uid))
        log_staff(db, uid, 'kick', 'staff', sid, row['name'], None, 'OWNER')
        db.commit()
        flash('All login devices for this person were signed out.', 'success')
    elif action == 'profile':
        name = (request.form.get('name') or '').strip().upper()
        phone = re.sub(r'\D', '', request.form.get('phone') or '')[-10:]
        email = (request.form.get('email') or '').strip().lower()
        if not name or len(phone) != 10:
            db.close()
            flash('Name and 10-digit mobile are required.', 'error')
            return redirect(url_for('shop_team'))
        if email and not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
            db.close()
            flash('Enter a valid email or leave it blank.', 'error')
            return redirect(url_for('shop_team'))
        if db.execute("SELECT id FROM users WHERE phone=%s", (phone,)).fetchone():
            db.close()
            flash('That mobile is already used by a shop account.', 'error')
            return redirect(url_for('shop_team'))
        taken = db.execute("SELECT id FROM shop_staff WHERE phone=%s AND id!=%s", (phone, sid)).fetchone()
        if not taken and email:
            taken = db.execute("SELECT id FROM shop_staff WHERE email=%s AND id!=%s", (email, sid)).fetchone()
        if taken:
            db.close()
            flash('That mobile or email is already used by another login.', 'error')
            return redirect(url_for('shop_team'))
        db.execute(
            "UPDATE shop_staff SET name=%s,phone=%s,email=%s WHERE id=%s AND owner_id=%s",
            (name, phone, email or None, sid, uid))
        log_staff(db, uid, 'edit_profile', 'staff', sid, name, None, 'OWNER')
        db.commit()
        flash('Contact details updated.', 'success')
    elif action == 'perms':
        job_kind = 'tech' if request.form.get('job_kind') == 'tech' else 'sales'
        can_sale = 1 if request.form.get('can_sale') == '1' else 0
        can_collect = 1 if request.form.get('can_collect') == '1' else 0
        can_jobs = 1 if request.form.get('can_jobs') == '1' else 0
        if job_kind == 'tech':
            can_sale, can_collect, can_jobs = 0, 0, 1
        try:
            target_sales = float(request.form.get('target_sales') or 0)
            target_jobs = int(float(request.form.get('target_jobs') or 0))
            commission_pct = float(request.form.get('commission_pct') or 0)
        except (TypeError, ValueError):
            target_sales, target_jobs, commission_pct = 0, 0, 0
        target_sales = max(0.0, target_sales)
        target_jobs = max(0, target_jobs)
        commission_pct = min(100.0, max(0.0, commission_pct))
        db.execute(
            '''UPDATE shop_staff SET job_kind=%s,can_sale=%s,can_collect=%s,can_jobs=%s,
               target_sales=%s,target_jobs=%s,commission_pct=%s WHERE id=%s AND owner_id=%s''',
            (job_kind, can_sale, can_collect, can_jobs, target_sales, target_jobs, commission_pct, sid, uid))
        log_staff(db, uid, 'perms', 'staff', sid, job_kind, None, 'OWNER')
        db.commit()
        flash('Role, permissions, and targets saved.', 'success')
    elif action == 'salary':
        try:
            salary_monthly = float(request.form.get('salary_monthly') or 0)
            salary_days = int(float(request.form.get('salary_days') or 26))
            salary_hours = float(request.form.get('salary_hours') or 8)
        except (TypeError, ValueError):
            salary_monthly, salary_days, salary_hours = 0.0, 26, 8.0
        salary_monthly = max(0.0, salary_monthly)
        salary_days = min(31, max(1, salary_days))
        if salary_hours <= 0:
            salary_hours = 8.0
        db.execute(
            "UPDATE shop_staff SET salary_monthly=%s,salary_days=%s,salary_hours=%s WHERE id=%s AND owner_id=%s",
            (salary_monthly, salary_days, salary_hours, sid, uid))
        log_staff(db, uid, 'salary', 'staff', sid, f'{salary_monthly:.2f}/{salary_days}d', None, 'OWNER')
        db.commit()
        flash('Salary details saved. Pay is calculated from attendance.', 'success')
    db.close()
    return redirect(url_for('shop_team'))

@app.route('/settings/devices/<int:did>/remove', methods=['POST'])
@login_required
@active_required
def shop_device_remove(did):
    if session.get('shop_role') == 'salesperson':
        return _owner_denied()
    db = get_db()
    uid = session['user_id']
    row = db.execute("SELECT token FROM shop_devices WHERE id=%s AND owner_id=%s", (did, uid)).fetchone()
    if not row:
        db.close()
        flash('Device not found.', 'error')
        return redirect(url_for('shop_team'))
    db.execute("DELETE FROM shop_devices WHERE id=%s AND owner_id=%s", (did, uid))
    db.commit()
    db.close()
    if row['token'] == session.get('device_token'):
        session.clear()
        flash('This device was removed. Sign in again.', 'error')
        return redirect(url_for('login'))
    flash('Device removed. That browser must sign in again if a slot is free.', 'success')
    return redirect(url_for('shop_team'))

@app.route('/verify_imei_pin', methods=['POST'])
@login_required
def verify_imei_pin():
    data = request.get_json(force=True)
    pin = data.get('pin', '').strip()
    db = get_db()
    user = db.execute("SELECT imei_skip_pin FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    db.close()
    if not user or not user['imei_skip_pin']:
        return jsonify({'error': 'No PIN configured. Contact your admin.'}), 400
    if pin.upper() == (user['imei_skip_pin'] or '').upper():
        return jsonify({'ok': True})
    return jsonify({'error': 'Incorrect PIN. Please check with your admin.'}), 400

@app.route('/admin/shops/<int:uid>/imei_toggle', methods=['POST'])
@admin_required
def admin_imei_toggle(uid):
    data = request.get_json(force=True)
    action = data.get('action')
    db = get_db()
    if action == 'disable':
        pin = data.get('pin', '').strip()
        if not pin:
            db.close()
            return jsonify({'error': 'PIN is required to disable IMEI capture.'}), 400
        db.execute("UPDATE users SET imei_skip=1, imei_skip_pin=%s WHERE id=%s AND role='user'", (pin, uid))
        log_admin_shop(db, uid, 'imei', 'IMEI capture disabled · skip PIN set')
        db.commit(); db.close()
        return jsonify({'ok': True, 'imei_skip': 1})
    elif action == 'enable':
        db.execute("UPDATE users SET imei_skip=0, imei_skip_pin=NULL WHERE id=%s AND role='user'", (uid,))
        log_admin_shop(db, uid, 'imei', 'IMEI capture enabled · required on delivery')
        db.commit(); db.close()
        return jsonify({'ok': True, 'imei_skip': 0})
    db.close()
    return jsonify({'error': 'Invalid action'}), 400

@app.route('/admin/shops/<int:uid>/imei_status')
@admin_required
def admin_imei_status(uid):
    db = get_db()
    row = db.execute("SELECT imei_skip, imei_skip_pin FROM users WHERE id=%s AND role='user'", (uid,)).fetchone()
    db.close()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    pin = (row['imei_skip_pin'] or '').strip()
    return jsonify({
        'imei_skip': int(row['imei_skip'] or 0),
        'pin_set': bool(pin),
    })

def _admin_job_json(row):
    d = {}
    for k in row.keys():
        if k in ('password', 'totp_secret', 'imei_skip_pin', 'aadhar_number'):
            continue
        v = row[k]
        if hasattr(v, 'isoformat'):
            v = v.isoformat()
        d[k] = v
    imei = (d.get('imei') or '').strip()
    imei_b = (d.get('imei_billing') or '').strip()
    d['imei_display'] = imei_b or imei
    quote = d.get('quote_items') or ''
    if isinstance(quote, str) and quote.strip().startswith(('[', '{')):
        try:
            d['quote_parsed'] = json.loads(quote)
        except Exception:
            d['quote_parsed'] = []
    else:
        d['quote_parsed'] = []
    return d

def _admin_imei_search_rows(db, q):
    like = '%' + q + '%'
    sqls = [
        '''SELECT j.id, j.user_id, j.customer_name, j.customer_phone, j.device_model,
                    j.imei, j.imei_billing, j.issue, j.received_without, j.status,
                    j.cost, j.notes, j.expected_return, j.delivery_date, j.quote_items,
                    j.advance_amount, j.happy_code, j.created_at, j.paid_status,
                    u.shop_name, u.phone AS shop_phone, u.email AS shop_email,
                    u.city AS shop_city, u.address AS shop_address, u.gstin AS shop_gstin
             FROM repair_jobs j
             JOIN users u ON u.id = j.user_id
             WHERE UPPER(COALESCE(j.imei,'')) LIKE %s OR UPPER(COALESCE(j.imei_billing,'')) LIKE %s
             ORDER BY j.id DESC LIMIT 40''',
        '''SELECT j.id, j.user_id, j.customer_name, j.customer_phone, j.device_model,
                    j.imei, j.imei_billing, j.issue, j.status, j.cost, j.notes, j.created_at,
                    u.shop_name, u.phone AS shop_phone, u.email AS shop_email
             FROM repair_jobs j
             JOIN users u ON u.id = j.user_id
             WHERE UPPER(COALESCE(j.imei,'')) LIKE %s OR UPPER(COALESCE(j.imei_billing,'')) LIKE %s
             ORDER BY j.id DESC LIMIT 40''',
    ]
    last_err = None
    for sql in sqls:
        try:
            return db.execute(sql, (like, like)).fetchall()
        except Exception as e:
            last_err = e
            try:
                db.rollback()
            except Exception:
                pass
    raise last_err

@app.route('/admin/imei-search')
@admin_required
def admin_imei_search():
    return render_template('admin_imei_search.html', q=(request.args.get('q') or '').strip())

@app.route('/admin/imei-search/api')
@admin_required
def admin_imei_search_api():
    q = clean_imei_serial(request.args.get('q') or request.args.get('imei') or '')
    if len(q) < 4:
        return jsonify({'ok': False, 'error': 'Enter at least 4 characters of the IMEI / serial.', 'jobs': []})
    db = get_db()
    try:
        rows = _admin_imei_search_rows(db, q)
        jobs = [_admin_job_json(r) for r in rows]
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        db.close()
        return jsonify({'ok': False, 'error': str(e) or 'Search failed. Try again.', 'jobs': []}), 500
    db.close()
    return jsonify({'ok': True, 'q': q, 'count': len(jobs), 'jobs': jobs})

@app.route('/admin/imei-search/<int:job_id>/print')
@admin_required
def admin_imei_search_print(job_id):
    db = get_db()
    job = db.execute("SELECT * FROM repair_jobs WHERE id=%s", (job_id,)).fetchone()
    if not job:
        db.close()
        flash('Job not found.', 'error')
        return redirect(url_for('admin_imei_search'))
    user = db.execute("SELECT * FROM users WHERE id=%s", (job['user_id'],)).fetchone()
    db.close()
    return render_template('print_job_card.html', job=job, user=user)

def _admin_shop_public(u):
    data = dict(u)
    for k in ('password', 'totp_secret', 'imei_skip_pin'):
        data.pop(k, None)
    data['gstin'] = (_rg(u, 'gstin') or '').strip().upper()
    data['status'] = u.get('status') or subscription_status(u)
    data['days_left'] = u.get('days_left') if u.get('days_left') is not None else days_left(u)
    for k, v in list(data.items()):
        if hasattr(v, 'isoformat'):
            data[k] = v.isoformat()
    return data

@app.route('/admin/shops/<int:uid>/detail')
@admin_required
def admin_shop_detail(uid):
    db = get_db()
    u = db.execute("SELECT * FROM users WHERE id=%s AND role='user'", (uid,)).fetchone()
    if not u:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    u = dict(u)
    u['status'] = subscription_status(u)
    u['days_left'] = days_left(u)
    lim = plan_limits(u)
    u['plan_tier'] = lim.get('tier') or 'trial'
    u['staff_cap'] = int(lim.get('staff') or 0)
    u['extra_staff'] = int(_rg(u, 'extra_staff') or 0)
    u['device_cap'] = int(lim.get('devices') or 1)
    u['extra_devices'] = int(_rg(u, 'extra_devices') or 0)
    staff_rows = db.execute(
        "SELECT name, enabled FROM shop_staff WHERE owner_id=%s ORDER BY id", (uid,)).fetchall()
    u['staff_list'] = [{'name': r['name'] or '—', 'enabled': int(r['enabled'] or 0)} for r in staff_rows]
    u['staff_on'] = sum(1 for s in u['staff_list'] if s['enabled'])
    extras = _admin_shop_extras(db)
    u['last_login'] = str(extras['last_login'].get(uid) or '')[:19]
    u['last_login_ist'] = to_ist_filter(u['last_login']) if u['last_login'] else 'Never'
    u['devices_n'] = int(extras['devices'].get(uid) or 0)
    u['jobs_today'] = int(extras['jobs_today'].get(uid) or 0)
    u['sales_today'] = int(extras['sales_today'].get(uid) or 0)
    u['sales_today_amt'] = float(extras['sales_today_amt'].get(uid) or 0)
    u['open_shifts'] = int(extras['open_shifts'].get(uid) or 0)
    u['enabled'] = int(u.get('enabled') or 0)
    u['imei_skip'] = int(_rg(u, 'imei_skip') or 0)
    u['shop_open_time'] = (_rg(u, 'shop_open_time') or '09:00')[:5]
    u['shop_close_time'] = (_rg(u, 'shop_close_time') or '21:00')[:5]
    u['admin_notes'] = _rg(u, 'admin_notes') or ''
    collected = "COALESCE(paid_amount, CASE WHEN paid_status='Paid' THEN total ELSE 0 END)"
    perf = db.execute(
        f"""SELECT COUNT(*) AS total_jobs,
                   COUNT(CASE WHEN status='Delivered' THEN 1 END) AS delivered,
                   COUNT(CASE WHEN status='Cancelled' THEN 1 END) AS cancelled,
                   COUNT(CASE WHEN status NOT IN ('Delivered','Cancelled') THEN 1 END) AS active_jobs,
                   COALESCE(SUM(CASE WHEN paid_status='Paid' THEN cost ELSE 0 END),0) AS total_revenue,
                   COUNT(CASE WHEN paid_status='Unpaid' AND status NOT IN ('Delivered','Cancelled') THEN 1 END) AS unpaid_active
            FROM repair_jobs WHERE user_id=%s""", (uid,)).fetchone()
    p = dict(perf or {})
    try:
        srow = db.execute(
            f"""SELECT COUNT(CASE WHEN paid_status!='Returned' THEN 1 END) AS sales_bills,
                       COALESCE(SUM(CASE WHEN paid_status!='Returned' THEN {collected} ELSE 0 END),0) AS sales_collected,
                       COALESCE(SUM(CASE WHEN paid_status!='Returned' AND total > {collected} + 0.009 THEN total - {collected} ELSE 0 END),0) AS sales_credit,
                       COUNT(CASE WHEN paid_status='Returned' THEN 1 END) AS sales_returns
                FROM sales_bills WHERE user_id=%s""", (uid,)).fetchone()
        if srow:
            p.update(dict(srow))
    except Exception:
        db.rollback()
    db.close()
    return jsonify({'user': _admin_shop_public(u), 'perf': {k: (float(v) if isinstance(v, (int, float)) or hasattr(v, 'real') else v) for k, v in p.items()}})

@app.route('/admin/impersonate/<int:uid>', methods=['POST'])
@admin_required
def admin_impersonate(uid):
    db = get_db()
    if not admin_password_ok(db, _request_password()):
        db.close()
        return jsonify({'error': 'Admin password is incorrect.'}), 403
    shop = db.execute("SELECT * FROM users WHERE id=%s AND role='user'", (uid,)).fetchone()
    if not shop:
        db.close()
        return jsonify({'error': 'Shop not found'}), 404
    admin_id = session.get('user_id')
    log_admin_shop(db, uid, 'impersonate', f"Opened as owner · {shop['shop_name'] or uid}")
    db.commit()
    db.close()
    _apply_shop_session(shop, None, '')
    session['impersonator_id'] = admin_id
    session['impersonator_shop'] = shop['shop_name'] or 'Shop'
    return jsonify({'ok': True, 'redirect': url_for('dashboard')})

@app.route('/admin/stop-impersonate')
def admin_stop_impersonate():
    admin_id = session.get('impersonator_id')
    if not admin_id:
        return redirect(url_for('login'))
    db = get_db()
    admin = db.execute("SELECT * FROM users WHERE id=%s AND role='admin'", (admin_id,)).fetchone()
    db.close()
    session.clear()
    if not admin:
        return redirect(url_for('login'))
    _apply_shop_session(admin, None, '')
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/staff-seats/<int:uid>', methods=['POST'])
@admin_required
def admin_staff_seats(uid):
    data = request.get_json(silent=True) or {}
    try:
        delta = int(data.get('delta') or 0)
    except (TypeError, ValueError):
        delta = 0
    if delta not in (1, -1):
        return jsonify({'error': 'Invalid change'}), 400
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s AND role='user'", (uid,)).fetchone()
    if not user:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    extra = int(_rg(user, 'extra_staff') or 0) + delta
    owner = dict(user)
    owner['extra_staff'] = extra
    lim = plan_limits(owner)
    if extra < 0 or lim['staff'] < int(lim.get('plan_staff') or 0):
        db.close()
        return jsonify({'error': f'Already at the plan allowance ({lim.get("plan_staff") or 0} users).'}), 400
    if lim['staff'] > 20:
        db.close()
        return jsonify({'error': 'Maximum 20 sales person seats per shop.'}), 400
    enabled = db.execute(
        "SELECT COUNT(*) FROM shop_staff WHERE owner_id=%s AND enabled=1", (uid,)).fetchone()[0]
    if delta < 0 and int(enabled or 0) > lim['staff']:
        db.close()
        return jsonify({
            'error': f'This shop has {int(enabled)} active users. Disable extra logins on Team first.'
        }), 400
    db.execute("UPDATE users SET extra_staff=%s WHERE id=%s", (extra, uid))
    seat_word = 'seat' if abs(delta) == 1 else 'seats'
    log_admin_shop(
        db, uid, 'seats',
        f"{'Added' if delta > 0 else 'Removed'} {abs(delta)} {seat_word} · now {lim['staff']} max ({int(enabled or 0)} active)")
    db.commit()
    db.close()
    return jsonify({
        'ok': True,
        'extra_staff': extra,
        'staff_cap': lim['staff'],
        'staff_on': int(enabled or 0),
        'plan_staff': int(lim.get('plan_staff') or 0),
    })

@app.route('/admin/device-slots/<int:uid>', methods=['POST'])
@admin_required
def admin_device_slots(uid):
    data = request.get_json(silent=True) or {}
    try:
        delta = int(data.get('delta') or 0)
    except (TypeError, ValueError):
        delta = 0
    if delta not in (1, -1):
        return jsonify({'error': 'Invalid change'}), 400
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s AND role='user'", (uid,)).fetchone()
    if not user:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    extra = int(_rg(user, 'extra_devices') or 0) + delta
    owner = dict(user)
    owner['extra_devices'] = extra
    lim = plan_limits(owner)
    
    if extra < 0 or lim['devices'] < int(lim.get('plan_devices') or 1):
        db.close()
        return jsonify({'error': f'Already at the plan allowance ({lim.get("plan_devices") or 1} devices).'}), 400
    
    if lim['devices'] > 20:
        db.close()
        return jsonify({'error': 'Maximum 20 devices per shop.'}), 400
        
    devices_in_use = db.execute(
        "SELECT COUNT(*) FROM shop_devices WHERE owner_id=%s", (uid,)).fetchone()[0]
    
    if delta < 0 and int(devices_in_use or 0) > lim['devices']:
        db.close()
        return jsonify({
            'error': f'This shop has {int(devices_in_use)} active devices. Ask shop owner to remove devices from Team settings first.'
        }), 400
        
    db.execute("UPDATE users SET extra_devices=%s WHERE id=%s", (extra, uid))
    slot_word = 'slot' if abs(delta) == 1 else 'slots'
    log_admin_shop(
        db, uid, 'devices',
        f"{'Added' if delta > 0 else 'Removed'} {abs(delta)} device {slot_word} · now {lim['devices']} max ({int(devices_in_use or 0)} active)")
    db.commit()
    db.close()
    
    return jsonify({
        'ok': True,
        'extra_devices': extra,
        'device_cap': lim['devices'],
        'devices_on': int(devices_in_use or 0),
        'plan_devices': int(lim.get('plan_devices') or 1),
    })

def _admin_map(db, sql, params=(), key='user_id', val='n'):
    out = {}
    try:
        for r in db.execute(sql, params).fetchall():
            out[r[key]] = r[val]
    except Exception:
        db.rollback()
    return out

def _admin_shop_extras(db):
    today = _today_str()
    last_login = {}
    try:
        for r in db.execute(
            "SELECT user_id, MAX(created_at) AS last_login FROM login_logs "
            "WHERE status='success' GROUP BY user_id").fetchall():
            last_login[r['user_id']] = r['last_login']
    except Exception:
        db.rollback()
    collected = "COALESCE(paid_amount, CASE WHEN paid_status='Paid' THEN total ELSE 0 END)"
    sales_today_amt = {}
    try:
        for r in db.execute(
            f"SELECT user_id, COALESCE(SUM({collected}),0) AS amt FROM sales_bills "
            f"WHERE paid_status!='Returned' AND {sql_date('created_at')}=%s GROUP BY user_id",
            (today,)).fetchall():
            sales_today_amt[r['user_id']] = float(r['amt'] or 0)
    except Exception:
        db.rollback()
    jobs_today = _admin_map(
        db, f"SELECT user_id, COUNT(*) AS n FROM repair_jobs WHERE {sql_date('created_at')}=%s GROUP BY user_id",
        (today,))
    return {
        'last_login': last_login,
        'devices': _admin_map(db, "SELECT owner_id AS user_id, COUNT(*) AS n FROM shop_devices GROUP BY owner_id"),
        'low_stock': _admin_map(
            db,
            "SELECT user_id, COUNT(*) AS n FROM inventory_items "
            "WHERE active=1 AND reorder_qty>0 AND qty<=reorder_qty GROUP BY user_id"),
        'jobs_today': jobs_today,
        'jobs_today_total': sum(jobs_today.values()),
        'sales_today': _admin_map(
            db,
            f"SELECT user_id, COUNT(*) AS n FROM sales_bills "
            f"WHERE paid_status!='Returned' AND {sql_date('created_at')}=%s GROUP BY user_id",
            (today,)),
        'sales_today_amt': sales_today_amt,
        'open_shifts': _admin_map(
            db,
            "SELECT owner_id AS user_id, COUNT(*) AS n FROM shop_shifts "
            "WHERE clock_out IS NULL GROUP BY owner_id"),
    }

@app.route('/admin')
@admin_required
def admin_dashboard():
    db = get_db()
    users    = db.execute("SELECT * FROM users WHERE role='user' ORDER BY created_at DESC").fetchall()
    enriched = [{**dict(u), 'status': subscription_status(u), 'days_left': days_left(u)} for u in users]
    staff_n, staff_on, staff_names = {}, {}, {}
    try:
        for r in db.execute("SELECT owner_id, name, enabled FROM shop_staff ORDER BY id").fetchall():
            oid = r['owner_id']
            staff_n[oid] = staff_n.get(oid, 0) + 1
            if r['enabled']:
                staff_on[oid] = staff_on.get(oid, 0) + 1
            staff_names.setdefault(oid, []).append({
                'name': r['name'] or '—', 'enabled': int(r['enabled'] or 0)})
    except Exception:
        db.rollback()
    plan_counts = {'trial': 0, 'diamond': 0, 'platinum': 0, 'enterprise': 0}
    for u in enriched:
        lim = plan_limits(u)
        tier = lim.get('tier') or 'trial'
        u['plan_tier'] = tier
        if tier in plan_counts:
            plan_counts[tier] += 1
            
        u['staff_n'] = int(staff_n.get(u['id'], 0))
        u['staff_on'] = int(staff_on.get(u['id'], 0))
        u['staff_cap'] = int(lim['staff'] or 0)
        u['extra_staff'] = int(_rg(u, 'extra_staff') or 0)
        u['staff_list'] = staff_names.get(u['id'], [])
        u['extra_devices'] = int(_rg(u, 'extra_devices') or 0)
        u['device_cap'] = int(lim['devices'] or 1)
        u['plan_devices'] = int(lim.get('plan_devices') or 1)
    total    = len(users)

    platform = {}
    platform['total_jobs']     = db.execute("SELECT COUNT(*) FROM repair_jobs").fetchone()[0]
    platform['total_revenue']  = float(db.execute("SELECT COALESCE(SUM(total),0) FROM invoices").fetchone()[0])
    platform['total_invoices'] = db.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
    platform['jobs_today']     = db.execute(
        f"SELECT COUNT(*) FROM repair_jobs WHERE {sql_date('created_at')}=%s", (_today_str(),)).fetchone()[0]

    shop_activity = db.execute("""
        SELECT u.id, u.shop_name, u.phone,
               COUNT(DISTINCT r.id) AS job_count,
               COALESCE(SUM(CASE WHEN r.status='Delivered' THEN r.cost ELSE 0 END),0) AS revenue,
               COUNT(DISTINCT CASE WHEN r.status NOT IN ('Delivered','Cancelled') THEN r.id END) AS pending
        FROM users u
        LEFT JOIN repair_jobs r ON r.user_id=u.id
        WHERE u.role='user'
        GROUP BY u.id ORDER BY job_count DESC
    """).fetchall()

    expiring = [u for u in enriched if u['status'] in ('trial', 'active') and u['days_left'] <= 7]

    from datetime import date
    month_start = date.today().replace(day=1).isoformat()
    new_this_month = db.execute(
        f"SELECT COUNT(*) FROM users WHERE role='user' AND {sql_date('created_at')}>=%s", (month_start,)).fetchone()[0]

    # Per-shop performance
    quiet_shops, health_shops = [], []
    try:
        shop_perf = db.execute("""
            SELECT u.id, u.shop_name, u.phone,
                   COUNT(r.id) AS total_jobs,
                   COUNT(CASE WHEN r.status='Delivered' THEN 1 END) AS delivered,
                   COUNT(CASE WHEN r.status='Cancelled' THEN 1 END) AS cancelled,
                   COUNT(CASE WHEN r.status NOT IN ('Delivered','Cancelled') THEN 1 END) AS active_jobs,
                   COALESCE(SUM(CASE WHEN r.paid_status='Paid' THEN r.cost ELSE 0 END),0) AS total_revenue,
                   COUNT(CASE WHEN r.paid_status='Unpaid' AND r.status NOT IN ('Delivered','Cancelled') THEN 1 END) AS unpaid_active
            FROM users u
            LEFT JOIN repair_jobs r ON r.user_id=u.id
            WHERE u.role='user'
            GROUP BY u.id, u.shop_name, u.phone
            ORDER BY total_jobs DESC
        """).fetchall()
        shop_perf = [dict(s) for s in shop_perf]
        collected = "COALESCE(paid_amount, CASE WHEN paid_status='Paid' THEN total ELSE 0 END)"
        sales_map = {}
        try:
            for row in db.execute(f"""
                SELECT user_id,
                       COUNT(CASE WHEN paid_status!='Returned' THEN 1 END) AS sales_bills,
                       COALESCE(SUM(CASE WHEN paid_status!='Returned' THEN {collected} ELSE 0 END),0) AS sales_collected,
                       COALESCE(SUM(CASE WHEN paid_status!='Returned' AND total > {collected} + 0.009 THEN total - {collected} ELSE 0 END),0) AS sales_credit,
                       COUNT(CASE WHEN paid_status='Returned' THEN 1 END) AS sales_returns
                FROM sales_bills
                GROUP BY user_id
            """).fetchall():
                sales_map[row['user_id']] = dict(row)
        except Exception:
            db.rollback()
        for s in shop_perf:
            sm = sales_map.get(s['id']) or {}
            s['sales_bills'] = int(sm.get('sales_bills') or 0)
            s['sales_collected'] = float(sm.get('sales_collected') or 0)
            s['sales_credit'] = float(sm.get('sales_credit') or 0)
            s['sales_returns'] = int(sm.get('sales_returns') or 0)

        extras = _admin_shop_extras(db)
        quiet_cut = (datetime.now(timezone.utc) - timedelta(days=15)).strftime('%Y-%m-%d %H:%M:%S')
        today = _today_str()
        for u in enriched:
            u['gstin'] = (_rg(u, 'gstin') or '').strip().upper()
            uid = u['id']
            last = extras['last_login'].get(uid)
            u['last_login'] = str(last)[:19] if last else ''
            u['last_login_ist'] = to_ist_filter(u['last_login']) if u['last_login'] else 'Never'
            u['devices_n'] = int(extras['devices'].get(uid) or 0)
            u['low_stock'] = int(extras['low_stock'].get(uid) or 0)
            u['jobs_today'] = int(extras['jobs_today'].get(uid) or 0)
            u['sales_today'] = int(extras['sales_today'].get(uid) or 0)
            u['sales_today_amt'] = float(extras['sales_today_amt'].get(uid) or 0)
            u['open_shifts'] = int(extras['open_shifts'].get(uid) or 0)
            u['shop_open_time'] = (_rg(u, 'shop_open_time') or '09:00')[:5]
            u['shop_close_time'] = (_rg(u, 'shop_close_time') or '21:00')[:5]
            u['admin_notes'] = _rg(u, 'admin_notes') or ''
            u['city'] = _rg(u, 'city') or ''
            last = u['last_login']
            u['quiet'] = 1 if u['enabled'] and (not last or str(last) < quiet_cut) else 0
            u['filter_key'] = (
                'disabled' if not u['enabled'] else
                'expired' if u['status'] in ('trial_expired', 'expired') else
                'trial' if u['status'] == 'trial' else
                'active'
            )

        platform['sales_today'] = sum(u['sales_today'] for u in enriched)
        platform['sales_today_amt'] = sum(u['sales_today_amt'] for u in enriched)
        platform['sales_bills'] = sum(int(s.get('sales_bills') or 0) for s in shop_perf)
        platform['sales_collected'] = sum(float(s.get('sales_collected') or 0) for s in shop_perf)
        platform['sales_credit'] = sum(float(s.get('sales_credit') or 0) for s in shop_perf)
        platform['low_stock'] = sum(u['low_stock'] for u in enriched)
        platform['quiet'] = sum(u['quiet'] for u in enriched)
        platform['devices'] = sum(u['devices_n'] for u in enriched)
        platform['open_shifts'] = sum(u['open_shifts'] for u in enriched)

        sub_stats = {}
        for u in enriched:
            plan = u['subscription_plan']
            status = u['filter_key']
            if status == 'disabled': continue
            
            k = plan if plan else 'free_trial'
            if k not in sub_stats:
                sub_stats[k] = {'active': 0, 'expired': 0, 'trial': 0, 'revenue': 0.0, 'jobs': 0, 'name': plan_display_name(k)}
            
            if status == 'active':
                sub_stats[k]['active'] += 1
            elif status == 'trial':
                sub_stats[k]['trial'] += 1
            else:
                sub_stats[k]['expired'] += 1
                
            s_perf = next((s for s in shop_perf if s['id'] == u['id']), None)
            if s_perf:
                sub_stats[k]['revenue'] += float(s_perf.get('total_revenue', 0)) + float(s_perf.get('sales_collected', 0))
                sub_stats[k]['jobs'] += int(s_perf.get('total_jobs', 0))
                
        sorted_sub_stats = sorted(sub_stats.values(), key=lambda x: (x['active'] + x['trial'], x['revenue']), reverse=True)
        platform['jobs_today'] = extras['jobs_today_total'] if extras['jobs_today_total'] is not None else platform['jobs_today']

        quiet_shops = [u for u in enriched if u['quiet']]
        health_shops = sorted(enriched, key=lambda x: (x['quiet'] and -1 or 0, x['last_login'] or ''))

        trial_tracker = db.execute(f"""
            SELECT u.id, u.shop_name, u.phone, u.subscription_plan,
                   {sql_date('u.trial_start')} AS trial_start,
                   COUNT(r.id) AS jobs_done,
                   COALESCE(SUM(CASE WHEN r.paid_status='Paid' THEN r.cost ELSE 0 END),0) AS revenue
            FROM users u
            LEFT JOIN repair_jobs r ON r.user_id=u.id
            WHERE u.role='user'
            GROUP BY u.id, u.shop_name, u.phone, u.subscription_plan, u.trial_start
            ORDER BY jobs_done DESC
        """).fetchall()
        trial_tracker = [dict(t) for t in trial_tracker]

        brand_stats = db.execute("""
            SELECT UPPER(TRIM(device_brand)) AS brand, COUNT(*) AS cnt
            FROM repair_jobs
            WHERE device_brand IS NOT NULL AND TRIM(device_brand) != ''
            GROUP BY UPPER(TRIM(device_brand))
            ORDER BY cnt DESC LIMIT 10
        """).fetchall()
        brand_stats = [dict(b) for b in brand_stats]

        cutoff = (datetime.now(IST) - timedelta(days=30)).strftime('%Y-%m-%d')
        daily_jobs = db.execute(f"""
            SELECT {sql_date('created_at')} AS day, COUNT(*) AS cnt
            FROM repair_jobs
            WHERE {sql_date('created_at')} >= %s
            GROUP BY day ORDER BY day
        """, (cutoff,)).fetchall()
        daily_jobs = [dict(d) for d in daily_jobs]
    except Exception:
        db.rollback()
        shop_perf = []; trial_tracker = []; brand_stats = []; daily_jobs = []
        sorted_sub_stats = []
        quiet_shops = []; health_shops = []
        platform.setdefault('sales_today', 0)
        platform.setdefault('sales_today_amt', 0)
        platform.setdefault('sales_bills', 0)
        platform.setdefault('sales_collected', 0)
        platform.setdefault('sales_credit', 0)
        platform.setdefault('low_stock', 0)
        platform.setdefault('quiet', 0)
        platform.setdefault('devices', 0)
        platform.setdefault('open_shifts', 0)

    q = (request.args.get('q') or '').strip()
    status_f = (request.args.get('status') or 'all').strip().lower()
    plan_f = (request.args.get('plan') or 'all').strip().lower()
    try:
        page = max(1, int(request.args.get('page') or 1))
    except (TypeError, ValueError):
        page = 1
    page_size = 25
    filtered = list(enriched)
    if q:
        ql = q.lower()
        def _shop_blob(u):
            return ' '.join(str(u.get(k) or '') for k in (
                'shop_name', 'phone', 'email', 'gstin', 'city', 'address')).lower()
        filtered = [u for u in filtered if ql in _shop_blob(u)]
    if status_f == 'quiet':
        filtered = [u for u in filtered if u.get('quiet')]
    elif status_f == 'expiring':
        filtered = [u for u in filtered if u.get('status') in ('trial', 'active') and int(u.get('days_left') or 0) <= 7]
    elif status_f in ('trial', 'active', 'expired', 'disabled'):
        filtered = [u for u in filtered if (u.get('filter_key') or '') == status_f]
    if plan_f != 'all':
        filtered = [u for u in filtered if (u.get('plan_tier') or '') == plan_f]
    shop_total = len(filtered)
    pages = max(1, (shop_total + page_size - 1) // page_size)
    page = min(page, pages)
    shops = filtered[(page - 1) * page_size:page * page_size]
    for s in shops:
        s.pop('password', None)
        s.pop('totp_secret', None)
        s.pop('imei_skip_pin', None)

    db.close()

    return render_template('admin_dashboard.html', users=shops, total=total,
                           active_count=sum(1 for u in users if subscription_status(u) in ('trial', 'active')),
                           disabled_count=sum(1 for u in users if not u['enabled']),
                           expired_count=sum(1 for u in users if subscription_status(u) in ('trial_expired', 'expired')),
                           platform=platform, shop_activity=shop_activity,
                           expiring=expiring, new_this_month=new_this_month,
                           shop_perf=shop_perf, trial_tracker=trial_tracker,
                           brand_stats=brand_stats, daily_jobs=daily_jobs,
                           quiet_shops=quiet_shops, health_shops=health_shops, sub_stats=sorted_sub_stats,
                           plan_counts=plan_counts,
                           shop_q=q, shop_status=status_f, shop_plan=plan_f,
                           shop_page=page, shop_pages=pages, shop_shown=shop_total)

@app.route('/api/ack_notification', methods=['POST'])
def api_ack_notification():
    if not session.get('user_id'):
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    notif_id = (data.get('notif_id') or '').strip()
    if not notif_id:
        return jsonify({'error': 'Missing ID'}), 400
    db = get_db()
    row = db.execute("SELECT id FROM notification_acks WHERE notif_id=%s AND user_id=%s", (notif_id, session['user_id'])).fetchone()
    if not row:
        db.execute("INSERT INTO notification_acks (notif_id, user_id) VALUES (%s, %s)", (notif_id, session['user_id']))
        db.commit()
    db.close()
    return jsonify({'ok': True})

@app.route('/admin/broadcasts')
@admin_required
def admin_broadcasts():
    db = get_db()
    ack_query = """
        SELECT u.shop_name, u.phone, a.created_at
        FROM notification_acks a
        JOIN users u ON a.user_id = u.id
        WHERE a.notif_id = %s
        ORDER BY a.created_at DESC
    """
    notif_rows = db.execute("SELECT key, value FROM app_settings WHERE key IN ('global_notif_active', 'global_notif_msg', 'global_notif_id')").fetchall()
    global_notif = {r['key']: r['value'] for r in notif_rows}
    
    if global_notif.get('global_notif_id'):
        global_notif['acks'] = [dict(r) for r in db.execute(ack_query, (global_notif['global_notif_id'],)).fetchall()]
    
    history_rows = db.execute("SELECT * FROM global_notif_history ORDER BY created_at DESC").fetchall()
    history = []
    for r in history_rows:
        row = dict(r)
        acks = db.execute(ack_query, (row['id'],)).fetchall()
        row['acks'] = [dict(a) for a in acks]
        row['ack_count'] = len(acks)
        history.append(row)
    db.close()
    return render_template('admin_broadcasts.html', global_notif=global_notif, history=history)

@app.route('/admin/settings/notification', methods=['POST'])
@admin_required
def admin_save_notification():
    data = request.get_json(silent=True) or {}
    active = '1' if data.get('active') else '0'
    msg = (data.get('msg') or '').strip()
    db = get_db()
    
    notif_id = str(int(time.time()))
    
    db.execute("DELETE FROM app_settings WHERE key IN ('global_notif_active', 'global_notif_msg', 'global_notif_id')")
    db.execute("INSERT INTO app_settings (key, value) VALUES (%s, %s)", ('global_notif_active', active))
    db.execute("INSERT INTO app_settings (key, value) VALUES (%s, %s)", ('global_notif_msg', msg))
    db.execute("INSERT INTO app_settings (key, value) VALUES (%s, %s)", ('global_notif_id', notif_id))
    
    # Store history
    db.execute("INSERT INTO global_notif_history (id, msg) VALUES (%s, %s)", (notif_id, msg))
    
    db.commit()
    db.close()
    return jsonify({'ok': True})

@app.route('/admin/broadcasts/<notif_id>/delete', methods=['POST'])
@admin_required
def admin_delete_broadcast(notif_id):
    db = get_db()
    db.execute("DELETE FROM global_notif_history WHERE id=%s", (notif_id,))
    db.execute("DELETE FROM notification_acks WHERE notif_id=%s", (notif_id,))
    
    active_id = db.execute("SELECT value FROM app_settings WHERE key='global_notif_id'").fetchone()
    if active_id and active_id['value'] == notif_id:
        db.execute("DELETE FROM app_settings WHERE key IN ('global_notif_active', 'global_notif_msg', 'global_notif_id')")
        
    db.commit()
    db.close()
    flash('Broadcast deleted successfully.', 'success')
    return redirect(url_for('admin_broadcasts'))

@app.route('/admin/toggle/<int:uid>', methods=['POST'])
@admin_required
def admin_toggle(uid):
    db = get_db()
    user = db.execute("SELECT enabled FROM users WHERE id=%s", (uid,)).fetchone()
    if not user:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    new_state = 0 if user['enabled'] else 1
    if user['enabled'] and not admin_password_ok(db, _request_password()):
        db.close()
        return jsonify({'error': 'Admin password is incorrect.'}), 403
    db.execute("UPDATE users SET enabled=%s WHERE id=%s", (new_state, uid))
    log_admin_shop(db, uid, 'account', 'Account enabled' if new_state else 'Account disabled')
    db.commit(); db.close()
    return jsonify({'enabled': new_state})

@app.route('/admin/set_subscription/<int:uid>', methods=['POST'])
@admin_required
def admin_set_subscription(uid):
    plan = (request.form.get('plan') or '').strip().lower()
    spec = plan_spec(plan)
    days = spec.get('days') or 0
    tier, dur = parse_plan_code(plan)
    if not days or not dur:
        return jsonify({'error': 'Invalid plan'}), 400
    if dur in ('1y', '2y', '3y') and not tier:
        return jsonify({'error': 'Pick Diamond, Platinum, or Enterprise with the year plan.'}), 400
    start_date = datetime.now(IST).strftime('%Y-%m-%d')
    end_date   = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    db = get_db()
    db.execute("UPDATE users SET subscription_plan=%s,subscription_end=%s,enabled=1 WHERE id=%s", (plan, end_date, uid))
    db.execute("INSERT INTO subscription_history (user_id,plan,start_date,end_date,activated_at) VALUES (%s,%s,%s,%s,%s)",
               (uid, plan, start_date, end_date[:10], _now_str()))
    log_admin_shop(db, uid, 'plan', f"Plan set to {plan_display_name(plan)} · expires {end_date[:10]}")
    db.commit(); db.close()
    return jsonify({'success': True, 'end_date': end_date[:10]})

@app.route('/admin/subscription_history/<int:uid>')
@admin_required
def admin_sub_history(uid):
    db = get_db()
    rows = db.execute("SELECT * FROM subscription_history WHERE user_id=%s ORDER BY activated_at DESC", (uid,)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

@app.route('/admin/shop-log/<int:uid>')
@admin_required
def admin_shop_log(uid):
    db = get_db()
    try:
        rows = db.execute(
            "SELECT action, detail, admin_name, created_at FROM admin_shop_log "
            "WHERE shop_user_id=%s ORDER BY created_at DESC",
            (uid,)).fetchall()
    except Exception:
        db.rollback()
        rows = []
    db.close()
    out = []
    for r in rows:
        d = dict(r)
        d['at'] = to_ist_filter(d.get('created_at'))
        d['detail'] = _title_words(d.get('detail'))
        out.append(d)
    return jsonify(out)

@app.route('/admin/notes/<int:uid>', methods=['POST'])
@admin_required
def admin_shop_notes(uid):
    data = request.get_json(silent=True) or {}
    notes = (data.get('notes') or '')[:2000]
    db = get_db()
    user = db.execute("SELECT id FROM users WHERE id=%s AND role='user'", (uid,)).fetchone()
    if not user:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    db.execute("UPDATE users SET admin_notes=%s WHERE id=%s", (notes, uid))
    log_admin_shop(db, uid, 'notes', 'Admin notes updated' if notes.strip() else 'Admin notes cleared')
    db.commit(); db.close()
    return jsonify({'ok': True})

@app.route('/admin/extend/<int:uid>', methods=['POST'])
@admin_required
def admin_extend_access(uid):
    data = request.get_json(silent=True) or {}
    try:
        days = int(data.get('days') or 0)
    except (TypeError, ValueError):
        days = 0
    if days not in (7, 15, 30):
        return jsonify({'error': 'Pick 7, 15, or 30 days.'}), 400
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s AND role='user'", (uid,)).fetchone()
    if not user:
        db.close()
        return jsonify({'error': 'Not found'}), 404
    now = datetime.now(timezone.utc)
    current = _parse_dt(user['subscription_end'])
    trial_end = _trial_end(_parse_dt(user['trial_start']))
    base = now
    if current and current > base:
        base = current
    elif trial_end and trial_end > base:
        base = trial_end
    new_end = base + timedelta(days=days)
    end_s = new_end.strftime('%Y-%m-%d %H:%M:%S')
    db.execute("UPDATE users SET subscription_end=%s, enabled=1 WHERE id=%s", (end_s, uid))
    log_admin_shop(db, uid, 'plan', f"Access extended by {days} days · expires {new_end.date().isoformat()}")
    db.commit(); db.close()
    return jsonify({'ok': True, 'end_date': new_end.date().isoformat(), 'days_left': days_left({'subscription_end': end_s, 'trial_start': user['trial_start'], 'enabled': 1})})

@app.route('/admin/shops.csv')
@admin_required
def admin_shops_csv():
    db = get_db()
    users = db.execute("SELECT * FROM users WHERE role='user' ORDER BY shop_name").fetchall()
    extras = _admin_shop_extras(db)
    rows = [['Shop', 'Phone', 'Email', 'City', 'Status', 'Plan', 'Days left', 'Staff on',
             'Devices', 'Last login', 'Jobs today', 'Sales today', 'Low stock', 'Joined', 'Notes']]
    for u in users:
        uid = u['id']
        st = subscription_status(u)
        last = extras['last_login'].get(uid) or ''
        rows.append([
            u['shop_name'] or '', u['phone'] or '', u['email'] or '', _rg(u, 'city') or '',
            st, plan_display_name(u['subscription_plan']), days_left(u),
            '', extras['devices'].get(uid) or 0, to_ist_filter(last) if last else 'Never',
            extras['jobs_today'].get(uid) or 0, extras['sales_today'].get(uid) or 0,
            extras['low_stock'].get(uid) or 0, str(u['created_at'] or '')[:10],
            (_rg(u, 'admin_notes') or '').replace('\n', ' '),
        ])
    db.close()
    return _csv_file(f"MobileFix-Shops-{_today_str()}.csv", rows)

def wipe_shop_operational_data(db, uid):
    """Remove shop operations. Keeps the owner login, plan, and shop profile."""
    for sql in (
        "DELETE FROM invoices WHERE user_id=%s",
        "DELETE FROM repair_jobs WHERE user_id=%s",
        "DELETE FROM warranty_replacements WHERE user_id=%s",
        "DELETE FROM inventory_item_logs WHERE user_id=%s",
        "DELETE FROM stock_movements WHERE user_id=%s",
        "DELETE FROM inventory_items WHERE user_id=%s",
        "DELETE FROM sales_bills WHERE user_id=%s",
        "DELETE FROM sales_customers WHERE user_id=%s",
        "DELETE FROM loyalty_ledger WHERE user_id=%s",
        "DELETE FROM loyalty_accounts WHERE user_id=%s",
        "DELETE FROM shop_shifts WHERE owner_id=%s",
        "DELETE FROM shop_lunch_breaks WHERE owner_id=%s",
        "DELETE FROM shop_staff_log WHERE owner_id=%s",
        "DELETE FROM shop_login_alerts WHERE owner_id=%s",
        "DELETE FROM shop_devices WHERE owner_id=%s",
        "DELETE FROM shop_staff WHERE owner_id=%s",
        "DELETE FROM login_logs WHERE user_id=%s",
    ):
        db.execute(sql, (uid,))

SHOP_BACKUP_TABLES = (
    ('shop_staff', 'owner_id'),
    ('inventory_items', 'user_id'),
    ('repair_jobs', 'user_id'),
    ('invoices', 'user_id'),
    ('sales_bills', 'user_id'),
    ('sales_customers', 'user_id'),
    ('loyalty_accounts', 'user_id'),
    ('loyalty_ledger', 'user_id'),
    ('warranty_replacements', 'user_id'),
    ('stock_movements', 'user_id'),
    ('inventory_item_logs', 'user_id'),
    ('shop_shifts', 'owner_id'),
    ('shop_staff_log', 'owner_id'),
    ('shop_login_alerts', 'owner_id'),
    ('login_logs', 'user_id'),
)
SHOP_PROFILE_FIELDS = (
    'shop_name', 'address', 'logo', 'google_review_link',
    'door_no', 'street', 'city', 'pincode', 'imei_skip', 'imei_skip_pin',
    'shop_open_time', 'shop_close_time',
)

def _table_cols(db, table):
    try:
        if USE_PG:
            rows = db.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
                (table,)).fetchall()
            return [r['column_name'] for r in rows]
        rows = db.execute(f"PRAGMA table_info({table})").fetchall()
        return [r['name'] for r in rows]
    except Exception:
        db.rollback()
        return []

def _row_jsonable(row):
    out = {}
    for k in row.keys():
        v = row[k]
        if isinstance(v, memoryview):
            v = v.tobytes()
        if isinstance(v, (bytes, bytearray)):
            out[k] = {'__b64__': base64.b64encode(bytes(v)).decode('ascii')}
        elif isinstance(v, (datetime, date)):
            out[k] = str(v)
        else:
            out[k] = v
    return out

def _from_jsonable(v):
    if isinstance(v, dict) and '__b64__' in v:
        try:
            return base64.b64decode(v['__b64__'])
        except Exception:
            return None
    return v

def _as_int(v):
    try:
        if v is None or v == '':
            return None
        return int(v)
    except (TypeError, ValueError):
        return None

def build_shop_backup(db, user):
    uid = user['id']
    profile = {'id': uid}
    for k in SHOP_PROFILE_FIELDS + ('phone', 'email', 'enabled', 'subscription_plan', 'subscription_end', 'trial_start', 'created_at'):
        try:
            profile[k] = user[k]
        except Exception:
            profile[k] = None
    profile.pop('password', None)
    tables = {}
    for table, owner_col in SHOP_BACKUP_TABLES:
        cols = _table_cols(db, table)
        if not cols or owner_col not in cols:
            continue
        rows = db.execute(f"SELECT * FROM {table} WHERE {owner_col}=%s ORDER BY id", (uid,)).fetchall()
        tables[table] = [_row_jsonable(r) for r in rows]
    return {
        'format': 'mobilefix-shop-backup',
        'version': 1,
        'exported_at': datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
        'shop': profile,
        'tables': tables,
    }

def _insert_backup_row(db, table, cols, row, uid, owner_col):
    data = {}
    for c in cols:
        if c == 'id' or c in ('user_id', 'owner_id'):
            continue
        if c in row:
            data[c] = _from_jsonable(row[c])
    data[owner_col] = uid
    use = [c for c in cols if c != 'id' and c in data]
    if not use:
        return None
    sql = f"INSERT INTO {table} ({','.join(use)}) VALUES ({','.join(['%s'] * len(use))})"
    return db_insert_id(db, sql, tuple(data[c] for c in use))

def _remap_staff(row, staff_map):
    sid = _as_int(row.get('staff_id'))
    if sid is not None:
        row['staff_id'] = staff_map.get(sid)
    return row

def _remap_sale_items(raw, item_map):
    try:
        lines = json.loads(raw or '[]')
    except Exception:
        return raw
    if not isinstance(lines, list):
        return raw
    for ln in lines:
        if not isinstance(ln, dict):
            continue
        iid = _as_int(ln.get('item_id'))
        if iid is not None and iid in item_map:
            ln['item_id'] = item_map[iid]
    return json.dumps(lines)

def restore_shop_backup(db, uid, payload):
    tables = payload.get('tables') or {}
    colmap = {t: _table_cols(db, t) for t, _ in SHOP_BACKUP_TABLES}
    wipe_shop_operational_data(db, uid)

    shop = payload.get('shop') or {}
    sets, params = [], []
    user_cols = set(colmap.get('users') or _table_cols(db, 'users'))
    for k in SHOP_PROFILE_FIELDS:
        if k in shop and k in user_cols:
            sets.append(f'{k}=%s')
            params.append(_from_jsonable(shop[k]))
    if sets:
        params.append(uid)
        db.execute(f"UPDATE users SET {', '.join(sets)} WHERE id=%s", tuple(params))

    staff_map, item_map, job_map, inv_map, sale_map, warr_map = {}, {}, {}, {}, {}, {}

    for row in tables.get('shop_staff') or []:
        row = _remap_staff(dict(row), {})
        old = _as_int(row.get('id'))
        new = _insert_backup_row(db, 'shop_staff', colmap.get('shop_staff') or [], row, uid, 'owner_id')
        if old and new:
            staff_map[old] = new

    for row in tables.get('inventory_items') or []:
        row = dict(row)
        old = _as_int(row.get('id'))
        new = _insert_backup_row(db, 'inventory_items', colmap.get('inventory_items') or [], row, uid, 'user_id')
        if old and new:
            item_map[old] = new

    pending_orig = []
    for row in tables.get('repair_jobs') or []:
        row = _remap_staff(dict(row), staff_map)
        old = _as_int(row.get('id'))
        orig = _as_int(row.get('original_job_id'))
        row['original_job_id'] = None
        new = _insert_backup_row(db, 'repair_jobs', colmap.get('repair_jobs') or [], row, uid, 'user_id')
        if old and new:
            job_map[old] = new
        if orig and new:
            pending_orig.append((new, orig))
    for new_id, orig in pending_orig:
        mapped = job_map.get(orig)
        if mapped:
            db.execute("UPDATE repair_jobs SET original_job_id=%s WHERE id=%s AND user_id=%s",
                       (mapped, new_id, uid))

    for row in tables.get('invoices') or []:
        row = _remap_staff(dict(row), staff_map)
        jid = _as_int(row.get('job_id'))
        if jid is not None:
            row['job_id'] = job_map.get(jid)
        old = _as_int(row.get('id'))
        new = _insert_backup_row(db, 'invoices', colmap.get('invoices') or [], row, uid, 'user_id')
        if old and new:
            inv_map[old] = new

    for row in tables.get('sales_bills') or []:
        row = _remap_staff(dict(row), staff_map)
        if 'items' in row:
            row['items'] = _remap_sale_items(row.get('items'), item_map)
        old = _as_int(row.get('id'))
        new = _insert_backup_row(db, 'sales_bills', colmap.get('sales_bills') or [], row, uid, 'user_id')
        if old and new:
            sale_map[old] = new

    for row in tables.get('sales_customers') or []:
        _insert_backup_row(db, 'sales_customers', colmap.get('sales_customers') or [], dict(row), uid, 'user_id')

    for row in tables.get('loyalty_accounts') or []:
        _insert_backup_row(db, 'loyalty_accounts', colmap.get('loyalty_accounts') or [], dict(row), uid, 'user_id')

    for row in tables.get('loyalty_ledger') or []:
        row = dict(row)
        rid = _as_int(row.get('ref_id'))
        rtype = (row.get('ref_type') or '').lower()
        if rid is not None:
            if rtype in ('sale', 'sales'):
                row['ref_id'] = sale_map.get(rid, rid)
            elif rtype in ('invoice', 'inv'):
                row['ref_id'] = inv_map.get(rid, rid)
            elif rtype in ('job',):
                row['ref_id'] = job_map.get(rid, rid)
        _insert_backup_row(db, 'loyalty_ledger', colmap.get('loyalty_ledger') or [], row, uid, 'user_id')

    for row in tables.get('warranty_replacements') or []:
        row = dict(row)
        iid = _as_int(row.get('item_id'))
        if iid is not None:
            row['item_id'] = item_map.get(iid, iid)
        old = _as_int(row.get('id'))
        new = _insert_backup_row(db, 'warranty_replacements', colmap.get('warranty_replacements') or [], row, uid, 'user_id')
        if old and new:
            warr_map[old] = new

    for row in tables.get('stock_movements') or []:
        row = dict(row)
        iid = _as_int(row.get('item_id'))
        if iid is not None:
            row['item_id'] = item_map.get(iid, iid)
        rid = _as_int(row.get('ref_id'))
        rtype = (row.get('ref_type') or '').lower()
        if rid is not None:
            if rtype in ('sale', 'out'):
                row['ref_id'] = sale_map.get(rid, rid)
            elif rtype in ('take', 'receive', 'warranty'):
                row['ref_id'] = warr_map.get(rid, rid)
            elif rtype in ('job',):
                row['ref_id'] = job_map.get(rid, rid)
        _insert_backup_row(db, 'stock_movements', colmap.get('stock_movements') or [], row, uid, 'user_id')

    for row in tables.get('inventory_item_logs') or []:
        row = dict(row)
        iid = _as_int(row.get('item_id'))
        if iid is not None:
            row['item_id'] = item_map.get(iid, iid)
        _insert_backup_row(db, 'inventory_item_logs', colmap.get('inventory_item_logs') or [], row, uid, 'user_id')

    for row in tables.get('shop_shifts') or []:
        row = _remap_staff(dict(row), staff_map)
        _insert_backup_row(db, 'shop_shifts', colmap.get('shop_shifts') or [], row, uid, 'owner_id')

    for row in tables.get('shop_staff_log') or []:
        row = _remap_staff(dict(row), staff_map)
        _insert_backup_row(db, 'shop_staff_log', colmap.get('shop_staff_log') or [], row, uid, 'owner_id')

    for row in tables.get('shop_login_alerts') or []:
        row = _remap_staff(dict(row), staff_map)
        _insert_backup_row(db, 'shop_login_alerts', colmap.get('shop_login_alerts') or [], row, uid, 'owner_id')

    for row in tables.get('login_logs') or []:
        _insert_backup_row(db, 'login_logs', colmap.get('login_logs') or [], dict(row), uid, 'user_id')

    counts = {t: len(tables.get(t) or []) for t, _ in SHOP_BACKUP_TABLES}
    return counts

@app.route('/admin/delete/<int:uid>', methods=['POST'])
@admin_required
def admin_delete_user(uid):
    db = get_db()
    if not admin_password_ok(db, _request_password()):
        db.close()
        return jsonify({'error': 'Admin password is incorrect.'}), 403
    user = db.execute("SELECT id, role, shop_name FROM users WHERE id=%s", (uid,)).fetchone()
    if not user or user['role'] == 'admin':
        db.close()
        return jsonify({'error': 'Not found'}), 404
    shop_name = (user['shop_name'] or '') or f'Shop #{uid}'
    log_admin_shop(db, uid, 'delete', f'Shop deleted · {shop_name}')
    wipe_shop_operational_data(db, uid)
    db.execute("DELETE FROM subscription_history WHERE user_id=%s", (uid,))
    db.execute("DELETE FROM admin_shop_log WHERE shop_user_id=%s", (uid,))
    db.execute("DELETE FROM shop_page_views WHERE user_id=%s", (uid,))
    db.execute("DELETE FROM users WHERE id=%s", (uid,))
    db.commit(); db.close()
    return jsonify({'success': True})

@app.route('/admin/reset/<int:uid>', methods=['POST'])
@admin_required
def admin_factory_reset(uid):
    db = get_db()
    if not admin_password_ok(db, _request_password()):
        db.close()
        return jsonify({'error': 'Admin password is incorrect.'}), 403
    user = db.execute("SELECT id, role, shop_name FROM users WHERE id=%s", (uid,)).fetchone()
    if not user or user['role'] == 'admin':
        db.close()
        return jsonify({'error': 'Not found'}), 404
    wipe_shop_operational_data(db, uid)
    log_admin_shop(db, uid, 'factory_reset', f"Factory reset · operational data wiped for {user['shop_name'] or 'shop'}")
    db.commit(); db.close()
    return jsonify({'success': True})

@app.route('/admin/backup/<int:uid>', methods=['POST'])
@admin_required
def admin_shop_backup(uid):
    db = get_db()
    if not admin_password_ok(db, _request_password()):
        db.close()
        return jsonify({'error': 'Admin password is incorrect.'}), 403
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    if not user or user['role'] == 'admin':
        db.close()
        return jsonify({'error': 'Shop not found'}), 404
    payload = build_shop_backup(db, user)
    log_admin_shop(db, uid, 'backup', f"Backup downloaded · {user['shop_name'] or 'shop'}")
    db.commit()
    db.close()
    raw = json.dumps(payload, ensure_ascii=False, default=str)
    shop = re.sub(r'[^A-Za-z0-9]+', '-', (user['shop_name'] or 'shop')).strip('-')[:40] or 'shop'
    fname = f"{shop}-backup-{datetime.now(IST).strftime('%Y%m%d-%H%M')}.json"
    return Response(
        raw.encode('utf-8'),
        mimetype='application/json; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename="{fname}"'},
    )

@app.route('/admin/restore/<int:uid>', methods=['POST'])
@admin_required
def admin_shop_restore(uid):
    db = get_db()
    if not admin_password_ok(db, _request_password()):
        db.close()
        return jsonify({'error': 'Admin password is incorrect.'}), 403
    user = db.execute("SELECT id, role, shop_name FROM users WHERE id=%s", (uid,)).fetchone()
    if not user or user['role'] == 'admin':
        db.close()
        return jsonify({'error': 'Shop not found'}), 404
    f = request.files.get('file')
    if not f or not f.filename:
        db.close()
        return jsonify({'error': 'Choose a backup JSON file.'}), 400
    blob = f.read()
    if len(blob) > 40 * 1024 * 1024:
        db.close()
        return jsonify({'error': 'Backup file is too large (40 MB max).'}), 400
    try:
        payload = json.loads(blob.decode('utf-8'))
    except Exception:
        db.close()
        return jsonify({'error': 'Backup file is not valid JSON.'}), 400
    if not isinstance(payload, dict) or payload.get('format') != 'mobilefix-shop-backup':
        db.close()
        return jsonify({'error': 'This is not a MobileFix shop backup file.'}), 400
    try:
        counts = restore_shop_backup(db, uid, payload)
        jobs = counts.get('repair_jobs') or 0
        bills = counts.get('sales_bills') or 0
        staff = counts.get('shop_staff') or 0
        log_admin_shop(
            db, uid, 'restore',
            f"Restored from backup · {jobs} jobs, {bills} bills, {staff} staff")
        db.commit()
    except Exception as e:
        db.rollback()
        db.close()
        return jsonify({'error': 'Restore failed. Shop data was not changed.'}), 500
    db.close()
    return jsonify({'success': True, 'jobs': jobs, 'bills': bills, 'staff': staff})

@app.route('/admin/login-activity')
@admin_required
def admin_login_activity():
    db = get_db()
    try:
        # Per-shop login summary
        shop_logins = db.execute("""
            SELECT u.id, u.shop_name, u.phone,
                   COUNT(CASE WHEN l.status='success' THEN 1 END) AS total_logins,
                   COUNT(CASE WHEN l.status='failed' THEN 1 END) AS failed_logins,
                   MAX(CASE WHEN l.status='success' THEN l.created_at END) AS last_login,
                   MAX(CASE WHEN l.status='success' THEN l.ip_address END) AS last_ip,
                   MAX(CASE WHEN l.status='success' THEN l.user_agent END) AS last_ua
            FROM users u
            LEFT JOIN login_logs l ON l.user_id=u.id
            WHERE u.role='user'
            GROUP BY u.id, u.shop_name, u.phone
            ORDER BY (CASE WHEN MAX(CASE WHEN l.status='success' THEN l.created_at END) IS NULL THEN 1 ELSE 0 END),
                     MAX(CASE WHEN l.status='success' THEN l.created_at END) DESC
        """).fetchall()
        shop_logins = [dict(s) for s in shop_logins]

        # Security alerts: 3+ failed in last 24h
        alert_cutoff = (datetime.now(IST) - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        brute_force = db.execute("""
            SELECT u.shop_name, u.phone, COUNT(*) AS attempts
            FROM login_logs l
            JOIN users u ON u.id=l.user_id
            WHERE l.status='failed' AND l.created_at >= %s AND u.role='user'
            GROUP BY u.id, u.shop_name, u.phone
            HAVING COUNT(*) >= 3
            ORDER BY attempts DESC
        """, (alert_cutoff,)).fetchall()
        brute_force = [dict(b) for b in brute_force]

        # Inactive: no login in 15+ days but account exists
        inactive_cutoff = (datetime.now(IST) - timedelta(days=15)).strftime('%Y-%m-%d %H:%M:%S')
        inactive_shops = db.execute("""
            SELECT u.shop_name, u.phone,
                   MAX(l.created_at) AS last_seen
            FROM users u
            LEFT JOIN login_logs l ON l.user_id=u.id AND l.status='success'
            WHERE u.role='user' AND u.enabled=1
            GROUP BY u.id, u.shop_name, u.phone
            HAVING MAX(l.created_at) < %s OR MAX(l.created_at) IS NULL
            ORDER BY (CASE WHEN MAX(l.created_at) IS NULL THEN 0 ELSE 1 END), MAX(l.created_at) ASC
        """, (inactive_cutoff,)).fetchall()
        inactive_shops = [dict(i) for i in inactive_shops]

        # Currently active: success login in last 30 mins
        active_cutoff = (datetime.now(IST) - timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
        online_now = db.execute("""
            SELECT DISTINCT u.shop_name, u.phone, MAX(l.created_at) AS last_seen
            FROM login_logs l
            JOIN users u ON u.id=l.user_id
            WHERE l.status='success' AND l.created_at >= %s AND u.role='user'
            GROUP BY u.id, u.shop_name, u.phone
        """, (active_cutoff,)).fetchall()
        online_now = [dict(o) for o in online_now]

        # Hourly heatmap (0-23) for all time
        hourly_raw = db.execute(f"""
            SELECT {sql_hour('created_at')} AS hr, COUNT(*) AS cnt
            FROM login_logs
            WHERE status='success'
            GROUP BY {sql_hour('created_at')} ORDER BY hr
        """).fetchall()
        hourly_data = {}
        for r in hourly_raw:
            try:
                hourly_data[f"{int(r['hr']):02d}"] = int(r['cnt'] or 0)
            except (TypeError, ValueError):
                continue
        hourly = [{'hr': f'{h:02d}', 'cnt': hourly_data.get(f'{h:02d}', 0)} for h in range(24)]

        # Day of week heatmap
        dow_names = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat']
        dow_raw = db.execute(f"""
            SELECT {sql_dow('created_at')} AS dow, COUNT(*) AS cnt
            FROM login_logs WHERE status='success'
            GROUP BY {sql_dow('created_at')} ORDER BY dow
        """).fetchall()
        dow_data = {}
        for r in dow_raw:
            try:
                dow_data[int(r['dow'])] = int(r['cnt'] or 0)
            except (TypeError, ValueError):
                continue
        dow_stats = [{'day': dow_names[i], 'cnt': dow_data.get(i, 0)} for i in range(7)]

    except Exception as e:
        db.rollback()
        shop_logins = []; brute_force = []; inactive_shops = []
        online_now = []; hourly = []; dow_stats = []
    finally:
        db.close()

    return render_template('admin_login_activity.html',
                           shop_logins=shop_logins, brute_force=brute_force,
                           inactive_shops=inactive_shops, online_now=online_now,
                           hourly=hourly, dow_stats=dow_stats)


@app.route('/admin/page-activity')
@admin_required
def admin_page_activity():
    db = get_db()
    shop_id = request.args.get('shop', type=int)
    today = datetime.now(IST).strftime('%Y-%m-%d')
    today_start = today + ' 00:00:00'
    live_cut = (datetime.now(IST) - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
    views = []
    shops = []
    live_now = []
    filter_shop = None
    views_today = 0
    shops_today = 0
    unique_pages = 0
    try:
        if shop_id:
            filter_shop = db.execute(
                "SELECT id, shop_name, phone FROM users WHERE id=%s AND role='user'",
                (shop_id,)).fetchone()
            if filter_shop:
                filter_shop = dict(filter_shop)
        shop_sql = """
            SELECT u.id, u.shop_name, u.phone,
                   COUNT(v.id) AS total_views,
                   COUNT(CASE WHEN v.created_at >= %s THEN 1 END) AS views_today,
                   MAX(v.created_at) AS last_seen
            FROM users u
            LEFT JOIN shop_page_views v ON v.user_id=u.id
            WHERE u.role='user'
            GROUP BY u.id, u.shop_name, u.phone
            ORDER BY (CASE WHEN MAX(v.created_at) IS NULL THEN 1 ELSE 0 END),
                     MAX(v.created_at) DESC
        """
        shops = [dict(s) for s in db.execute(shop_sql, (today_start,)).fetchall()]
        last_rows = db.execute("""
            SELECT v.user_id, v.page, v.who, v.staff_name, v.path, v.created_at, v.ip_address
            FROM shop_page_views v
            WHERE v.id IN (SELECT MAX(id) FROM shop_page_views GROUP BY user_id)
        """).fetchall()
        last_map = {int(r['user_id']): dict(r) for r in last_rows}
        for s in shops:
            last = last_map.get(int(s['id'])) or {}
            s['last_page'] = last.get('page') or ''
            s['last_who'] = last.get('who') or ''
            s['last_staff'] = last.get('staff_name') or ''
            s['last_path'] = last.get('path') or ''
            s['last_ip'] = last.get('ip_address') or ''
            s['is_live'] = bool(s.get('last_seen') and str(s['last_seen']) >= live_cut)

        view_sql = """
            SELECT v.created_at, v.page, v.path, v.who, v.staff_name, v.ip_address,
                   v.endpoint, u.id AS shop_id, u.shop_name, u.phone
            FROM shop_page_views v
            JOIN users u ON u.id=v.user_id
            WHERE u.role='user'
        """
        params = []
        if filter_shop:
            view_sql += " AND v.user_id=%s"
            params.append(filter_shop['id'])
        view_sql += " ORDER BY v.created_at DESC, v.id DESC LIMIT 300"
        views = [dict(r) for r in db.execute(view_sql, tuple(params)).fetchall()]

        live_now = [s for s in shops if s.get('is_live')]
        if filter_shop:
            live_now = [s for s in live_now if int(s['id']) == int(filter_shop['id'])]
            shops = [s for s in shops if int(s['id']) == int(filter_shop['id'])]

        today_row = db.execute(
            "SELECT COUNT(*) AS n, COUNT(DISTINCT user_id) AS shops, COUNT(DISTINCT page) AS pages "
            "FROM shop_page_views WHERE created_at >= %s",
            (today_start,)).fetchone()
        views_today = int(today_row['n'] or 0) if today_row else 0
        shops_today = int(today_row['shops'] or 0) if today_row else 0
        unique_pages = int(today_row['pages'] or 0) if today_row else 0
        if filter_shop:
            t2 = db.execute(
                "SELECT COUNT(*) AS n, COUNT(DISTINCT page) AS pages "
                "FROM shop_page_views WHERE user_id=%s AND created_at >= %s",
                (filter_shop['id'], today_start)).fetchone()
            views_today = int(t2['n'] or 0) if t2 else 0
            shops_today = 1 if views_today else 0
            unique_pages = int(t2['pages'] or 0) if t2 else 0
    except Exception:
        db.rollback()
        views, shops, live_now = [], [], []
    finally:
        db.close()
    return render_template(
        'admin_page_activity.html',
        views=views, shops=shops, live_now=live_now, filter_shop=filter_shop,
        views_today=views_today, shops_today=shops_today, unique_pages=unique_pages)


@app.route('/admin/page-views/<int:uid>')
@admin_required
def admin_page_views(uid):
    db = get_db()
    try:
        rows = db.execute("""
            SELECT created_at, page, path, who, staff_name, ip_address, endpoint
            FROM shop_page_views WHERE user_id=%s
            ORDER BY created_at DESC, id DESC LIMIT 80
        """, (uid,)).fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception:
        db.rollback()
        return jsonify([])
    finally:
        db.close()

@app.route('/admin/clear-devices/<int:uid>', methods=['POST'])
@admin_required
def admin_clear_devices(uid):
    db = get_db()
    data = request.get_json() or {}
    ip = data.get('ip')
    ua = data.get('ua')
    try:
        if ip is not None and ua is not None:
            db.execute("DELETE FROM shop_devices WHERE owner_id=%s AND COALESCE(ip_address, '')=%s AND COALESCE(user_agent, '')=%s", (uid, ip, ua))
        else:
            db.execute("DELETE FROM shop_devices WHERE owner_id=%s", (uid,))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()


@app.route('/admin/login-history/<int:uid>')
@admin_required
def admin_login_history(uid):
    db = get_db()
    try:
        try:
            logs = db.execute("""
                SELECT status, ip_address, user_agent, created_at, fail_reason, device_type, location
                FROM login_logs WHERE user_id=%s
                ORDER BY created_at DESC LIMIT 50
            """, (uid,)).fetchall()
        except Exception:
            db.rollback()
            logs = db.execute("""
                SELECT status, ip_address, user_agent, created_at
                FROM login_logs WHERE user_id=%s
                ORDER BY created_at DESC LIMIT 50
            """, (uid,)).fetchall()
        try:
            active_devs = db.execute("SELECT ip_address, user_agent FROM shop_devices WHERE owner_id=%s", (uid,)).fetchall()
        except:
            active_devs = []

        return jsonify({
            'logs': [dict(l) for l in logs],
            'active_devices': [dict(d) for d in active_devs]
        })
    except Exception:
        return jsonify([])
    finally:
        db.close()


@app.route('/login/2fa', methods=['GET', 'POST'])
def login_2fa():
    uid = session.get('pending_2fa_uid')
    if not uid:
        return redirect(url_for('login'))
    if request.method == 'POST':
        code = request.form.get('code', '').strip().replace(' ', '')
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
        if user and user['totp_secret'] and pyotp.TOTP(user['totp_secret']).verify(code, valid_window=1):
            db.close()
            token = session.get('pending_device_token') or _device_token_from_request()
            if user['role'] != 'admin':
                db2 = get_db()
                err = _claim_device(db2, user, token, None)
                if err:
                    _log_login(db2, user['id'], user['phone'] or user['email'], 'device_limit', 'Device limit reached')
                    db2.commit()
                    db2.close()
                    flash(err, 'error')
                    return redirect(url_for('login'))
                db2.commit()
                db2.close()
            _apply_shop_session(user, None, token)
            return _login_redirect(user)
        _log_login(db, uid, (user['phone'] if user else ''), 'failed', 'Invalid 2FA code')
        db.close()
        flash('Invalid or expired code. Please try again.', 'error')
    return render_template('login_2fa.html')

@app.route('/categories')
@active_required
def manage_categories():
    db = get_db()
    uid = session['user_id']
    user, st, dl = _shop_ctx(db, uid)
    
    cats = [dict(r) for r in db.execute('SELECT id, name FROM inventory_categories WHERE user_id=%s ORDER BY name', (uid,)).fetchall()]
    all_sub = db.execute('''
        SELECT s.id, s.category_id, s.name, c.name as cat_name 
        FROM inventory_subcategories s
        JOIN inventory_categories c ON s.category_id = c.id
        WHERE s.user_id=%s ORDER BY s.name
    ''', (uid,)).fetchall()
    
    subs = {}
    for sub in all_sub:
        subs.setdefault(sub['category_id'], []).append(dict(sub))
        
    db.close()
    return render_template('categories.html', user=user, status=st, days_left=dl, categories=cats, subcategories=subs)

@app.route('/api/categories/add', methods=['POST'])
@active_required
def api_categories_add():
    db = get_db()
    uid = session['user_id']
    name = (request.form.get('name') or '').strip()
    if not name:
        db.close()
        return jsonify({'error': 'Name is required'})
    # Check exists
    ex = db.execute('SELECT id FROM inventory_categories WHERE user_id=%s AND LOWER(name)=LOWER(%s)', (uid, name)).fetchone()
    if ex:
        db.close()
        return jsonify({'error': 'Category already exists'})
    cid = db_insert_id(db, 'INSERT INTO inventory_categories (user_id, name) VALUES (%s, %s)', (uid, name))
    db.commit()
    db.close()
    return jsonify({'success': True, 'id': cid, 'name': name})

@app.route('/api/categories/delete', methods=['POST'])
@active_required
def api_categories_delete():
    db = get_db()
    uid = session['user_id']
    cid = request.form.get('id')
    db.execute('DELETE FROM inventory_subcategories WHERE user_id=%s AND category_id=%s', (uid, cid))
    db.execute('DELETE FROM inventory_categories WHERE user_id=%s AND id=%s', (uid, cid))
    db.commit()
    db.close()
    return jsonify({'success': True})

@app.route('/api/subcategories/add', methods=['POST'])
@active_required
def api_subcategories_add():
    db = get_db()
    uid = session['user_id']
    cid = request.form.get('category_id')
    cname = request.form.get('category_name')
    name = (request.form.get('name') or '').strip()
    
    if not name:
        db.close()
        return jsonify({'error': 'Name is required'})
    
    if cname and not cid:
        # Create category if it doesn't exist
        ex_cat = db.execute('SELECT id FROM inventory_categories WHERE user_id=%s AND LOWER(name)=LOWER(%s)', (uid, cname)).fetchone()
        if ex_cat:
            cid = ex_cat['id']
        else:
            cid = db_insert_id(db, 'INSERT INTO inventory_categories (user_id, name) VALUES (%s, %s)', (uid, cname))

    if not cid:
        db.close()
        return jsonify({'error': 'Category ID is required'})

    ex = db.execute('SELECT id FROM inventory_subcategories WHERE user_id=%s AND category_id=%s AND LOWER(name)=LOWER(%s)', (uid, cid, name)).fetchone()
    if ex:
        db.close()
        return jsonify({'error': 'Sub-category already exists'})
    
    sid = db_insert_id(db, 'INSERT INTO inventory_subcategories (user_id, category_id, name) VALUES (%s, %s, %s)', (uid, cid, name))
    db.commit()
    db.close()
    return jsonify({'success': True, 'id': sid, 'category_id': cid, 'name': name})

@app.route('/api/subcategories/delete', methods=['POST'])
@active_required
def api_subcategories_delete():
    db = get_db()
    uid = session['user_id']
    sid = request.form.get('id')
    db.execute('DELETE FROM inventory_subcategories WHERE user_id=%s AND id=%s', (uid, sid))
    db.commit()
    db.close()
    return jsonify({'success': True})

@app.route('/settings/2fa/setup')
@login_required
@active_required
def setup_2fa():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    secret = user['totp_secret'] if user['totp_secret'] else pyotp.random_base32()
    if not user['totp_secret']:
        db.execute("UPDATE users SET totp_secret=%s WHERE id=%s", (secret, session['user_id']))
        db.commit()
    db.close()
    totp_uri = pyotp.totp.TOTP(secret).provisioning_uri(
        name=user['email'], issuer_name='MobileFix Pro'
    )
    return render_template('setup_2fa.html', secret=secret, totp_uri=totp_uri,
                           status=subscription_status(user), days_left=days_left(user))

@app.route('/settings/2fa/verify', methods=['POST'])
@login_required
@active_required
def verify_2fa_setup():
    code = request.form.get('code', '').strip().replace(' ', '')
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    if user['totp_secret'] and pyotp.TOTP(user['totp_secret']).verify(code, valid_window=1):
        db.execute("UPDATE users SET totp_enabled=TRUE WHERE id=%s", (session['user_id'],))
        db.commit(); db.close()
        flash('Two-factor authentication enabled! Your account is now more secure.', 'success')
        return redirect(url_for('settings'))
    db.close()
    flash('Invalid code. Please try again — make sure your phone clock is accurate.', 'error')
    return redirect(url_for('setup_2fa'))

@app.route('/settings/2fa/disable', methods=['POST'])
@login_required
@active_required
def disable_2fa():
    code = request.form.get('code', '').strip().replace(' ', '')
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    if user['totp_secret'] and pyotp.TOTP(user['totp_secret']).verify(code, valid_window=1):
        db.execute("UPDATE users SET totp_enabled=FALSE, totp_secret=NULL WHERE id=%s", (session['user_id'],))
        db.commit(); db.close()
        flash('Two-factor authentication has been disabled.', 'success')
        return redirect(url_for('settings'))
    db.close()
    flash('Invalid code. 2FA was not disabled.', 'error')
    return redirect(url_for('settings'))

def _send_otp_email(to_email, otp):
    api_key = os.environ.get('BREVO_API_KEY', '').strip()
    print(f"[OTP] Sending to {to_email}, key configured: {bool(api_key)}", flush=True)
    html = f"""
    <div style="font-family:sans-serif;max-width:420px;margin:0 auto;padding:32px 24px;background:#f0f4f8;border-radius:16px;">
      <div style="text-align:center;margin-bottom:24px;">
        <div style="display:inline-block;background:linear-gradient(135deg,#00BCD4,#0097A7);border-radius:14px;padding:12px 20px;">
          <span style="color:white;font-size:1.2rem;font-weight:900;">MobileFix Pro</span>
        </div>
      </div>
      <div style="background:white;border-radius:12px;padding:28px 24px;text-align:center;">
        <h2 style="color:#1a2332;margin-bottom:8px;">Password Reset OTP</h2>
        <p style="color:#6b7c93;margin-bottom:24px;">Use the code below to reset your password. It expires in <strong>5 minutes</strong>.</p>
        <div style="background:#e0f7fa;border-radius:12px;padding:20px;margin-bottom:24px;">
          <span style="font-size:2.4rem;font-weight:900;letter-spacing:12px;color:#0097A7;">{otp}</span>
        </div>
        <p style="color:#94a3b8;font-size:0.8rem;">Do not share this code with anyone.<br>If you did not request a password reset, ignore this email.</p>
      </div>
    </div>"""
    try:
        payload = json.dumps({
            'sender': {'name': 'MobileFix Pro', 'email': 'noreply@mobilefix.cloud'},
            'to': [{'email': to_email}],
            'subject': f'MobileFix Pro — Your OTP is {otp}',
            'htmlContent': html
        }).encode('utf-8')
        req = urllib.request.Request(
            'https://api.brevo.com/v3/smtp/email',
            data=payload,
            headers={'api-key': api_key, 'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            print(f"[OTP] Brevo response: {result}", flush=True)
            return True
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        print(f"[OTP] Brevo HTTP {e.code}: {body}", flush=True)
        return False
    except Exception as e:
        print(f"[OTP] Brevo error: {e}", flush=True)
        return False

@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        phone = request.form.get('phone', '').strip()
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE email=%s AND phone=%s AND role='user'", (email, phone)).fetchone()
        db.close()
        if user:
            otp = str(random.randint(100000, 999999))
            session['fp_uid']       = user['id']
            session['fp_otp']       = otp
            session['fp_otp_time']  = time.time()
            session['fp_2fa_done']  = False
            if _send_otp_email(user['email'], otp):
                return redirect(url_for('forgot_password_2fa'))
            flash('Failed to send OTP email. Please try again.', 'error')
            session.pop('fp_uid', None)
            return render_template('forgot_password.html', step=1)
        flash('No account found with that email and phone combination.', 'error')
    return render_template('forgot_password.html', step=1)

@app.route('/forgot-password/otp', methods=['GET', 'POST'])
def forgot_password_2fa():
    if 'fp_uid' not in session or session.get('fp_2fa_done'):
        return redirect(url_for('forgot_password'))
    if request.method == 'POST':
        entered = request.form.get('otp', '').strip()
        expired = time.time() - session.get('fp_otp_time', 0) > 300   # 5 min
        if expired:
            flash('OTP expired. Please try again.', 'error')
            session.pop('fp_uid', None); session.pop('fp_otp', None)
            return redirect(url_for('forgot_password'))
        if entered == session.get('fp_otp'):
            session['fp_2fa_done'] = True
            session.pop('fp_otp', None); session.pop('fp_otp_time', None)
            return redirect(url_for('forgot_password_reset'))
        flash('Invalid OTP. Please try again.', 'error')
    return render_template('forgot_password.html', step='otp')

@app.route('/forgot-password/reset', methods=['GET', 'POST'])
def forgot_password_reset():
    if 'fp_uid' not in session or not session.get('fp_2fa_done'):
        return redirect(url_for('forgot_password'))
    if request.method == 'POST':
        new_pw = request.form.get('new_password', '')
        if len(new_pw) < 6:
            flash('Password must be at least 6 characters.', 'error')
            return render_template('forgot_password.html', step=2)
        db = get_db()
        db.execute("UPDATE users SET password=%s WHERE id=%s", (hash_pw(new_pw), session['fp_uid']))
        db.commit(); db.close()
        session.pop('fp_uid', None)
        session.pop('fp_2fa_done', None)
        flash('Password reset successfully! Please log in with your new password.', 'success')
        return redirect(url_for('login'))
    return render_template('forgot_password.html', step=2)

@app.route('/manual')
@login_required
def user_manual():
    return render_template('user_manual.html')

@app.route('/terms')
def terms_page():
    return render_template('legal.html', page='terms', title='Terms and Conditions')

@app.route('/privacy')
def privacy_page():
    return render_template('legal.html', page='privacy', title='Privacy Policy')

@app.route('/refund-policy')
def refund_policy_page():
    return render_template('legal.html', page='refund', title='Refund and Return Policy')

@app.route('/smart-tools')
@login_required
def smart_tools():
    return render_template('smart_tools/index.html')

@app.route('/smart-tools/e-aadhaar')
@login_required
def e_aadhaar_print():
    return render_template('smart_tools/e_aadhaar.html')

@app.route('/smart-tools/doc-print')
@login_required
def smart_tools_doc_print():
    return render_template('smart_tools/doc_print.html')

@app.route('/smart-tools/id-card')
@login_required
def id_card_generator():
    return render_template('smart_tools/id_card.html')


@app.route('/smart-tools/passport-photo')
@login_required
def passport_photo_maker():
    return render_template('smart_tools/passport_photo.html')

@app.route('/api/remove-background', methods=['POST'])
@login_required
def api_remove_background():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    try:
        from rembg import remove
        from PIL import Image
        import io
        
        input_image = Image.open(file.stream)
        output_image = remove(input_image)
        
        img_io = io.BytesIO()
        output_image.save(img_io, 'PNG')
        img_io.seek(0)
        
        return send_file(img_io, mimetype='image/png', as_attachment=False, download_name='nobg.png')
    except Exception as e:
        print(f"Error removing background: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/smart-tools/pdf-tools')
@login_required
def pdf_tools():
    return render_template('smart_tools/pdf_tools.html')

@app.route('/api/unlock-pdf', methods=['POST'])
@login_required
def api_unlock_pdf():
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file uploaded'}), 400
    
    file = request.files['file']
    password = request.form.get('password', '')
    
    if not file or file.filename == '':
        return jsonify({'success': False, 'message': 'No selected file'}), 400
        
    try:
        import fitz
        import io
        
        pdf_bytes = file.read()
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        
        if doc.is_encrypted:
            if not doc.authenticate(password):
                return jsonify({'success': False, 'message': 'Incorrect password. Please try again.'}), 401
                
        out_pdf = doc.write()
        doc.close()
        
        return send_file(
            io.BytesIO(out_pdf),
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f"{file.filename.replace('.pdf', '')}_unlocked.pdf"
        )
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# =====================================================================
# GST REPORTS & PURCHASES
# =====================================================================

_GST_STATES = {
    'JAMMU AND KASHMIR': '01', 'JAMMU & KASHMIR': '01',
    'HIMACHAL PRADESH': '02', 'PUNJAB': '03', 'CHANDIGARH': '04',
    'UTTARAKHAND': '05', 'HARYANA': '06', 'DELHI': '07', 'RAJASTHAN': '08', 'UTTAR PRADESH': '09',
    'BIHAR': '10', 'SIKKIM': '11', 'ARUNACHAL PRADESH': '12', 'NAGALAND': '13', 'MANIPUR': '14',
    'MIZORAM': '15', 'TRIPURA': '16', 'MEGHALAYA': '17', 'ASSAM': '18', 'WEST BENGAL': '19',
    'JHARKHAND': '20', 'ODISHA': '21', 'ORISSA': '21', 'CHHATTISGARH': '22', 'MADHYA PRADESH': '23',
    'GUJARAT': '24',
    'DADRA AND NAGAR HAVELI AND DAMAN AND DIU': '26',
    'DADRA & NAGAR HAVELI AND DAMAN & DIU': '26',
    'DADRA AND NAGAR HAVELI': '26', 'DAMAN AND DIU': '26',
    'MAHARASHTRA': '27', 'ANDHRA PRADESH': '37', 'ANDHRA PRADESH (NEW)': '37',
    'KARNATAKA': '29', 'GOA': '30', 'LAKSHADWEEP': '31', 'KERALA': '32', 'TAMIL NADU': '33',
    'PUDUCHERRY': '34', 'PONDICHERRY': '34',
    'ANDAMAN AND NICOBAR ISLANDS': '35', 'ANDAMAN AND NICOBAR': '35', 'ANDAMAN & NICOBAR ISLANDS': '35',
    'TELANGANA': '36', 'LADAKH': '38',
    'FOREIGN COUNTRY': '96', 'OTHER COUNTRY': '96',
    'OTHER TERRITORY': '97',
}
for _c, _n in GST_STATE_LIST:
    _GST_STATES[_n] = _c

def _gst_state_code(val):
    s = (val or '').strip().upper()
    m = re.match(r'^(\d{2})', s)
    if m:
        return m.group(1)
    s = re.sub(r'^\d{2}\s*-?\s*', '', s).strip()
    return _GST_STATES.get(s, '')

def _gst_fp(month):
    parts = (month or '').split('-')
    if len(parts) >= 2 and len(parts[0]) == 4:
        return parts[1] + parts[0]
    return (month or '').replace('-', '')

def _gst_idt(val):
    s = str(val or '')[:10]
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        y, m, d = s.split('-')
        return f'{d}-{m}-{y}'
    return s

def _gst_sale_taxable(row):
    return max(0.0,
        float(_rg(row, 'subtotal') or 0)
        - float(_rg(row, 'discount') or 0)
        - float(_rg(row, 'loyalty_rupees') or 0))

def _gst_json_file(payload, filename):
    raw = json.dumps(payload, indent=2, ensure_ascii=False)
    return send_file(
        io.BytesIO(raw.encode('utf-8')),
        mimetype='application/json',
        as_attachment=True,
        download_name=filename)

def _gstr1_payload(user, sales, month):
    shop_gstin = (_rg(user, 'gstin') or '').strip().upper()
    shop_pos = _gst_state_code(_rg(user, 'state'))
    b2b_groups = {}
    b2cs_map = {}
    for s in sales:
        cgst = float(_rg(s, 'cgst') or 0)
        sgst = float(_rg(s, 'sgst') or 0)
        igst = float(_rg(s, 'igst') or 0)
        txval = round(_gst_sale_taxable(s), 2)
        total = float(_rg(s, 'total') or 0)
        rate = float(_rg(s, 'gst_rate') or 0)
        pos = _gst_state_code(_rg(s, 'billing_state')) or shop_pos
        ctin = (_rg(s, 'customer_gstin') or '').strip().upper()
        bill_no = _rg(s, 'bill_no') or 0
        try:
            inum = f"SAL-{int(bill_no):04d}"
        except (TypeError, ValueError):
            inum = f"SAL-{bill_no}"
        item = {
            "num": 1,
            "itm_det": {
                "txval": txval, "rt": rate,
                "iamt": round(igst, 2), "camt": round(cgst, 2),
                "samt": round(sgst, 2), "csamt": 0
            }
        }
        if ctin:
            b2b_groups.setdefault(ctin, []).append({
                "inum": inum,
                "idt": _gst_idt(_rg(s, 'created_at')),
                "val": round(total, 2),
                "pos": pos or shop_pos,
                "rchrg": "N",
                "inv_typ": "R",
                "itms": [item]
            })
        else:
            key = (pos or shop_pos or '97', rate, 'INTER' if igst else 'INTRA')
            rec = b2cs_map.setdefault(key, {
                "sply_ty": key[2], "rt": rate, "typ": "OE", "pos": key[0],
                "txval": 0, "iamt": 0, "camt": 0, "samt": 0, "csamt": 0
            })
            rec["txval"] = round(rec["txval"] + txval, 2)
            rec["iamt"] = round(rec["iamt"] + igst, 2)
            rec["camt"] = round(rec["camt"] + cgst, 2)
            rec["samt"] = round(rec["samt"] + sgst, 2)
    hsn_map = {}
    for s in sales:
        try:
            lines = json.loads(_rg(s, 'items') or '[]')
        except Exception:
            lines = []
        if not isinstance(lines, list) or not lines:
            continue
        rate = float(_rg(s, 'gst_rate') or 0)
        bill_cgst = float(_rg(s, 'cgst') or 0)
        bill_sgst = float(_rg(s, 'sgst') or 0)
        bill_igst = float(_rg(s, 'igst') or 0)
        sub = float(_rg(s, 'subtotal') or 0) or 1.0
        tx_bill = _gst_sale_taxable(s)
        for ln in lines:
            if not isinstance(ln, dict):
                continue
            hsn = _hsn_code(ln.get('hsn') or ln.get('hsn_code'))
            if not hsn:
                continue
            amt = float(ln.get('amount') or 0)
            share = amt / sub
            key = (hsn, rate)
            row = hsn_map.setdefault(key, {
                "hsn_sc": hsn, "desc": "", "uqc": "PCS", "qty": 0, "rt": rate,
                "txval": 0, "iamt": 0, "camt": 0, "samt": 0, "csamt": 0
            })
            row["qty"] = round(row["qty"] + float(ln.get('qty') or 0), 3)
            row["txval"] = round(row["txval"] + tx_bill * share, 2)
            row["iamt"] = round(row["iamt"] + bill_igst * share, 2)
            row["camt"] = round(row["camt"] + bill_cgst * share, 2)
            row["samt"] = round(row["samt"] + bill_sgst * share, 2)
    hsn_data = []
    for i, row in enumerate(hsn_map.values(), 1):
        rec = dict(row)
        rec["num"] = i
        hsn_data.append(rec)
    return {
        "gstin": shop_gstin,
        "fp": _gst_fp(month),
        "gt": 0,
        "cur_gt": 0,
        "version": "GST3.1.6",
        "hash": "hash",
        "b2b": [{"ctin": ctin, "inv": invs} for ctin, invs in b2b_groups.items()],
        "b2cs": list(b2cs_map.values()),
        "hsn": {"data": hsn_data},
    }

def _gstr2_payload(user, purchases, month):
    shop_gstin = (_rg(user, 'gstin') or '').strip().upper()
    shop_pos = _gst_state_code(_rg(user, 'state'))
    b2b_groups = {}
    for p in purchases:
        ctin = (_rg(p, 'vendor_gstin') or '').strip().upper() or 'UNREGISTERED'
        cgst = float(_rg(p, 'cgst') or 0)
        sgst = float(_rg(p, 'sgst') or 0)
        igst = float(_rg(p, 'igst') or 0)
        txval = float(_rg(p, 'subtotal') or 0)
        total = float(_rg(p, 'total') or 0)
        rate = round(((cgst + sgst + igst) / txval) * 100, 2) if txval else 0
        pos = _gst_state_code(_rg(p, 'vendor_state')) or shop_pos
        b2b_groups.setdefault(ctin, []).append({
            "inum": str(_rg(p, 'bill_no') or ''),
            "idt": _gst_idt(_rg(p, 'invoice_date') or _rg(p, 'created_at')),
            "val": round(total, 2),
            "pos": pos,
            "rchrg": "N",
            "inv_typ": "R",
            "itms": [{
                "num": 1,
                "itm_det": {
                    "txval": round(txval, 2), "rt": rate,
                    "iamt": round(igst, 2), "camt": round(cgst, 2),
                    "samt": round(sgst, 2), "csamt": 0
                }
            }]
        })
    return {
        "gstin": shop_gstin,
        "fp": _gst_fp(month),
        "version": "GST3.1.6",
        "hash": "hash",
        "b2b": [{"ctin": ctin, "inv": invs} for ctin, invs in b2b_groups.items()],
    }

@app.route('/purchases')
@active_required
def purchases():
    db = get_db()
    user, st, dl = _shop_ctx(db, session['user_id'])
    db.close()
    return render_template('purchases.html', user=user, shop_settings=st)

@app.route('/purchases/new', methods=['GET', 'POST'])
@active_required
def purchase_new():
    db = get_db()
    uid = session['user_id']
    if request.method == 'POST':
        payload = request.json
        bill_no = payload.get('bill_no', '').strip()
        vendor_name = payload.get('vendor_name', '').strip()
        vendor_gstin = payload.get('vendor_gstin', '').strip().upper()
        vendor_state = payload.get('vendor_state', '').strip().upper()
        auto_vendor_state = _gst_state_from_gstin(vendor_gstin)
        if auto_vendor_state:
            vendor_state = auto_vendor_state
        invoice_date = payload.get('invoice_date', '')
        subtotal = float(payload.get('subtotal', 0))
        cgst = float(payload.get('cgst', 0))
        sgst = float(payload.get('sgst', 0))
        igst = float(payload.get('igst', 0))
        total = float(payload.get('total', 0))
        items = json.dumps(payload.get('items', []))
        
        db.execute('''INSERT INTO purchase_bills 
            (user_id, bill_no, vendor_name, vendor_gstin, vendor_state, invoice_date, subtotal, cgst, sgst, igst, total, items, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', 
            (uid, bill_no, vendor_name, vendor_gstin, vendor_state, invoice_date, subtotal, cgst, sgst, igst, total, items, _now_str()))
        db.commit()
        db.close()
        return jsonify({'ok': True})
        
    user, st, dl = _shop_ctx(db, uid)
    db.close()
    return render_template('purchase_new.html', user=user, shop_settings=st)

@app.route('/api/purchases/list')
@active_required
def api_purchases_list():
    db = get_db()
    uid = session['user_id']
    rows = db.execute("SELECT * FROM purchase_bills WHERE user_id=? ORDER BY id DESC LIMIT 100", (uid,)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

@app.route('/reports/gst')
@active_required
def gst_reports():
    db = get_db()
    user, st, dl = _shop_ctx(db, session['user_id'])
    db.close()
    return render_template('gst_reports.html', user=user, shop_settings=st)

@app.route('/api/reports/gst/data')
@active_required
def api_gst_report_data():
    month = request.args.get('month', '') # format YYYY-MM
    uid = session['user_id']
    db = get_db()
    
    # Sales
    sales = db.execute("SELECT * FROM sales_bills WHERE user_id=? AND created_at LIKE ? AND is_gst=1", (uid, f"{month}%")).fetchall()
    
    # Purchases
    purchases = db.execute("SELECT * FROM purchase_bills WHERE user_id=? AND invoice_date LIKE ?", (uid, f"{month}%")).fetchall()
    
    db.close()
    return jsonify({
        'sales': [dict(s) for s in sales],
        'purchases': [dict(p) for p in purchases]
    })

@app.route('/api/reports/gst/pdf')
@active_required
def api_gst_report_pdf():
    month = request.args.get('month', '')
    uid = session['user_id']
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    sales = db.execute("SELECT * FROM sales_bills WHERE user_id=? AND created_at LIKE ? AND is_gst=1", (uid, f"{month}%")).fetchall()
    purchases = db.execute("SELECT * FROM purchase_bills WHERE user_id=? AND invoice_date LIKE ?", (uid, f"{month}%")).fetchall()
    db.close()
    
    return render_template('gst_report_pdf.html', user=user, month=month, sales=sales, purchases=purchases)

@app.route('/api/reports/gst/purchases/pdf')
@active_required
def api_gst_purchases_pdf():
    month = request.args.get('month', '')
    uid = session['user_id']
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    purchases = db.execute(
        "SELECT * FROM purchase_bills WHERE user_id=%s AND invoice_date LIKE %s ORDER BY invoice_date, id",
        (uid, f"{month}%")).fetchall()
    db.close()
    return render_template('gst_purchases_pdf.html', user=user, month=month, purchases=purchases)

@app.route('/api/reports/gst/json')
@active_required
def api_gst_report_json():
    month = request.args.get('month', '')
    uid = session['user_id']
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    sales = db.execute(
        "SELECT * FROM sales_bills WHERE user_id=%s AND created_at LIKE %s AND is_gst=1",
        (uid, f"{month}%")).fetchall()
    db.close()
    shop_gstin = (_rg(user, 'gstin') or 'NOGSTIN').strip().upper()
    return _gst_json_file(_gstr1_payload(user, sales, month), f"GSTR1_{shop_gstin}_{month}.json")

@app.route('/api/reports/gst/purchases/json')
@active_required
def api_gst_purchases_json():
    month = request.args.get('month', '')
    uid = session['user_id']
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    purchases = db.execute(
        "SELECT * FROM purchase_bills WHERE user_id=%s AND invoice_date LIKE %s ORDER BY invoice_date, id",
        (uid, f"{month}%")).fetchall()
    db.close()
    shop_gstin = (_rg(user, 'gstin') or 'NOGSTIN').strip().upper()
    return _gst_json_file(_gstr2_payload(user, purchases, month), f"GSTR2_{shop_gstin}_{month}.json")


init_db()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
