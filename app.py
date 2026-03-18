from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash
from functools import wraps
from datetime import datetime, timedelta, timezone
import psycopg2, psycopg2.extras, hashlib, os, re, json, random, string, base64, pyotp, time
import urllib.request, urllib.error

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY') or os.urandom(24)
DATABASE_URL = os.environ.get('DATABASE_URL', '')
IST = timezone(timedelta(hours=5, minutes=30))

# ── DB Wrapper ─────────────────────────────────────────────────────────────────
class DbWrapper:
    """Thin psycopg2 wrapper that matches the sqlite3 interface used throughout."""
    def __init__(self, conn):
        self._conn = conn
        self._cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def execute(self, sql, params=()):
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
    conn = psycopg2.connect(DATABASE_URL)
    return DbWrapper(conn)

def _now_str():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def _today_str():
    return datetime.now(IST).strftime('%Y-%m-%d')

# ── Template filter ────────────────────────────────────────────────────────────
@app.template_filter('to_ist')
def to_ist_filter(dt_str):
    if not dt_str: return '—'
    try:
        dt = datetime.fromisoformat(str(dt_str)[:19]) + timedelta(hours=5, minutes=30)
        return dt.strftime('%Y-%m-%d %H:%M')
    except:
        return str(dt_str)[:16]

def generate_happy_code():
    return ''.join(random.choices(string.digits, k=6))

# ── DB Init ────────────────────────────────────────────────────────────────────
def init_db():
    db = get_db()
    try:
        db.execute('''CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
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
        db.execute('''CREATE TABLE IF NOT EXISTS repair_jobs (
            id SERIAL PRIMARY KEY,
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
        db.execute('''CREATE TABLE IF NOT EXISTS invoices (
            id SERIAL PRIMARY KEY,
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
        db.execute('''CREATE TABLE IF NOT EXISTS subscription_history (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            plan TEXT,
            start_date TEXT,
            end_date TEXT,
            activated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        db.commit()

        # Seed admin
        admin_email = os.environ.get('ADMIN_EMAIL', 'admin@mobilefix.com')
        admin_phone = os.environ.get('ADMIN_PHONE', '0000000000')
        admin_pw = hashlib.sha256(os.environ.get('ADMIN_PASSWORD', 'admin123').encode()).hexdigest()
        db.execute('''INSERT INTO users (phone,email,password,shop_name,role,enabled,trial_start)
                      VALUES (%s,%s,%s,'MobileFix Admin','admin',1,%s)
                      ON CONFLICT (email) DO NOTHING''',
                   (admin_phone, admin_email, admin_pw, _now_str()))
        db.commit()

        # Safe column migrations (IF NOT EXISTS — PostgreSQL 9.6+)
        for col in [
            "imei TEXT", "imei_billing TEXT", "aadhar_number TEXT", "received_without TEXT",
            "expected_return TEXT", "delivery_date TEXT", "cancel_reason TEXT", "quote_items TEXT",
            "advance_amount REAL DEFAULT 0", "advance_method TEXT", "paid_status TEXT DEFAULT 'Unpaid'",
            "happy_code TEXT", "reminder_date TEXT", "rework_details TEXT", "advance_history TEXT",
            "diagnosed_at TEXT", "refund_amount REAL DEFAULT 0", "refund_method TEXT", "refund_date TEXT"
        ]:
            try:
                db.execute(f"ALTER TABLE repair_jobs ADD COLUMN IF NOT EXISTS {col}")
                db.commit()
            except Exception:
                db.rollback()

        for col in ["advance_amount REAL DEFAULT 0", "discount REAL DEFAULT 0",
                    "pay_method TEXT", "paid TEXT DEFAULT 'Unpaid'", "due_date TEXT"]:
            try:
                db.execute(f"ALTER TABLE invoices ADD COLUMN IF NOT EXISTS {col}")
                db.commit()
            except Exception:
                db.rollback()

        for col in ["logo TEXT", "google_review_link TEXT", "phone TEXT",
                    "door_no TEXT", "street TEXT", "city TEXT", "pincode TEXT",
                    "totp_secret TEXT", "totp_enabled BOOLEAN DEFAULT FALSE"]:
            try:
                db.execute(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col}")
                db.commit()
            except Exception:
                db.rollback()

        db.execute('''CREATE TABLE IF NOT EXISTS login_logs (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
            identifier TEXT,
            ip_address TEXT,
            user_agent TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
        )''')
        db.commit()

        # Fix NULL trial_start
        db.execute("UPDATE users SET trial_start=%s WHERE trial_start IS NULL", (_now_str(),))
        db.commit()

        # Uppercase existing addresses
        db.execute("""UPDATE users SET
            shop_name = UPPER(shop_name),
            door_no   = UPPER(COALESCE(door_no,'')),
            street    = UPPER(COALESCE(street,'')),
            city      = UPPER(COALESCE(city,''))
            WHERE role != 'admin'""")
        db.commit()
    finally:
        db.close()

# ── Helpers ────────────────────────────────────────────────────────────────────
def hash_pw(p): return hashlib.sha256(p.encode()).hexdigest()

def _parse_dt(s):
    if not s: return None
    return datetime.fromisoformat(str(s)[:19]).replace(tzinfo=timezone.utc)

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

def _log_login(db, user_id, identifier, status):
    ip = (request.headers.get('X-Forwarded-For') or request.remote_addr or 'unknown').split(',')[0].strip()
    ua = (request.headers.get('User-Agent') or '')[:300]
    now_ist = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    db.execute(
        "INSERT INTO login_logs (user_id, identifier, ip_address, user_agent, status, created_at) VALUES (%s,%s,%s,%s,%s,%s)",
        (user_id, identifier, ip, ua, status, now_ist)
    )
    db.commit()

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        identifier = request.form.get('identifier', '').strip()
        password = request.form.get('password', '')
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE phone=%s OR email=%s", (identifier, identifier)).fetchone()
        if user and user['password'] == hash_pw(password):
            if not user['enabled'] and user['role'] != 'admin':
                _log_login(db, user['id'], identifier, 'blocked')
                db.close()
                flash('Your account has been disabled. Contact support.', 'error')
                return render_template('login.html')
            _log_login(db, user['id'], identifier, 'success')
            session['user_id'] = user['id']
            session['role'] = user['role']
            session['shop_name'] = user['shop_name'] or 'My Shop'
            db.close()
            return redirect(url_for('admin_dashboard') if user['role'] == 'admin' else url_for('dashboard'))
        uid = user['id'] if user else None
        _log_login(db, uid, identifier, 'failed')
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
        new_user = db.execute("SELECT id FROM users WHERE phone=%s", (phone,)).fetchone()
        db.close()
        session['user_id'] = new_user['id']
        session['role'] = 'user'
        session['shop_name'] = shop_name
        flash('Welcome! Your 30-day free trial has started. Please set up Two-Factor Authentication to secure your account.', 'success')
        return redirect(url_for('setup_2fa'))
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear(); return redirect(url_for('login'))

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
    job_count     = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=%s", (uid,)).fetchone()[0]
    invoice_count = db.execute("SELECT COUNT(*) FROM invoices WHERE user_id=%s", (uid,)).fetchone()[0]
    pending   = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=%s AND status NOT IN ('Delivered','Cancelled')", (uid,)).fetchone()[0]
    delivered = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=%s AND status='Delivered'", (uid,)).fetchone()[0]
    overdue   = db.execute(
        "SELECT COUNT(*) FROM repair_jobs WHERE user_id=%s AND status NOT IN ('Delivered','Cancelled')"
        " AND expected_return IS NOT NULL AND expected_return!='' AND expected_return < %s",
        (uid, _today_str())).fetchone()[0]
    partial   = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=%s AND paid_status='Partial' AND status='Delivered'", (uid,)).fetchone()[0]
    recent_jobs = db.execute("SELECT * FROM repair_jobs WHERE user_id=%s ORDER BY created_at DESC LIMIT 6", (uid,)).fetchall()
    db.close()
    return render_template('dashboard.html', user=user, status=subscription_status(user),
                           days_left=days_left(user), job_count=job_count, invoice_count=invoice_count,
                           pending=pending, delivered=delivered, recent_jobs=recent_jobs,
                           overdue_count=overdue, partial_count=partial)

@app.route('/jobs')
@active_required
def jobs():
    db = get_db()
    uid = session['user_id']
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    all_jobs = db.execute("SELECT * FROM repair_jobs WHERE user_id=%s ORDER BY created_at DESC", (uid,)).fetchall()
    db.close()
    jobs_list = [dict(j) for j in all_jobs]
    today = _today_str()
    overdue_jobs = [j for j in jobs_list if j.get('expected_return') and j['expected_return'] < today
                    and j['status'] not in ('Delivered', 'Cancelled')]
    partial_jobs = [j for j in jobs_list if j.get('paid_status') == 'Partial' and j['status'] == 'Delivered']
    return render_template('jobs.html', jobs=all_jobs, jobs_json=json.dumps(jobs_list),
                           user=user, status=subscription_status(user), days_left=days_left(user),
                           overdue_jobs=overdue_jobs, partial_jobs=partial_jobs)

@app.route('/jobs/add', methods=['GET', 'POST'])
@active_required
def add_job():
    if request.method == 'POST':
        db = get_db()
        adv  = float(request.form.get('advance_amount') or 0)
        cost = float(request.form.get('cost') or 0)
        paid_status = 'Paid' if adv > 0 and cost > 0 and adv >= cost else ('Partial' if adv > 0 else 'Unpaid')
        happy_code = generate_happy_code()
        db.execute('''INSERT INTO repair_jobs
                      (user_id,customer_name,customer_phone,device_model,imei,imei_billing,
                       issue,aadhar_number,received_without,cost,notes,advance_amount,advance_method,
                       paid_status,expected_return,happy_code,created_at,updated_at)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                   (session['user_id'],
                    request.form.get('customer_name', '').upper(),
                    request.form.get('customer_phone', ''),
                    (request.form.get('device_model', '') or
                     (request.form.get('device_brand', '') + ' ' + request.form.get('device_model_only', '')).strip()).upper(),
                    request.form.get('imei', '').upper(),
                    request.form.get('imei_billing', '').upper(),
                    request.form.get('issue', '').upper(),
                    request.form.get('aadhar_number', '').replace(' ', ''),
                    request.form.get('received_without_val', ''),
                    cost, request.form.get('notes', '').upper(),
                    adv, request.form.get('advance_method', ''), paid_status,
                    request.form.get('expected_return', ''),
                    happy_code, _now_str(), _now_str()))
        db.commit(); db.close()
        flash('Repair job added successfully!', 'success')
        return redirect(url_for('jobs'))
    return render_template('add_job.html')

@app.route('/jobs/<int:job_id>/update', methods=['POST'])
@active_required
def update_job(job_id):
    db = get_db()
    adv_str = request.form.get('advance_amount')
    new_adv = float(adv_str or 0) if adv_str is not None else 0
    existing = db.execute("SELECT advance_amount, advance_history FROM repair_jobs WHERE id=%s AND user_id=%s",
                          (job_id, session['user_id'])).fetchone()
    has_cost = 'cost' in request.form
    _ts = "to_char(NOW() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')"
    set_parts = ['status=%s', f'diagnosed_at=COALESCE(diagnosed_at,{_ts})', f'updated_at={_ts}']
    params = [request.form.get('status')]
    if has_cost:
        new_cost = float(request.form.get('cost') or 0)
        set_parts.insert(1, 'cost=%s')
        set_parts.insert(2, 'notes=%s')
        set_parts.insert(3, 'quote_items=%s')
        params += [new_cost, request.form.get('notes', ''), request.form.get('quote_items', '')]
        if adv_str is None or new_adv == 0:
            row = db.execute("SELECT advance_amount FROM repair_jobs WHERE id=%s AND user_id=%s", (job_id, session['user_id'])).fetchone()
            cur_adv = float(row['advance_amount'] or 0) if row else 0
            recalc_paid = 'Paid' if new_cost > 0 and cur_adv >= new_cost else ('Partial' if cur_adv > 0 else 'Unpaid')
            set_parts.insert(4, 'paid_status=%s')
            params += [recalc_paid]
    if adv_str is not None and new_adv > 0:
        adv_method = request.form.get('advance_method', '')
        old_total = float(existing['advance_amount'] or 0) if existing else 0
        history = json.loads(existing['advance_history'] or '[]') if existing else []
        history.append({'amount': new_adv, 'method': adv_method,
                        'date': datetime.now(IST).strftime('%Y-%m-%d %H:%M')})
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
    params += [job_id, session['user_id']]
    db.execute(f"UPDATE repair_jobs SET {', '.join(set_parts)} WHERE id=%s AND user_id=%s", params)
    db.commit(); db.close()
    return jsonify({'ok': True})

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
    data = request.get_json(force=True)
    total          = float(data.get('total', 0))
    advance        = float(data.get('advance', 0))
    amount_paid_now= float(data.get('amountPaidNow', 0))
    discount       = float(data.get('discount', 0))
    pay_method     = data.get('payMethod', 'Cash')
    credit_due_date= data.get('creditDueDate') or None
    items_str      = json.dumps(data.get('items', []))
    total_collected= advance + amount_paid_now
    balance        = max(0, total - total_collected)
    paid = 'Paid' if balance < 0.01 else ('Partial' if total_collected > 0 else 'Unpaid')
    delivery_date  = datetime.now(IST).strftime('%Y-%m-%d')
    db = get_db()
    imei = data.get('imei', '').strip().upper()
    if imei:
        db.execute("UPDATE repair_jobs SET imei_billing=%s WHERE id=%s AND user_id=%s AND (imei_billing IS NULL OR imei_billing='')",
                   (imei, job_id, session['user_id']))
    db.execute("UPDATE repair_jobs SET status='Delivered',cost=%s,delivery_date=%s,paid_status=%s,updated_at=%s WHERE id=%s AND user_id=%s",
               (total, delivery_date, paid, _now_str(), job_id, session['user_id']))
    db.execute('''INSERT INTO invoices (user_id,job_id,customer_name,customer_phone,items,total,advance_amount,discount,pay_method,paid,due_date,created_at)
                  SELECT %s,id,customer_name,customer_phone,%s,%s,%s,%s,%s,%s,%s,%s FROM repair_jobs WHERE id=%s''',
               (session['user_id'], items_str, total, total_collected, discount, pay_method, paid, credit_due_date, _now_str(), job_id))
    db.commit()
    inv = db.execute("SELECT id FROM invoices WHERE job_id=%s ORDER BY id DESC LIMIT 1", (job_id,)).fetchone()
    db.close()
    return jsonify({'ok': True, 'inv_id': inv['id'] if inv else None})

@app.route('/jobs/<int:job_id>/cancel', methods=['POST'])
@active_required
def cancel_job_route(job_id):
    reason = request.form.get('reason', 'Cancelled')
    db = get_db()
    db.execute("UPDATE repair_jobs SET status='Cancelled',cancel_reason=%s,updated_at=%s WHERE id=%s AND user_id=%s",
               (reason, _now_str(), job_id, session['user_id']))
    db.commit(); db.close()
    return jsonify({'ok': True})

@app.route('/jobs/<int:job_id>/record_refund', methods=['POST'])
@active_required
def record_refund(job_id):
    amount = float(request.form.get('amount', 0))
    method = request.form.get('method', 'Cash')
    date   = request.form.get('date', datetime.now(IST).strftime('%Y-%m-%d'))
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
    db.execute('''INSERT INTO repair_jobs
                  (user_id,customer_name,customer_phone,device_model,imei,imei_billing,
                   issue,aadhar_number,received_without,cost,notes,status,happy_code,created_at,updated_at)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Rework',%s,%s,%s)''',
               (session['user_id'], orig['customer_name'], orig['customer_phone'],
                orig['device_model'], orig['imei'], orig['imei_billing'],
                f"REWORK: {details}", orig['aadhar_number'], orig['received_without'],
                0, f"Original Job: #{job_id} | {details}", happy_code, _now_str(), _now_str()))
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

@app.route('/invoices/create', methods=['GET', 'POST'])
@active_required
def create_invoice():
    db = get_db()
    user      = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    jobs_list = db.execute("SELECT * FROM repair_jobs WHERE user_id=%s ORDER BY created_at DESC", (session['user_id'],)).fetchall()
    if request.method == 'POST':
        db.execute('INSERT INTO invoices (user_id,job_id,customer_name,customer_phone,items,total,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)',
                   (session['user_id'], request.form.get('job_id') or None,
                    request.form.get('customer_name'), request.form.get('customer_phone'),
                    request.form.get('items'), float(request.form.get('total') or 0), _now_str()))
        db.commit(); db.close()
        flash('Invoice created!', 'success')
        return redirect(url_for('invoices'))
    db.close()
    return render_template('create_invoice.html', jobs=jobs_list, user=user)

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
    db.close()
    return render_template('print_invoice.html', inv=inv, user=user, job=job)

@app.route('/invoices/<int:inv_id>/mark_paid', methods=['POST'])
@active_required
def mark_invoice_paid(inv_id):
    amount_received = float(request.form.get('amount_received', 0))
    pay_method = request.form.get('pay_method', 'Cash')
    due_date   = request.form.get('due_date', '')
    db = get_db()
    inv = db.execute("SELECT * FROM invoices WHERE id=%s AND user_id=%s", (inv_id, session['user_id'])).fetchone()
    if not inv: db.close(); return jsonify({'error': 'Not found'}), 404
    new_adv = float(inv['advance_amount'] or 0) + amount_received
    total   = float(inv['total'] or 0)
    balance = max(0, total - new_adv)
    paid    = 'Paid' if balance < 0.01 else ('Partial' if new_adv > 0 else 'Unpaid')
    db.execute("UPDATE invoices SET advance_amount=%s,pay_method=%s,paid=%s,due_date=%s WHERE id=%s",
               (new_adv, pay_method, paid, due_date or None, inv_id))
    if inv['job_id']:
        db.execute("UPDATE repair_jobs SET paid_status=%s WHERE id=%s AND user_id=%s", (paid, inv['job_id'], session['user_id']))
    db.commit(); db.close()
    return jsonify({'ok': True, 'paid': paid, 'advance': new_adv, 'balance': balance})

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
    return render_template('customers.html',
                           customers=sorted(customer_map.values(), key=lambda x: x['total_business'], reverse=True),
                           user=user, status=subscription_status(user), days_left=days_left(user))

@app.route('/reports')
@active_required
def reports():
    db = get_db()
    uid  = session['user_id']
    user = db.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
    from datetime import date
    today       = date.today()
    week_start  = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    today_str   = today.isoformat()

    def count(where, params=()):
        return db.execute(f"SELECT COUNT(*) FROM repair_jobs WHERE user_id=%s {where}", (uid,) + params).fetchone()[0]
    def revenue(where, params=()):
        r = db.execute(f"SELECT SUM(cost) FROM repair_jobs WHERE user_id=%s {where}", (uid,) + params).fetchone()[0]
        return float(r or 0)

    stats = {
        'total':           count(''),
        'pending':         count("AND status NOT IN ('Delivered','Cancelled')"),
        'delivered':       count("AND status='Delivered'"),
        'cancelled':       count("AND status='Cancelled'"),
        'rework':          count("AND status='Rework'"),
        'today_new':       count("AND SUBSTRING(created_at FROM 1 FOR 10)=%s", (today_str,)),
        'week_new':        count("AND SUBSTRING(created_at FROM 1 FOR 10)>=%s", (str(week_start),)),
        'month_new':       count("AND SUBSTRING(created_at FROM 1 FOR 10)>=%s", (str(month_start),)),
        'month_revenue':   revenue("AND status='Delivered' AND delivery_date>=%s", (str(month_start),)),
        'total_revenue':   revenue("AND status='Delivered'"),
        'partial_pending': count("AND paid_status='Partial'"),
        'overdue':         count("AND status NOT IN ('Delivered','Cancelled') AND expected_return IS NOT NULL AND expected_return!='' AND expected_return<%s", (today_str,)),
    }
    recent_delivered = db.execute(
        "SELECT * FROM repair_jobs WHERE user_id=%s AND status='Delivered' ORDER BY delivery_date DESC LIMIT 10", (uid,)).fetchall()
    db.close()
    return render_template('reports.html', user=user, stats=stats, recent_delivered=recent_delivered,
                           status=subscription_status(user), days_left=days_left(user))

@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],)).fetchone()
    if request.method == 'POST':
        shop_name          = request.form.get('shop_name', '').strip().upper()
        door_no            = request.form.get('door_no', '').strip().upper()
        street             = request.form.get('street', '').strip().upper()
        city               = request.form.get('city', '').strip().upper()
        pincode            = request.form.get('pincode', '').strip().upper()
        addr_parts = [p for p in [door_no, street] if p]
        addr_line1 = ', '.join(addr_parts)
        addr_line2 = city + (' - ' + pincode if pincode else '')
        address = '\n'.join([l for l in [addr_line1, addr_line2] if l])
        google_review_link = request.form.get('google_review_link', '').strip()
        new_pw             = request.form.get('new_password', '')
        current_pw         = request.form.get('current_password', '')
        logo_data = None
        if 'logo' in request.files:
            f = request.files['logo']
            if f and f.filename:
                mime = f.content_type or 'image/png'
                logo_data = 'data:' + mime + ';base64,' + base64.b64encode(f.read()).decode()
        db.execute("UPDATE users SET shop_name=%s,address=%s,door_no=%s,street=%s,city=%s,pincode=%s,google_review_link=%s WHERE id=%s",
                   (shop_name, address, door_no, street, city, pincode, google_review_link, session['user_id']))
        if new_pw:
            if not current_pw or user['password'] != hash_pw(current_pw):
                db.close()
                flash('Current password is incorrect.', 'error')
                return redirect(url_for('settings'))
            if len(new_pw) < 6:
                db.close()
                flash('New password must be at least 6 characters.', 'error')
                return redirect(url_for('settings'))
            db.execute("UPDATE users SET password=%s WHERE id=%s", (hash_pw(new_pw), session['user_id']))
        if logo_data:
            db.execute("UPDATE users SET logo=%s WHERE id=%s", (logo_data, session['user_id']))
        db.commit(); db.close()
        session['shop_name'] = shop_name
        flash('Settings saved!', 'success')
        return redirect(url_for('settings'))
    db.close()
    return render_template('settings.html', user=user, status=subscription_status(user), days_left=days_left(user))

@app.route('/admin')
@admin_required
def admin_dashboard():
    db = get_db()
    users    = db.execute("SELECT * FROM users WHERE role='user' ORDER BY created_at DESC").fetchall()
    enriched = [{**dict(u), 'status': subscription_status(u), 'days_left': days_left(u)} for u in users]
    total    = len(users)

    platform = {}
    platform['total_jobs']     = db.execute("SELECT COUNT(*) FROM repair_jobs").fetchone()[0]
    platform['total_revenue']  = float(db.execute("SELECT COALESCE(SUM(total),0) FROM invoices").fetchone()[0])
    platform['total_invoices'] = db.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
    platform['jobs_today']     = db.execute(
        "SELECT COUNT(*) FROM repair_jobs WHERE SUBSTRING(created_at FROM 1 FOR 10)=%s", (_today_str(),)).fetchone()[0]

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
        "SELECT COUNT(*) FROM users WHERE role='user' AND SUBSTRING(created_at FROM 1 FOR 10)>=%s", (month_start,)).fetchone()[0]

    # Per-shop performance
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

        trial_tracker = db.execute("""
            SELECT u.id, u.shop_name, u.phone, u.subscription_plan,
                   SUBSTRING(u.trial_start FROM 1 FOR 10) AS trial_start,
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
        daily_jobs = db.execute("""
            SELECT SUBSTRING(created_at FROM 1 FOR 10) AS day, COUNT(*) AS cnt
            FROM repair_jobs
            WHERE SUBSTRING(created_at FROM 1 FOR 10) >= %s
            GROUP BY day ORDER BY day
        """, (cutoff,)).fetchall()
        daily_jobs = [dict(d) for d in daily_jobs]
    except Exception:
        db.rollback()
        shop_perf = []; trial_tracker = []; brand_stats = []; daily_jobs = []

    db.close()

    return render_template('admin_dashboard.html', users=enriched, total=total,
                           active_count=sum(1 for u in users if subscription_status(u) in ('trial', 'active')),
                           disabled_count=sum(1 for u in users if not u['enabled']),
                           expired_count=sum(1 for u in users if subscription_status(u) in ('trial_expired', 'expired')),
                           platform=platform, shop_activity=shop_activity,
                           expiring=expiring, new_this_month=new_this_month,
                           shop_perf=shop_perf, trial_tracker=trial_tracker,
                           brand_stats=brand_stats, daily_jobs=daily_jobs)

@app.route('/admin/toggle/<int:uid>', methods=['POST'])
@admin_required
def admin_toggle(uid):
    db = get_db()
    user = db.execute("SELECT enabled FROM users WHERE id=%s", (uid,)).fetchone()
    new_state = 0 if user['enabled'] else 1
    db.execute("UPDATE users SET enabled=%s WHERE id=%s", (new_state, uid))
    db.commit(); db.close()
    return jsonify({'enabled': new_state})

@app.route('/admin/set_subscription/<int:uid>', methods=['POST'])
@admin_required
def admin_set_subscription(uid):
    plan = request.form.get('plan')
    days = {'30d': 30, '1y': 365, '2y': 730, '3y': 1095}.get(plan)
    if not days: return jsonify({'error': 'Invalid plan'}), 400
    start_date = datetime.now(IST).strftime('%Y-%m-%d')
    end_date   = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    db = get_db()
    db.execute("UPDATE users SET subscription_plan=%s,subscription_end=%s,enabled=1 WHERE id=%s", (plan, end_date, uid))
    db.execute("INSERT INTO subscription_history (user_id,plan,start_date,end_date,activated_at) VALUES (%s,%s,%s,%s,%s)",
               (uid, plan, start_date, end_date[:10], _now_str()))
    db.commit(); db.close()
    return jsonify({'success': True, 'end_date': end_date[:10]})

@app.route('/admin/subscription_history/<int:uid>')
@admin_required
def admin_sub_history(uid):
    db = get_db()
    rows = db.execute("SELECT * FROM subscription_history WHERE user_id=%s ORDER BY activated_at DESC", (uid,)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

@app.route('/admin/delete/<int:uid>', methods=['POST'])
@admin_required
def admin_delete_user(uid):
    db = get_db()
    # Order matters: delete referencing tables before referenced ones (PostgreSQL FK enforcement)
    db.execute("DELETE FROM invoices WHERE user_id=%s", (uid,))
    db.execute("DELETE FROM subscription_history WHERE user_id=%s", (uid,))
    db.execute("DELETE FROM repair_jobs WHERE user_id=%s", (uid,))
    db.execute("DELETE FROM users WHERE id=%s", (uid,))
    db.commit(); db.close()
    return jsonify({'success': True})

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
            ORDER BY last_login DESC NULLS LAST
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
            ORDER BY last_seen ASC NULLS FIRST
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
        hourly_raw = db.execute("""
            SELECT SUBSTRING(created_at FROM 12 FOR 2) AS hr, COUNT(*) AS cnt
            FROM login_logs
            WHERE status='success'
            GROUP BY hr ORDER BY hr
        """).fetchall()
        hourly_data = {r['hr']: r['cnt'] for r in hourly_raw}
        hourly = [{'hr': str(h).zfill(2), 'cnt': hourly_data.get(str(h).zfill(2), 0)} for h in range(24)]

        # Day of week heatmap
        dow_names = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat']
        dow_raw = db.execute("""
            SELECT EXTRACT(DOW FROM created_at::timestamp)::int AS dow, COUNT(*) AS cnt
            FROM login_logs WHERE status='success'
            GROUP BY dow ORDER BY dow
        """).fetchall()
        dow_data = {r['dow']: r['cnt'] for r in dow_raw}
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


@app.route('/admin/login-history/<int:uid>')
@admin_required
def admin_login_history(uid):
    db = get_db()
    try:
        logs = db.execute("""
            SELECT status, ip_address, user_agent, created_at
            FROM login_logs WHERE user_id=%s
            ORDER BY created_at DESC LIMIT 50
        """, (uid,)).fetchall()
        return jsonify([dict(l) for l in logs])
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
        db.close()
        if user and user['totp_secret'] and pyotp.TOTP(user['totp_secret']).verify(code, valid_window=1):
            session.pop('pending_2fa_uid', None)
            session['user_id'] = user['id']
            session['role'] = user['role']
            session['shop_name'] = user['shop_name'] or 'My Shop'
            return redirect(url_for('admin_dashboard') if user['role'] == 'admin' else url_for('dashboard'))
        flash('Invalid or expired code. Please try again.', 'error')
    return render_template('login_2fa.html')

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
    api_key = os.environ.get('RESEND_API_KEY', '').strip()
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
            'from': 'MobileFix Pro <noreply@send.mobilefix.cloud>',
            'to': [to_email],
            'subject': f'MobileFix Pro — Your OTP is {otp}',
            'html': html
        }).encode('utf-8')
        req = urllib.request.Request(
            'https://api.resend.com/emails',
            data=payload,
            headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            print(f"[OTP] Resend response: {result}", flush=True)
            return True
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        print(f"[OTP] Resend HTTP {e.code}: {body}", flush=True)
        return False
    except Exception as e:
        print(f"[OTP] Resend error: {e}", flush=True)
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

init_db()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
