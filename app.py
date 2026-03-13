from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash
from functools import wraps
from datetime import datetime, timedelta, timezone
import sqlite3, hashlib, os, re, json, random, string, base64

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY') or os.urandom(24)
DATABASE = os.environ.get('DATABASE_PATH', 'instance/mobilefix.db')
IST = timezone(timedelta(hours=5, minutes=30))

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

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

def init_db():
    with get_db() as db:
        db.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            shop_name TEXT,
            address TEXT,
            role TEXT DEFAULT "user",
            enabled INTEGER DEFAULT 1,
            trial_start TEXT,
            subscription_plan TEXT,
            subscription_end TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS repair_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            customer_name TEXT,
            customer_phone TEXT,
            device_model TEXT,
            imei TEXT,
            imei_billing TEXT,
            issue TEXT,
            aadhar_number TEXT,
            received_without TEXT,
            status TEXT DEFAULT "Received",
            cost REAL DEFAULT 0,
            notes TEXT,
            expected_return TEXT,
            delivery_date TEXT,
            cancel_reason TEXT,
            quote_items TEXT,
            advance_amount REAL DEFAULT 0,
            advance_method TEXT,
            paid_status TEXT DEFAULT "Unpaid",
            happy_code TEXT,
            reminder_date TEXT,
            rework_details TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            job_id INTEGER,
            customer_name TEXT,
            customer_phone TEXT,
            items TEXT,
            total REAL,
            advance_amount REAL DEFAULT 0,
            discount REAL DEFAULT 0,
            pay_method TEXT,
            paid TEXT DEFAULT "Unpaid",
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(job_id) REFERENCES repair_jobs(id)
        );
        ''')
        admin_email = os.environ.get('ADMIN_EMAIL', 'admin@mobilefix.com')
        admin_phone = os.environ.get('ADMIN_PHONE', '0000000000')
        admin_pw = hashlib.sha256(os.environ.get('ADMIN_PASSWORD', 'admin123').encode()).hexdigest()
        try:
            db.execute('''INSERT INTO users (phone,email,password,shop_name,role,enabled,trial_start)
                          VALUES (?,?,?,"MobileFix Admin","admin",1,datetime("now"))''', (admin_phone, admin_email, admin_pw))
            db.commit()
        except: pass
        # Safe migrations
        for col in ['imei TEXT','imei_billing TEXT','aadhar_number TEXT','received_without TEXT',
                    'expected_return TEXT','delivery_date TEXT','cancel_reason TEXT','quote_items TEXT',
                    'advance_amount REAL DEFAULT 0','advance_method TEXT','paid_status TEXT DEFAULT "Unpaid"',
                    'happy_code TEXT','reminder_date TEXT','rework_details TEXT','advance_history TEXT','diagnosed_at TEXT',
                    'refund_amount REAL DEFAULT 0','refund_method TEXT','refund_date TEXT']:
            try: db.execute(f"ALTER TABLE repair_jobs ADD COLUMN {col}"); db.commit()
            except: pass
        for col in ['advance_amount REAL DEFAULT 0','discount REAL DEFAULT 0','pay_method TEXT','paid TEXT DEFAULT "Unpaid"','due_date TEXT']:
            try: db.execute(f"ALTER TABLE invoices ADD COLUMN {col}"); db.commit()
            except: pass
        for col in ['logo TEXT']:
            try: db.execute(f"ALTER TABLE users ADD COLUMN {col}"); db.commit()
            except: pass
        # Fix NULL trial_start for existing users
        try: db.execute("UPDATE users SET trial_start=datetime('now') WHERE trial_start IS NULL"); db.commit()
        except: pass

def hash_pw(p): return hashlib.sha256(p.encode()).hexdigest()

def _parse_dt(s):
    """Parse datetime string as UTC-aware datetime regardless of stored format."""
    if not s: return None
    return datetime.fromisoformat(str(s)[:19]).replace(tzinfo=timezone.utc)

def subscription_status(user):
    if not user['enabled']: return 'inactive'
    now = datetime.now(timezone.utc)
    trial_start = _parse_dt(user['trial_start'])
    sub_end = _parse_dt(user['subscription_end'])
    if sub_end and now < sub_end: return 'active'
    if trial_start and now < trial_start + timedelta(days=30): return 'trial'
    if sub_end and now >= sub_end: return 'expired'
    if trial_start and now >= trial_start + timedelta(days=30): return 'trial_expired'
    return 'trial'

def days_left(user):
    now = datetime.now(timezone.utc)
    sub_end = _parse_dt(user['subscription_end'])
    trial_start = _parse_dt(user['trial_start'])
    if sub_end:
        return max(0, (sub_end - now).days)
    if trial_start:
        return max(0, (trial_start + timedelta(days=30) - now).days)
    return 0

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session: return redirect(url_for('login'))
        db = get_db()
        user = db.execute("SELECT id FROM users WHERE id=?", (session['user_id'],)).fetchone()
        if not user:
            session.clear()
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('role') != 'admin': return redirect(url_for('login'))
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
        user = db.execute("SELECT * FROM users WHERE id=?", (session['user_id'],)).fetchone()
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

@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('admin_dashboard') if session.get('role')=='admin' else url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        identifier = request.form.get('identifier','').strip()
        password = request.form.get('password','')
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE phone=? OR email=?", (identifier,identifier)).fetchone()
        if user and user['password'] == hash_pw(password):
            if not user['enabled'] and user['role'] != 'admin':
                flash('Your account has been disabled. Contact support.', 'error')
                return render_template('login.html')
            session['user_id'] = user['id']
            session['role'] = user['role']
            session['shop_name'] = user['shop_name'] or 'My Shop'
            return redirect(url_for('admin_dashboard') if user['role']=='admin' else url_for('dashboard'))
        flash('Invalid phone/email or password.', 'error')
    return render_template('login.html')

@app.route('/register', methods=['GET','POST'])
def register():
    if request.method == 'POST':
        phone = request.form.get('phone','').strip()
        email = request.form.get('email','').strip().lower()
        password = request.form.get('password','')
        shop_name = request.form.get('shop_name','').strip()
        address = request.form.get('address','').strip()
        if not re.match(r'^\d{10}$', phone):
            flash('Phone must be exactly 10 digits.', 'error'); return render_template('register.html')
        if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
            flash('Invalid email address.', 'error'); return render_template('register.html')
        if len(password) < 6:
            flash('Password must be at least 6 characters.', 'error'); return render_template('register.html')
        db = get_db()
        if db.execute("SELECT id FROM users WHERE phone=? OR email=?", (phone,email)).fetchone():
            flash('Phone or email already registered.', 'error'); return render_template('register.html')
        db.execute('INSERT INTO users (phone,email,password,shop_name,address,trial_start) VALUES (?,?,?,?,?,datetime("now"))',
                   (phone, email, hash_pw(password), shop_name, address))
        db.commit()
        flash('Registration successful! Your 30-day free trial has started.', 'success')
        return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear(); return redirect(url_for('login'))

@app.route('/subscription')
@login_required
def subscription_page():
    reason = request.args.get('reason','')
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (session['user_id'],)).fetchone()
    return render_template('subscription.html', user=user, status=subscription_status(user),
                           days_left=days_left(user), reason=reason)

@app.route('/dashboard')
@active_required
def dashboard():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (session['user_id'],)).fetchone()
    uid = session['user_id']
    job_count = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=?", (uid,)).fetchone()[0]
    invoice_count = db.execute("SELECT COUNT(*) FROM invoices WHERE user_id=?", (uid,)).fetchone()[0]
    pending = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=? AND status NOT IN ('Delivered','Cancelled')", (uid,)).fetchone()[0]
    delivered = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=? AND status='Delivered'", (uid,)).fetchone()[0]
    overdue = db.execute("""SELECT COUNT(*) FROM repair_jobs WHERE user_id=? AND status NOT IN ('Delivered','Cancelled')
                            AND expected_return IS NOT NULL AND expected_return!='' AND expected_return < date('now')""", (uid,)).fetchone()[0]
    partial = db.execute("SELECT COUNT(*) FROM repair_jobs WHERE user_id=? AND paid_status='Partial' AND status='Delivered'", (uid,)).fetchone()[0]
    recent_jobs = db.execute("SELECT * FROM repair_jobs WHERE user_id=? ORDER BY created_at DESC LIMIT 6", (uid,)).fetchall()
    return render_template('dashboard.html', user=user, status=subscription_status(user),
                           days_left=days_left(user), job_count=job_count, invoice_count=invoice_count,
                           pending=pending, delivered=delivered, recent_jobs=recent_jobs,
                           overdue_count=overdue, partial_count=partial)

@app.route('/jobs')
@active_required
def jobs():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (session['user_id'],)).fetchone()
    uid = session['user_id']
    all_jobs = db.execute("SELECT * FROM repair_jobs WHERE user_id=? ORDER BY created_at DESC", (uid,)).fetchall()
    jobs_list = [dict(j) for j in all_jobs]
    today_str = datetime.now(IST).strftime('%Y-%m-%d')
    overdue_jobs = [j for j in jobs_list if j.get('expected_return') and j['expected_return'] < today_str
                    and j['status'] not in ('Delivered','Cancelled')]
    partial_jobs = [j for j in jobs_list if j.get('paid_status') == 'Partial' and j['status'] == 'Delivered']
    return render_template('jobs.html', jobs=all_jobs, jobs_json=json.dumps(jobs_list),
                           user=user, status=subscription_status(user), days_left=days_left(user),
                           overdue_jobs=overdue_jobs, partial_jobs=partial_jobs)

@app.route('/jobs/add', methods=['GET','POST'])
@active_required
def add_job():
    if request.method == 'POST':
        db = get_db()
        adv = float(request.form.get('advance_amount') or 0)
        cost = float(request.form.get('cost') or 0)
        paid_status = 'Paid' if adv>0 and cost>0 and adv>=cost else ('Partial' if adv>0 else 'Unpaid')
        happy_code = generate_happy_code()
        db.execute('''INSERT INTO repair_jobs
                      (user_id,customer_name,customer_phone,device_model,imei,imei_billing,
                       issue,aadhar_number,received_without,cost,notes,advance_amount,advance_method,
                       paid_status,expected_return,happy_code)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                   (session['user_id'],
                    request.form.get('customer_name','').upper(),
                    request.form.get('customer_phone',''),
                    (request.form.get('device_model','') or 
                     (request.form.get('device_brand','') + ' ' + request.form.get('device_model_only','')).strip()).upper(),
                    request.form.get('imei','').upper(),
                    request.form.get('imei_billing','').upper(),
                    request.form.get('issue','').upper(),
                    request.form.get('aadhar_number','').replace(' ',''),
                    request.form.get('received_without_val',''),
                    cost, request.form.get('notes','').upper(),
                    adv, request.form.get('advance_method',''), paid_status,
                    request.form.get('expected_return',''),
                    happy_code))
        db.commit()
        flash('Repair job added successfully!', 'success')
        return redirect(url_for('jobs'))
    return render_template('add_job.html')

@app.route('/jobs/<int:job_id>/update', methods=['POST'])
@active_required
def update_job(job_id):
    db = get_db()
    adv_str = request.form.get('advance_amount')
    new_adv = float(adv_str or 0) if adv_str is not None else 0
    existing = db.execute("SELECT advance_amount, advance_history FROM repair_jobs WHERE id=? AND user_id=?",
                          (job_id, session['user_id'])).fetchone()
    # Only update cost/notes/quote_items if explicitly sent (not just a status update)
    has_cost = 'cost' in request.form
    set_parts = ['status=?', 'diagnosed_at=COALESCE(diagnosed_at,datetime("now"))', 'updated_at=datetime("now")']
    params = [request.form.get('status')]
    if has_cost:
        set_parts.insert(1, 'cost=?')
        set_parts.insert(2, 'notes=?')
        set_parts.insert(3, 'quote_items=?')
        params += [float(request.form.get('cost') or 0),
                   request.form.get('notes', ''),
                   request.form.get('quote_items', '')]
    if adv_str is not None and new_adv > 0:
        adv_method = request.form.get('advance_method', '')
        old_total = float(existing['advance_amount'] or 0) if existing else 0
        history = json.loads(existing['advance_history'] or '[]') if existing else []
        history.append({'amount': new_adv, 'method': adv_method,
                        'date': datetime.now(IST).strftime('%Y-%m-%d %H:%M')})
        total_adv = old_total + new_adv
        set_parts.insert(-2, 'advance_amount=?')
        set_parts.insert(-2, 'advance_method=?')
        set_parts.insert(-2, 'advance_history=?')
        params += [total_adv, adv_method, json.dumps(history)]
    params += [job_id, session['user_id']]
    db.execute(f"UPDATE repair_jobs SET {', '.join(set_parts)} WHERE id=? AND user_id=?", params)
    db.commit()
    return jsonify({'ok': True})

@app.route('/jobs/<int:job_id>/verify_happy', methods=['POST'])
@active_required
def verify_happy_code(job_id):
    code = request.json.get('code','').strip()
    db = get_db()
    job = db.execute("SELECT happy_code,status FROM repair_jobs WHERE id=? AND user_id=?",
                     (job_id, session['user_id'])).fetchone()
    if not job: return jsonify({'ok': False, 'error': 'Job not found'}), 404
    if job['happy_code'] == code:
        return jsonify({'ok': True})
    return jsonify({'ok': False, 'error': 'Invalid Happy Code'})

@app.route('/jobs/<int:job_id>/set_reminder', methods=['GET','POST'])
@active_required
def set_reminder(job_id):
    db = get_db()
    if request.method == 'POST':
        reminder_date = request.form.get('reminder_date','')
        db.execute("UPDATE repair_jobs SET reminder_date=? WHERE id=? AND user_id=?",
                   (reminder_date, job_id, session['user_id']))
        db.commit()
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'ok': True})
        flash('Reminder set!', 'success')
        return redirect(url_for('jobs'))
    job = db.execute("SELECT * FROM repair_jobs WHERE id=? AND user_id=?", (job_id, session['user_id'])).fetchone()
    if not job: return redirect(url_for('jobs'))
    return render_template('set_reminder.html', job=job)

@app.route('/jobs/<int:job_id>/deliver', methods=['POST'])
@active_required
def deliver_job(job_id):
    data = request.get_json(force=True)
    total = float(data.get('total', 0))
    advance = float(data.get('advance', 0))
    amount_paid_now = float(data.get('amountPaidNow', 0))
    discount = float(data.get('discount', 0))
    pay_method = data.get('payMethod', 'Cash')
    credit_due_date = data.get('creditDueDate') or None
    items_str = json.dumps(data.get('items', []))
    total_collected = advance + amount_paid_now
    balance = max(0, total - total_collected)
    paid = 'Paid' if balance < 0.01 else ('Partial' if total_collected > 0 else 'Unpaid')
    delivery_date = datetime.now(IST).strftime('%Y-%m-%d')
    db = get_db()
    imei = data.get('imei','').strip().upper()
    if imei:
        db.execute("UPDATE repair_jobs SET imei_billing=? WHERE id=? AND user_id=? AND (imei_billing IS NULL OR imei_billing='')",
                   (imei, job_id, session['user_id']))
    db.execute('''UPDATE repair_jobs SET status='Delivered',cost=?,delivery_date=?,paid_status=?,updated_at=datetime("now")
                  WHERE id=? AND user_id=?''',
               (total, delivery_date, paid, job_id, session['user_id']))
    db.execute('''INSERT INTO invoices (user_id,job_id,customer_name,customer_phone,items,total,advance_amount,discount,pay_method,paid,due_date)
                  SELECT ?,id,customer_name,customer_phone,?,?,?,?,?,?,? FROM repair_jobs WHERE id=?''',
               (session['user_id'], items_str, total, total_collected, discount, pay_method, paid, credit_due_date, job_id))
    db.commit()
    inv = db.execute("SELECT id FROM invoices WHERE job_id=? ORDER BY id DESC LIMIT 1", (job_id,)).fetchone()
    return jsonify({'ok': True, 'inv_id': inv['id'] if inv else None})

@app.route('/jobs/<int:job_id>/cancel', methods=['POST'])
@active_required
def cancel_job_route(job_id):
    reason = request.form.get('reason','Cancelled')
    db = get_db()
    db.execute("UPDATE repair_jobs SET status='Cancelled',cancel_reason=?,updated_at=datetime('now') WHERE id=? AND user_id=?",
               (reason, job_id, session['user_id']))
    db.commit()
    return jsonify({'ok': True})

@app.route('/jobs/<int:job_id>/record_refund', methods=['POST'])
@active_required
def record_refund(job_id):
    amount = float(request.form.get('amount', 0))
    method = request.form.get('method', 'Cash')
    date = request.form.get('date', datetime.now(IST).strftime('%Y-%m-%d'))
    db = get_db()
    db.execute("UPDATE repair_jobs SET refund_amount=?,refund_method=?,refund_date=? WHERE id=? AND user_id=?",
               (amount, method, date, job_id, session['user_id']))
    db.commit()
    return jsonify({'ok': True})

@app.route('/jobs/<int:job_id>/rework', methods=['POST'])
@active_required
def rework_job(job_id):
    details = request.form.get('details','')
    db = get_db()
    orig = db.execute("SELECT * FROM repair_jobs WHERE id=? AND user_id=?", (job_id, session['user_id'])).fetchone()
    if not orig: return jsonify({'error':'Not found'}), 404
    happy_code = generate_happy_code()
    db.execute('''INSERT INTO repair_jobs
                  (user_id,customer_name,customer_phone,device_model,imei,imei_billing,
                   issue,aadhar_number,received_without,cost,notes,status,happy_code)
                  VALUES (?,?,?,?,?,?,?,?,?,?,?,'Rework',?)''',
               (session['user_id'], orig['customer_name'], orig['customer_phone'],
                orig['device_model'], orig['imei'], orig['imei_billing'],
                f"REWORK: {details}", orig['aadhar_number'], orig['received_without'],
                0, f"Original Job: #{job_id} | {details}", happy_code))
    db.commit()
    return jsonify({'ok': True})

@app.route('/jobs/<int:job_id>/delete', methods=['POST'])
@active_required
def delete_job(job_id):
    db = get_db()
    db.execute("DELETE FROM repair_jobs WHERE id=? AND user_id=?", (job_id, session['user_id']))
    db.commit()
    flash('Job deleted.', 'success')
    return redirect(url_for('jobs'))

@app.route('/invoices')
@active_required
def invoices():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (session['user_id'],)).fetchone()
    all_inv = db.execute("SELECT * FROM invoices WHERE user_id=? ORDER BY created_at DESC", (session['user_id'],)).fetchall()
    return render_template('invoices.html', invoices=all_inv, user=user,
                           status=subscription_status(user), days_left=days_left(user))

@app.route('/invoices/create', methods=['GET','POST'])
@active_required
def create_invoice():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (session['user_id'],)).fetchone()
    jobs_list = db.execute("SELECT * FROM repair_jobs WHERE user_id=? ORDER BY created_at DESC", (session['user_id'],)).fetchall()
    if request.method == 'POST':
        db.execute('INSERT INTO invoices (user_id,job_id,customer_name,customer_phone,items,total) VALUES (?,?,?,?,?,?)',
                   (session['user_id'], request.form.get('job_id') or None,
                    request.form.get('customer_name'), request.form.get('customer_phone'),
                    request.form.get('items'), float(request.form.get('total') or 0)))
        db.commit()
        flash('Invoice created!', 'success')
        return redirect(url_for('invoices'))
    return render_template('create_invoice.html', jobs=jobs_list, user=user)

@app.route('/invoices/<int:inv_id>/print')
@active_required
def print_invoice(inv_id):
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (session['user_id'],)).fetchone()
    inv = db.execute("SELECT * FROM invoices WHERE id=? AND user_id=?", (inv_id, session['user_id'])).fetchone()
    if not inv:
        flash('Invoice not found.', 'error')
        return redirect(url_for('invoices'))
    job = db.execute("SELECT * FROM repair_jobs WHERE id=? AND user_id=?", (inv['job_id'], session['user_id'])).fetchone() if inv['job_id'] else None
    return render_template('print_invoice.html', inv=inv, user=user, job=job)

@app.route('/invoices/<int:inv_id>/mark_paid', methods=['POST'])
@active_required
def mark_invoice_paid(inv_id):
    amount_received = float(request.form.get('amount_received', 0))
    pay_method = request.form.get('pay_method', 'Cash')
    due_date = request.form.get('due_date', '')
    db = get_db()
    inv = db.execute("SELECT * FROM invoices WHERE id=? AND user_id=?", (inv_id, session['user_id'])).fetchone()
    if not inv: return jsonify({'error': 'Not found'}), 404
    new_adv = float(inv['advance_amount'] or 0) + amount_received
    total = float(inv['total'] or 0)
    balance = max(0, total - new_adv)
    paid = 'Paid' if balance < 0.01 else ('Partial' if new_adv > 0 else 'Unpaid')
    db.execute("UPDATE invoices SET advance_amount=?,pay_method=?,paid=?,due_date=? WHERE id=?",
               (new_adv, pay_method, paid, due_date or None, inv_id))
    if inv['job_id']:
        db.execute("UPDATE repair_jobs SET paid_status=? WHERE id=? AND user_id=?", (paid, inv['job_id'], session['user_id']))
    db.commit()
    return jsonify({'ok': True, 'paid': paid, 'advance': new_adv, 'balance': balance})

@app.route('/api/customers/search')
@active_required
def customer_search():
    q = request.args.get('q', '').strip().upper()
    if len(q) < 2:
        return jsonify([])
    db = get_db()
    rows = db.execute(
        """SELECT DISTINCT customer_name, customer_phone
           FROM repair_jobs
           WHERE user_id=? AND (
               UPPER(customer_name) LIKE ? OR customer_phone LIKE ?
           )
           ORDER BY created_at DESC
           LIMIT 10""",
        (session['user_id'], f'%{q}%', f'%{q}%')
    ).fetchall()
    return jsonify([{'name': r['customer_name'], 'phone': r['customer_phone']} for r in rows])

@app.route('/customers')
@active_required
def customers():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (session['user_id'],)).fetchone()
    all_jobs = db.execute("SELECT * FROM repair_jobs WHERE user_id=? ORDER BY created_at DESC", (session['user_id'],)).fetchall()
    all_invs = db.execute("SELECT id, job_id, total, advance_amount, paid, due_date FROM invoices WHERE user_id=?", (session['user_id'],)).fetchall()
    inv_map = {r['job_id']: r['id'] for r in all_invs}
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
            if due > 0:
                customer_map[ph]['total_due'] += due
        # Add invoice balance for delivered jobs with partial/unpaid invoices
        if job['status'] == 'Delivered' and inv_info and inv_info['balance'] > 0.01:
            customer_map[ph]['total_due'] += inv_info['balance']
    return render_template('customers.html',
                           customers=sorted(customer_map.values(), key=lambda x: x['total_business'], reverse=True),
                           user=user, status=subscription_status(user), days_left=days_left(user))

@app.route('/reports')
@active_required
def reports():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (session['user_id'],)).fetchone()
    uid = session['user_id']
    from datetime import date
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    def count(where, params=()):
        return db.execute(f"SELECT COUNT(*) FROM repair_jobs WHERE user_id=? {where}", (uid,)+params).fetchone()[0]
    def revenue(where, params=()):
        r = db.execute(f"SELECT SUM(cost) FROM repair_jobs WHERE user_id=? {where}", (uid,)+params).fetchone()[0]
        return float(r or 0)
    stats = {
        'total': count(''),
        'pending': count("AND status NOT IN ('Delivered','Cancelled')"),
        'delivered': count("AND status='Delivered'"),
        'cancelled': count("AND status='Cancelled'"),
        'rework': count("AND status='Rework'"),
        'today_new': count("AND date(created_at)=date('now')"),
        'week_new': count("AND date(created_at)>=?", (str(week_start),)),
        'month_new': count("AND date(created_at)>=?", (str(month_start),)),
        'month_revenue': revenue("AND status='Delivered' AND date(delivery_date)>=?", (str(month_start),)),
        'total_revenue': revenue("AND status='Delivered'"),
        'partial_pending': count("AND paid_status='Partial'"),
        'overdue': count("AND status NOT IN ('Delivered','Cancelled') AND expected_return IS NOT NULL AND expected_return!='' AND expected_return<date('now')"),
    }
    recent_delivered = db.execute("SELECT * FROM repair_jobs WHERE user_id=? AND status='Delivered' ORDER BY delivery_date DESC LIMIT 10", (uid,)).fetchall()
    return render_template('reports.html', user=user, stats=stats, recent_delivered=recent_delivered,
                           status=subscription_status(user), days_left=days_left(user))

@app.route('/settings', methods=['GET','POST'])
@login_required
def settings():
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (session['user_id'],)).fetchone()
    if request.method == 'POST':
        shop_name = request.form.get('shop_name','').strip()
        address = request.form.get('address','').strip()
        new_pw = request.form.get('new_password','')
        logo_data = None
        if 'logo' in request.files:
            f = request.files['logo']
            if f and f.filename:
                mime = f.content_type or 'image/png'
                logo_data = 'data:' + mime + ';base64,' + base64.b64encode(f.read()).decode()
        db.execute("UPDATE users SET shop_name=?,address=? WHERE id=?", (shop_name, address, session['user_id']))
        if new_pw and len(new_pw) >= 6:
            db.execute("UPDATE users SET password=? WHERE id=?", (hash_pw(new_pw), session['user_id']))
        if logo_data:
            db.execute("UPDATE users SET logo=? WHERE id=?", (logo_data, session['user_id']))
        db.commit()
        session['shop_name'] = shop_name
        flash('Settings saved!', 'success')
        return redirect(url_for('settings'))
    return render_template('settings.html', user=user, status=subscription_status(user), days_left=days_left(user))

@app.route('/admin')
@admin_required
def admin_dashboard():
    db = get_db()
    users = db.execute("SELECT * FROM users WHERE role='user' ORDER BY created_at DESC").fetchall()
    enriched = [{**dict(u), 'status': subscription_status(u), 'days_left': days_left(u)} for u in users]
    total = len(users)
    return render_template('admin_dashboard.html', users=enriched, total=total,
                           active_count=sum(1 for u in users if subscription_status(u) in ('trial','active')),
                           expired_count=sum(1 for u in users if subscription_status(u) in ('trial_expired','expired')),
                           disabled_count=sum(1 for u in users if not u['enabled']))

@app.route('/admin/toggle/<int:uid>', methods=['POST'])
@admin_required
def admin_toggle(uid):
    db = get_db()
    user = db.execute("SELECT enabled FROM users WHERE id=?", (uid,)).fetchone()
    new_state = 0 if user['enabled'] else 1
    db.execute("UPDATE users SET enabled=? WHERE id=?", (new_state, uid))
    db.commit()
    return jsonify({'enabled': new_state})

@app.route('/admin/set_subscription/<int:uid>', methods=['POST'])
@admin_required
def admin_set_subscription(uid):
    plan = request.form.get('plan')
    days = {'30d':30,'1y':365,'2y':730,'3y':1095}.get(plan)
    if not days: return jsonify({'error': 'Invalid plan'}), 400
    end_date = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    db = get_db()
    db.execute("UPDATE users SET subscription_plan=?,subscription_end=?,enabled=1 WHERE id=?", (plan, end_date, uid))
    db.commit()
    return jsonify({'success': True, 'end_date': end_date[:10]})

@app.route('/admin/delete/<int:uid>', methods=['POST'])
@admin_required
def admin_delete_user(uid):
    db = get_db()
    db.execute("DELETE FROM repair_jobs WHERE user_id=?", (uid,))
    db.execute("DELETE FROM invoices WHERE user_id=?", (uid,))
    db.execute("DELETE FROM users WHERE id=?", (uid,))
    db.commit()
    return jsonify({'success': True})

@app.route('/manual')
@login_required
def user_manual():
    return render_template('user_manual.html')

os.makedirs('instance', exist_ok=True)
init_db()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
