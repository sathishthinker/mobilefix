# MobileFix Pro — Repair Shop Management System

## Quick Setup

```bash
# 1. Install dependencies
pip install flask

# 2. Run the app
python app.py
```

Visit: http://localhost:5000

---

## Default Admin Login

| Field    | Value                |
|----------|----------------------|
| Email    | admin@mobilefix.com  |
| Password | admin123             |

> ⚠️ Change the admin password after first login!

---

## Features

### User (Shop) Panel
- 🆓 **30-day free trial** on registration (phone + email required)
- 📱 **Repair Job Tracking** — Add, update, delete jobs with status workflow
- 🧾 **Invoice Generation** — Create & print professional invoices
- ⏰ **Expiry Popup** — Alert shown when ≤7 days remaining
- 🔒 **Settings** — Update shop name, address, password

### Admin Panel
- 👥 Manage all registered shops
- ✅ / 🚫 Enable / Disable accounts
- 📅 Set subscription plans: **30 Days | 1 Year | 2 Years | 3 Years**
- 🗑️ Delete shop accounts
- 📊 Dashboard stats (total, active, expired, disabled)

### Subscription Flow
1. User registers → 30-day free trial starts automatically
2. After 30 days → Trial expired page shown, access blocked
3. Admin manually activates a plan from admin panel
4. User regains access until subscription end date

---

## Project Structure

```
mobileFix/
├── app.py                  # Flask backend
├── requirements.txt
├── instance/
│   └── mobilefix.db        # SQLite database (auto-created)
└── templates/
    ├── base.html
    ├── login.html
    ├── register.html
    ├── dashboard.html
    ├── jobs.html
    ├── add_job.html
    ├── invoices.html
    ├── create_invoice.html
    ├── print_invoice.html
    ├── settings.html
    ├── subscription.html
    └── admin_dashboard.html
```

## Subscription Plan Codes
| Code | Duration |
|------|----------|
| 30d  | 30 Days  |
| 1y   | 1 Year   |
| 2y   | 2 Years  |
| 3y   | 3 Years  |
