# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MobileFix Pro v4 is a Flask-based repair shop management system for mobile phone repair businesses. It handles job tracking, invoicing, customer management, and multi-tenant shop subscriptions.

## Setup & Running

```bash
pip install flask
python app.py
```

App runs at `http://localhost:5000` with debug mode enabled.

Default admin credentials: `admin@mobilefix.com` / `admin123`

## Architecture

**Single-file Flask app** (`app.py`, ~554 lines) with Jinja2 templates. No ORM — uses raw `sqlite3` with the database auto-created at `instance/mobilefix.db`.

### Key Patterns

- **Access control decorators**: `@login_required`, `@admin_required`, `@active_required` — applied to routes to enforce session-based auth and subscription status
- **Database access**: `get_db()` returns a `sqlite3.Connection` with `row_factory = sqlite3.Row`; `init_db()` creates tables and seeds the admin user on first run
- **Multi-tenancy**: Each shop owner (`role='user'`) has a `user_id` foreign key on `repair_jobs` and `invoices` — all queries filter by `session['user_id']`
- **Subscription system**: 30-day free trial on registration; admin activates paid plans (`30d`, `1y`, `2y`, `3y`) via `/admin/set_subscription/<uid>`

### Database Schema

Three tables: `users`, `repair_jobs`, `invoices`

- `repair_jobs.status`: `Received` → `Diagnosing` → `Repairing` → `Ready` → `Delivered` (also `Cancelled`, `Rework`)
- `repair_jobs.paid_status`: `Unpaid` / `Partial` / `Paid`
- `repair_jobs.quote_items`: stored as JSON string
- `invoices.items`: stored as JSON string
- Passwords hashed with SHA256 (no salt)

### Templates

14 Jinja2 templates in `templates/`. `base.html` provides the sidebar/topbar shell; all other pages extend it. Inline CSS and JS throughout — no separate asset files or bundler.

## Important Notes

- The Flask `SECRET_KEY` is regenerated with `os.urandom(24)` on every restart, which invalidates all existing sessions on restart
- No test suite exists
- No environment variable / `.env` support — configuration is hardcoded in `app.py`
