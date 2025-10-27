from flask import Flask, render_template, request, redirect, url_for, session, send_file, flash
import threading
from db import init_db, get_conn
import json, os, time
from datetime import datetime, timedelta
from parser import run_stream, check_proxy
from utils import ensure_dir, cleanup_old_files

# Load example config (can be replaced with real config)
cfg = {}
try:
    with open("example_config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
except:
    cfg = {"ADMIN_USER":"admin","ADMIN_PASS":"changeme","DATA_DIR":"data","FILE_RETENTION_DAYS":7,"MAX_WORKERS":8}

app = Flask(__name__)
app.secret_key = cfg.get("SECRET_KEY", "devsecret")
DATA_DIR = cfg.get("DATA_DIR","data")
ensure_dir(DATA_DIR)

# DB init
init_db()

# In-memory registry of running threads and stop flags
RUNNING = {}
RUNNING_LOCK = threading.Lock()

def update_state_callback(stream_id, links, items):
    conn = get_conn()
    with conn:
        conn.execute("UPDATE streams SET collected_links=?, collected_items=? WHERE id=?", (links, items, stream_id))

@app.route("/", methods=["GET","POST"])
def login():
    if request.method == "POST":
        u = request.form.get("username")
        p = request.form.get("password")
        if u == cfg.get("ADMIN_USER") and p == cfg.get("ADMIN_PASS"):
            session["admin"] = True
            return redirect(url_for("dashboard"))
        flash("Неверные учётные данные", "danger")
    return render_template("login.html")

def admin_required(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*a, **kw):
        if not session.get("admin"):
            return redirect(url_for("login"))
        return f(*a, **kw)
    return wrapper

@app.route("/dashboard")
@admin_required
def dashboard():
    conn = get_conn()
    cur = conn.execute("SELECT * FROM streams ORDER BY id DESC")
    rows = cur.fetchall()
    # cleanup old files in background
    try:
        cleanup_old_files(DATA_DIR, days=cfg.get("FILE_RETENTION_DAYS",7))
    except:
        pass
    return render_template("dashboard.html", streams=rows)

@app.route("/create", methods=["GET","POST"])
@admin_required
def create():
    if request.method == "POST":
        name = request.form.get("name") or "Результат"
        proxies_text = request.form.get("proxies") or ""
        proxies = [p.strip() for p in proxies_text.splitlines() if p.strip()]
        categories_text = request.form.get("categories") or ""
        # categories: one per line (URLs)
        categories = [c.strip() for c in categories_text.splitlines() if c.strip()]
        cities = request.form.getlist("cities")
        max_workers = int(request.form.get("max_workers") or cfg.get("MAX_WORKERS",6))
        price_split = request.form.get("price_split") == "on"

        # create DB entry
        conn = get_conn()
        created_at = datetime.now().isoformat()
        expire = (datetime.now() + timedelta(days=cfg.get("FILE_RETENTION_DAYS",7))).isoformat()
        cur = conn.execute("INSERT INTO streams (name, status, created_at, proxies, categories, cities, filename, expire_at, meta) VALUES (?,?,?,?,?,?,?,?,?)",
                           (name, "running", created_at, json.dumps(proxies), json.dumps(categories), json.dumps(cities), "", expire, json.dumps({"max_workers":max_workers})))
        stream_id = cur.lastrowid
        conn.commit()

        # Start background thread
        stop_flag = threading.Event()
        def target():
            params = {"name": name, "proxies": proxies, "categories": categories, "cities": cities, "data_dir": DATA_DIR, "max_workers": max_workers, "price_split": price_split}
            result = run_stream(stream_id, params, update_state_callback=update_state_callback, stop_flag=stop_flag)
            # update DB based on result
            conn2 = get_conn()
            status = result.get("status","error")
            fname = result.get("filename","")
            with conn2:
                if status == "finished":
                    conn2.execute("UPDATE streams SET status=?, filename=?, collected_links=?, collected_items=? WHERE id=?", ("finished", fname, result.get("collected_links",0), result.get("collected_items",0), stream_id))
                else:
                    conn2.execute("UPDATE streams SET status=?, meta=? WHERE id=?", ("error", json.dumps(result), stream_id))

        t = threading.Thread(target=target, daemon=True)
        with RUNNING_LOCK:
            RUNNING[stream_id] = {"thread": t, "stop": stop_flag}
        t.start()

        return redirect(url_for("dashboard"))
    # GET: show form
    kaz_cities = ["Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Тараз","Кокшетау","Костанай","Усть-Каменогорск"]
    return render_template("create.html", cities=kaz_cities)

@app.route("/stop/<int:stream_id>")
@admin_required
def stop(stream_id):
    with RUNNING_LOCK:
        item = RUNNING.get(stream_id)
        if item:
            item["stop"].set()
            # update DB
            conn = get_conn()
            with conn:
                conn.execute("UPDATE streams SET status=? WHERE id=?", ("stopped", stream_id))
            return redirect(url_for("dashboard"))
    flash("Поток не найден или уже завершён", "warning")
    return redirect(url_for("dashboard"))

@app.route("/delete/<int:stream_id>")
@admin_required
def delete(stream_id):
    conn = get_conn()
    cur = conn.execute("SELECT filename FROM streams WHERE id=?", (stream_id,))
    row = cur.fetchone()
    if row and row["filename"]:
        try:
            os.remove(row["filename"])
        except:
            pass
    with conn:
        conn.execute("DELETE FROM streams WHERE id=?", (stream_id,))
    with RUNNING_LOCK:
        if stream_id in RUNNING:
            RUNNING[stream_id]["stop"].set()
            del RUNNING[stream_id]
    flash("Поток и файл удалены", "success")
    return redirect(url_for("dashboard"))

@app.route("/download/<int:stream_id>")
@admin_required
def download(stream_id):
    conn = get_conn()
    cur = conn.execute("SELECT filename FROM streams WHERE id=?", (stream_id,))
    row = cur.fetchone()
    if row and row["filename"] and os.path.exists(row["filename"]):
        return send_file(row["filename"], as_attachment=True)
    flash("Файл не найден", "danger")
    return redirect(url_for("dashboard"))

@app.route("/check_proxy", methods=["POST"])
@admin_required
def check_proxy_route():
    proxy = request.form.get("proxy")
    ok = check_proxy(proxy)
    return ("OK" if ok else "BAD"), 200

@app.route("/logout")
def logout():
    session.pop("admin", None)
    return redirect(url_for("login"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
