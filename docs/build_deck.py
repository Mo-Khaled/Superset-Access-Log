"""Build docs/SCB_Pipeline_Flow.pptx (Suez Canal Bank themed). Run: python docs/build_deck.py"""
from pathlib import Path
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR

HERE = Path(__file__).parent
LOGO = str(HERE / "assets" / "scb_logo.png")
BLUE = RGBColor(0x1B, 0x51, 0xA4)
DARK = RGBColor(0x2B, 0x30, 0x38)
GREY = RGBColor(0x5A, 0x62, 0x6E)
PALE = RGBColor(0xE8, 0xEF, 0xF9)
RED = RGBColor(0xC0, 0x39, 0x2B)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
FONT = "Calibri"

prs = Presentation()
prs.slide_width, prs.slide_height = Inches(13.333), Inches(7.5)
BLANK = prs.slide_layouts[6]


def text(slide, x, y, w, h, s, size=16, bold=False, color=DARK, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP):
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    lines = s if isinstance(s, list) else [s]
    for i, line in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align
        p.space_after = Pt(6)
        r = p.add_run()
        r.text = line
        r.font.size, r.font.bold, r.font.name = Pt(size), bold, FONT
        r.font.color.rgb = color
    return tb


def bullets(slide, x, y, w, h, items, size=17):
    return text(slide, x, y, w, h, ["•  " + i for i in items], size=size)


def box(slide, x, y, w, h, title, sub="", fill=PALE, line=BLUE, tcolor=BLUE, size=14):
    sh = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
    sh.adjustments[0] = 0.12
    sh.fill.solid()
    sh.fill.fore_color.rgb = fill
    sh.line.color.rgb = line
    sh.line.width = Pt(1.5)
    sh.shadow.inherit = False
    tf = sh.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    for m in ("margin_left", "margin_right"):
        setattr(tf, m, Inches(0.08))
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    r = p.add_run()
    r.text = title
    r.font.size, r.font.bold, r.font.name = Pt(size), True, FONT
    r.font.color.rgb = tcolor
    if sub:
        p2 = tf.add_paragraph()
        p2.alignment = PP_ALIGN.CENTER
        r2 = p2.add_run()
        r2.text = sub
        r2.font.size, r2.font.name = Pt(size - 3), FONT
        r2.font.color.rgb = GREY
    return sh


def arrow(slide, x1, y1, x2, y2):
    c = slide.shapes.add_connector(1, Inches(x1), Inches(y1), Inches(x2), Inches(y2))
    c.line.color.rgb = BLUE
    c.line.width = Pt(2)
    ln = c.line._get_or_add_ln()
    ln.append(ln.makeelement("{http://schemas.openxmlformats.org/drawingml/2006/main}tailEnd", {"type": "triangle"}))


def wave(slide, y, color=BLUE):
    """Three stacked bars echoing the logo's wave mark."""
    for i in range(3):
        b = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.6 + i * 0.35), Inches(y), Inches(0.28), Inches(0.12))
        b.adjustments[0] = 0.5
        b.fill.solid()
        b.fill.fore_color.rgb = color
        b.line.fill.background()


def content(title, n):
    s = prs.slides.add_slide(BLANK)
    s.shapes.add_picture(LOGO, Inches(11.55), Inches(0.12), height=Inches(1.15))
    wave(s, 0.5)
    text(s, 0.6, 0.65, 10.8, 0.9, title, size=30, bold=True, color=BLUE)
    bar = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.6), Inches(1.45), Inches(12.1), Inches(0.03))
    bar.fill.solid()
    bar.fill.fore_color.rgb = BLUE
    bar.line.fill.background()
    text(s, 0.6, 7.0, 9, 0.35, "Superset Dashboard Access Log  |  Failure rate & load time pipeline", size=10, color=GREY)
    text(s, 12.0, 7.0, 0.8, 0.35, str(n), size=10, color=GREY, align=PP_ALIGN.RIGHT)
    return s


# 1 Title
s = prs.slides.add_slide(BLANK)
s.shapes.add_picture(LOGO, Inches(0.9), Inches(0.8), height=Inches(2.6))
text(s, 0.9, 3.9, 11.5, 1.2, "Superset Dashboard Access Log", size=44, bold=True, color=BLUE)
text(s, 0.9, 4.9, 11.5, 1.0, "From silent chart timeouts to a measured failure rate and load time", size=24, color=DARK)
text(s, 0.9, 6.3, 11.5, 0.5, "End-to-end pipeline flow  |  Superset, Trino, Iceberg", size=14, color=GREY)

# 2 Problem
s = content("The problem: a silent, intermittent chart error", 2)
bullets(s, 0.6, 1.8, 6.4, 5, [
    "A chart query runs longer than the gunicorn timeout (15s).",
    "Gunicorn kills the worker; the user just sees a generic error.",
    "Superset logs nothing server-side for that request.",
    "The error comes and goes: Redis caches a successful result for 300s.",
    "Goal: measure, per dashboard, how often charts fail (any cause) and how fast they load.",
], size=18)
box(s, 7.6, 2.0, 5.0, 0.9, "User opens dashboard", "chart query starts", size=15)
box(s, 7.6, 3.3, 5.0, 0.9, "Query takes 18s", "gunicorn limit is 15s", size=15)
box(s, 7.6, 4.6, 5.0, 0.9, "Worker SIGKILLed", "no Superset log row", fill=RGBColor(0xFB, 0xE9, 0xE7), line=RED, tcolor=RED, size=15)
box(s, 7.6, 5.9, 5.0, 0.8, "Generic error in browser", "only the browser knows", size=15)
for y in (2.9, 4.2, 5.5):
    arrow(s, 10.1, y, 10.1, y + 0.4)

# 3 Architecture
s = content("Architecture: two small stacks side by side", 3)
text(s, 0.6, 1.65, 6, 0.4, "Main stack (docker-compose.yml)", size=16, bold=True, color=BLUE)
box(s, 0.6, 2.1, 2.3, 0.9, "Superset 4.1.4", "gunicorn :8089")
box(s, 3.1, 2.1, 2.3, 0.9, "Postgres", "metadata + logs")
box(s, 5.6, 2.1, 2.3, 0.9, "Redis", "cache + Celery")
box(s, 0.6, 3.3, 2.3, 0.9, "Pipeline container", "ETL + seed + repro")
box(s, 3.1, 3.3, 2.3, 0.9, "Superset Celery", "worker + beat")
box(s, 5.6, 3.3, 2.3, 0.9, "Bootstrap", "dashboards (one-shot)")
text(s, 0.6, 4.6, 6, 0.4, "Lakehouse stand-in (lakehouse/docker-compose.yml)", size=16, bold=True, color=BLUE)
box(s, 0.6, 5.05, 2.3, 0.9, "MinIO", "warehouse bucket")
box(s, 3.1, 5.05, 2.3, 0.9, "Iceberg REST", "catalog :8183")
box(s, 5.6, 5.05, 2.3, 0.9, "Trino 483", "SQL :8085")
arrow(s, 2.9, 5.5, 3.1, 5.5)
arrow(s, 5.4, 5.5, 5.6, 5.5)
bullets(s, 8.4, 2.1, 4.4, 4.5, [
    "Lakehouse is a stand-in for the bank's real Iceberg + Trino platform.",
    "connect-pipeline.yml joins pipeline, superset and bootstrap to the lakehouse network.",
    "Iceberg is the only store for the pipeline: no MySQL.",
    "Superset gets a Trino connection via trino[sqlalchemy].",
], size=15)

# 4 End-to-end flow
s = content("End-to-end data flow", 4)
steps = [("1  Traffic", "browser + seed"), ("2  Superset logs", "logs table + stderr"),
         ("3  ETL", "sync_logs + ingest"), ("4  Classify", "category + signature"),
         ("5  Iceberg", "MERGE via Trino"), ("6  Superset", "reliability dashboard")]
for i, (t, sub) in enumerate(steps):
    x = 0.6 + i * 2.05
    box(s, x, 2.3, 1.8, 1.3, t, sub, size=14)
    if i < len(steps) - 1:
        arrow(s, x + 1.8, 2.95, x + 2.05, 2.95)
bullets(s, 0.6, 4.1, 12.1, 2.8, [
    "run_etl.sh is one cycle, scheduled every 10 minutes: iceberg_init.py, sync_logs.py, then container logs piped into ingest_container_errors.py.",
    "Every write is idempotent (MERGE by key, stable dedupe_key), so re-reads never duplicate rows.",
    "Watermark = max(source_dttm) of raw_log_landing, written last, so it never gets ahead of the data.",
], size=17)

# 5 Log sources
s = content("Log sources and server-side tracing", 5)
box(s, 0.6, 1.9, 3.8, 1.2, "Superset logs table", "read through /api/v1/log/")
box(s, 0.6, 3.4, 3.8, 1.2, "Browser events", "mount_dashboard, load_chart, has_err")
box(s, 0.6, 4.9, 3.8, 1.2, "Container stdout/stderr", "superset + superset-worker")
bullets(s, 4.9, 1.9, 7.9, 5, [
    "Superset 4.1.4 browsers write action='log' with json.event_name; the ETL maps these to dashboard_view / chart_view.",
    "FLASK_APP_MUTATOR emits REQTRACE for every request start (pid, user, request id, url).",
    "REQEND marks dashboard/chart requests ending in HTTP 400+; REQEXC marks unhandled exceptions.",
    "A killed request never reaches an end hook, so REQTRACE is the only trace of it.",
    "A gunicorn timeout line carries only a pid; it is joined to that pid's last REQTRACE (max 120s old) to recover user, dashboard and chart.",
], size=16)

# 6 ETL
s = content("ETL: incremental, safe, repeatable", 6)
bullets(s, 0.6, 1.8, 6.0, 5, [
    "sync_logs.py pulls dttm > watermark, starting 24h earlier as a lookback.",
    "A lease row in Iceberg (etl_lock) keeps it to a single writer; commit conflicts are retried.",
    "Facts and dims are written first, raw_log_landing last; rows already stored are skipped.",
    "Browser load_chart with has_err becomes a fact_error_events row (source=browser).",
], size=17)
bullets(s, 6.9, 1.8, 5.9, 5, [
    "Reconciliation: if Superset has more rows than the landing table, re-pull once with a warning.",
    "--since and --full force a backfill; future-dated rows never move the watermark.",
    "backfill_iceberg.py replays history through the same code and can reclassify old errors.",
    "acceptance_watermark.py tests rows 3h old, 10 days old and tomorrow.",
], size=17)

# 7 Classification
s = content("Every failure is stored: category, signature, raw message", 7)
cats = ["timeout", "worker_failure", "connection", "permission", "query_error", "not_found", "rate_limit", "data_error"]
for i, c in enumerate(cats):
    box(s, 0.6 + (i % 4) * 3.05, 1.8 + (i // 4) * 0.9, 2.85, 0.7, c, size=14)
box(s, 0.6, 3.7, 4.0, 0.7, "client_error / server_error", "HTTP status fallbacks", size=13)
box(s, 4.8, 3.7, 4.0, 0.7, "unclassified", "stored, never dropped", fill=RGBColor(0xFF, 0xF4, 0xDD), line=RGBColor(0xD9, 0x8E, 0x04), tcolor=RGBColor(0x9A, 0x62, 0x00), size=13)
bullets(s, 0.6, 4.7, 12.1, 2.2, [
    "error_classify.py: first matching rule wins; row_category_and_signature() is the single function every writer uses.",
    "Signature = message with numbers, UUIDs, ids, timestamps, IPs and quoted values stripped, so the same cause groups together.",
    "A growing 'unclassified' signature in Top Error Messages is how a new cause gets noticed.",
], size=16)

# 8 Iceberg
s = content("Iceberg tables and the failure-rate view", 8)
box(s, 0.6, 1.9, 3.9, 1.0, "fact_access_events", "day(event_ts), duration_ms, is_error")
box(s, 0.6, 3.1, 3.9, 1.0, "fact_error_events", "source, category, signature, message")
box(s, 0.6, 4.3, 3.9, 1.0, "raw_log_landing + dims", "raw copy, dashboards, users")
box(s, 5.2, 3.1, 3.0, 1.2, "vw_chart_failure_rate", "per day and dashboard", fill=BLUE, tcolor=WHITE, line=BLUE)
arrow(s, 4.5, 2.4, 5.2, 3.5)
arrow(s, 4.5, 3.6, 5.2, 3.7)
arrow(s, 4.5, 4.8, 5.2, 3.9)
bullets(s, 8.6, 1.9, 4.2, 5, [
    "Parquet, format v2, MERGE INTO via iceberg_sink.py; daily compaction and snapshot expiry.",
    "chart_loads, rated_loads, failed_loads, server_kills, server_errors.",
    "Permission denials excluded unless AUTHZ_COUNTS_AS_FAILURE.",
    "Future-dated rows excluded.",
], size=15)

# 9 Dashboards
s = content("Superset dashboards and metric definitions", 9)
text(s, 0.6, 1.7, 6, 0.4, "Dashboard Reliability and Speed (Trino)", size=17, bold=True, color=BLUE)
bullets(s, 0.6, 2.2, 6.0, 4.8, [
    "Failure rate: daily / weekly / monthly",
    "Failed vs total chart loads",
    "Failure rate by dashboard",
    "Chart load time and slow loads",
    "Errors by category, Top error messages",
    "Chart Load Errors table",
], size=16)
text(s, 6.9, 1.7, 6, 0.4, "Definitions", size=17, bold=True, color=BLUE)
bullets(s, 6.9, 2.2, 5.9, 4.8, [
    "Failure rate = SUM(failed_loads) / SUM(rated_loads).",
    "Load time = avg and p95 of browser duration_ms, successful loads only (a timeout always reads ~15s).",
    "Slow-load rate = successful loads above SLOW_LOAD_MS (5000).",
    "A lower bound: the browser reports only while the tab stays open.",
    "Also: Superset Usage Analytics dashboard (Iceberg) for views and users.",
], size=15)

# 10 Demo
s = content("Demo: reproducing the intermittent error", 10)
demo = [("Open :8089", "fails at ~15s"), ("show_evidence.sh", "WORKER TIMEOUT, no log row"),
        ("Run ETL", "browser + gunicorn rows"), ("Open :8090", "60s limit, succeeds at 18s"),
        ("Refresh :8089", "instant (cache hit)"), ("After 300s", "error returns")]
for i, (t, sub) in enumerate(demo):
    box(s, 0.6 + i * 2.05, 1.9, 1.8, 1.3, t, sub, size=14, fill=RED if False else PALE)
    if i < len(demo) - 1:
        arrow(s, 2.4 + i * 2.05, 2.55, 2.65 + i * 2.05, 2.55)
bullets(s, 0.6, 3.7, 12.1, 3.2, [
    "Setup: gunicorn timeout 15s, slow dataset sleeps 18s (pg_sleep), chart cache 300s.",
    "The no-limit server uses CACHE_KEY_PREFIX so it does not read the main server's cached 15s browser timeout.",
    "Chart results share the superset_data_ prefix, so a result computed on :8090 is served on :8089.",
    "Teardown: docker rm -f superset-nolimit.",
], size=16)

# 11 Fix
s = content("The fix and the lesson: order the timeouts", 11)
for i, t in enumerate(["Database statement_timeout (fails first)", "gunicorn timeout (15s in the lab)", "Proxy / load balancer", "CDN"]):
    box(s, 0.6 + i * 0.5, 1.9 + i * 0.95, 6.2 - i * 0.5, 0.8, t, size=13)
bullets(s, 7.3, 1.9, 5.5, 5, [
    "Fail inside-out: the database should give up first so Superset raises and logs a real exception.",
    "Fix script: intermittent-error/fix/apply_fix.py (not applied in the demo, so the problem stays visible).",
    "gunicorn timeout only applies to sync workers; gthread hides stalls.",
    "Even with the fix, the pipeline keeps the browser-side view, so nothing is silent again.",
], size=15)

# 12 Run
s = content("How to run it, and next steps", 12)
code = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.6), Inches(1.8), Inches(7.2), Inches(4.9))
code.adjustments[0] = 0.04
code.fill.solid()
code.fill.fore_color.rgb = RGBColor(0x1E, 0x24, 0x30)
code.line.fill.background()
text(s, 0.8, 1.9, 6.9, 4.7, [
    "cp .env.example .env",
    "docker compose -f lakehouse/docker-compose.yml up -d",
    "docker compose -f docker-compose.yml \\",
    "   -f lakehouse/connect-pipeline.yml up -d --build",
    "docker compose exec -T pipeline python etl/iceberg_init.py",
    "bash access-log-pipeline/run_etl.sh",
    "docker compose ... run --rm superset-bootstrap",
    "",
    "Superset  http://localhost:8089  (admin/admin)",
], size=13, color=RGBColor(0xDD, 0xE6, 0xF5))
text(s, 8.2, 1.8, 4.6, 0.4, "Next steps for the bank platform", size=17, bold=True, color=BLUE)
bullets(s, 8.2, 2.3, 4.6, 4.5, [
    "Confirm Trino version, permissions, TLS.",
    "Decide who owns Iceberg compaction.",
    "Schedule via Kubernetes CronJob (concurrencyPolicy Forbid) or Airflow; alert on etl_run_log staleness.",
    "Automate container-log shipping (today a manual pipe).",
    "Populate source_ip (currently empty).",
], size=15)

out = HERE / "SCB_Pipeline_Flow.pptx"
prs.save(out)
print("saved", out)
