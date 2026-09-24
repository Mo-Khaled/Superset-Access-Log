from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import queries

app = FastAPI(title="Superset Usage Dashboard (standalone)")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")


@app.get("/api/top-dashboards")
def api_top_dashboards(window: str = Query("30d"), limit: int = Query(10)):
    return queries.top_dashboards(window, limit)


@app.get("/api/trend")
def api_trend(window: str = Query("30d"), top_n: int = Query(5)):
    return queries.trend_for_top_dashboards(window, top_n)


@app.get("/api/zero-view")
def api_zero_view(window: str = Query("30d")):
    return queries.zero_view_dashboards(window)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
