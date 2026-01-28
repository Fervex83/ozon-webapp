import json
import os
import re
import threading
import time
import uuid
from io import BytesIO
from pathlib import Path
from queue import Queue

from flask import Flask, Response, jsonify, render_template, request, send_file
from openpyxl import Workbook

from ozon_check import (
    CheckResult,
    check_current_page,
    check_url,
    collect_search_urls,
    expand_seller_aliases,
    normalize_text,
)
from ts import list_ts_configs, get_ts_config, load_ts_presets

BASE_DIR = Path(__file__).resolve().parent

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
)

JOB_QUEUE: "Queue[str]" = Queue()
JOB_LOCK = threading.Lock()
JOBS: dict[str, dict] = {}
JOB_HISTORY_FILE = Path(os.getenv("OZON_JOB_HISTORY_FILE", str(BASE_DIR / "job_history.jsonl")))
MAX_JOBS = int(os.getenv("OZON_MAX_JOBS", "50"))
JOB_TTL_SEC = int(os.getenv("OZON_JOB_TTL_SEC", "21600"))
DEFAULT_TS_ID = "ozon_tecno"
DEBUG_WEB = os.getenv("OZON_WEB_DEBUG", "1") == "1"

MARKETPLACES = [
    {"id": "ozon", "name": "OZON", "enabled": True},
    {"id": "wildberries", "name": "Wildberries", "enabled": True},
    {"id": "yandex_market", "name": "Я.Маркет", "enabled": True},
]



def serialize_result(result):
    return {
        "url": result.url,
        "ok": result.ok,
        "has_label": result.has_label,
        "seller_ok": result.seller_ok,
        "seller_name": result.seller_name,
        "label_text": result.label_text,
        "error": result.error,
    }


def normalize_rules(values) -> list[str]:
    if not values:
        return []
    if isinstance(values, str):
        values = [values]
    return [str(val).strip() for val in values if str(val).strip()]


def match_condition(
    condition: str,
    label_text: str,
    label_norm: str,
    allow_icon_fallback: bool = False,
) -> bool:
    condition_norm = normalize_text(condition)
    if not label_norm:
        return False
    cond_tokens = [t for t in condition_norm.split() if len(t) > 1]
    label_tokens = set(label_norm.split())
    if not cond_tokens:
        return False
    if "🎁" in condition and "🎁" not in (label_text or ""):
        if allow_icon_fallback:
            tokens = set(label_norm.split())
            if "подарок" in tokens:
                return False
            if "sim" in tokens and ("tecno" in tokens or "карта" in tokens):
                return True
        return False
    if "подарок" in cond_tokens and "подарок" not in label_tokens:
        return False
    return all(token in label_tokens for token in cond_tokens)


def rules_empty(rules: dict) -> bool:
    if not rules:
        return True
    return not normalize_rules(rules.get("error_conditions")) and not normalize_rules(
        rules.get("ok_conditions")
    )


def normalize_seller_filter(value: str) -> str:
    return normalize_text(value or "")


def seller_matches(filter_value: str, seller_name: str | None, seller_ok: bool | None) -> bool:
    if not filter_value:
        return True
    if not seller_name:
        return False
    parts = re.split(r"[,\n;]+", filter_value)
    values = [normalize_text(p.strip()) for p in parts if normalize_text(p.strip())]
    values = expand_seller_aliases(values)
    if not values:
        return True
    seller_norm = normalize_text(seller_name)
    return any(seller_norm == val for val in values)


def evaluate_result(result, rules: dict) -> tuple[str, str, dict]:
    label_text = result.label_text or ""
    label_norm = normalize_text(label_text)
    error_conditions = normalize_rules(rules.get("error_conditions"))
    ok_conditions = normalize_rules(rules.get("ok_conditions"))

    debug_info = {
        "label_text": label_text,
        "label_norm": label_norm,
        "error_conditions": error_conditions,
        "ok_conditions": ok_conditions,
        "matched_error": None,
        "matched_ok": None,
    }

    for condition in error_conditions:
        if match_condition(condition, label_text, label_norm, allow_icon_fallback=False):
            debug_info["matched_error"] = condition
            return "nok", f"Совпало с условием ошибки: {condition}", debug_info

    for condition in ok_conditions:
        condition_norm = normalize_text(condition)
        if condition_norm in ("без виджета", "нет виджета"):
            if not result.has_label:
                debug_info["matched_ok"] = condition
                return "ok", f"Совпало с условием OK: {condition}", debug_info
        if match_condition(condition, label_text, label_norm, allow_icon_fallback=True):
            debug_info["matched_ok"] = condition
            return "ok", f"Совпало с условием OK: {condition}", debug_info

    return "unknown", "Нет совпадений с условиями", debug_info


def persist_job(job: dict):
    try:
        JOB_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "id": job["id"],
            "status": job["status"],
            "created_at": job["created_at"],
            "started_at": job["started_at"],
            "finished_at": job["finished_at"],
            "total": job["total"],
            "results": job["results"],
        }
        with JOB_HISTORY_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


def prune_jobs():
    now = time.time()
    with JOB_LOCK:
        expired = [
            job_id
            for job_id, job in JOBS.items()
            if job.get("finished_at") and now - job["finished_at"] > JOB_TTL_SEC
        ]
        for job_id in expired:
            JOBS.pop(job_id, None)

        if len(JOBS) > MAX_JOBS:
            items = sorted(JOBS.values(), key=lambda j: j["created_at"])
            for job in items[: max(0, len(JOBS) - MAX_JOBS)]:
                JOBS.pop(job["id"], None)


def worker_loop():
    while True:
        job_id = JOB_QUEUE.get()
        with JOB_LOCK:
            job = JOBS.get(job_id)
            if not job:
                continue
            if job.get("cancelled"):
                job["status"] = "stopped"
                job["finished_at"] = time.time()
                persist_job(job)
                JOB_QUEUE.task_done()
                continue
            job["status"] = "running"
            job["started_at"] = time.time()

        def is_cancelled() -> bool:
            with JOB_LOCK:
                active = JOBS.get(job_id)
                return bool(active and active.get("cancelled"))

        def on_progress(urls: list[str]) -> None:
            with JOB_LOCK:
                active = JOBS.get(job_id)
                if not active:
                    return
                active["pending_urls"] = list(urls)
                active["collected_count"] = len(urls)
                active["total"] = len(urls)
                active["phase_count"] = len(urls)

        def on_search_raw(urls: list[str]) -> None:
            with JOB_LOCK:
                active = JOBS.get(job_id)
                if not active:
                    return
                active["search_urls"] = list(urls)
                active["search_total"] = len(urls)

        def on_seller_progress(checked: int, total: int, kept: int) -> None:
            with JOB_LOCK:
                active = JOBS.get(job_id)
                if not active:
                    return
                active["seller_checked"] = checked
                active["seller_total"] = total
                active["seller_kept"] = kept
                active["phase_count"] = kept

        def on_phase(phase: str) -> None:
            with JOB_LOCK:
                active = JOBS.get(job_id)
                if not active:
                    return
                active["phase"] = phase
                active["phase_count"] = 0
                active["phase_started_at"] = time.time()
                if phase == "search":
                    active["search_eta_sec"] = None
                if phase == "seller":
                    active["seller_checked"] = 0
                    active["seller_total"] = 0
                    active["seller_kept"] = 0

        def on_eta(phase: str, eta_sec: float) -> None:
            with JOB_LOCK:
                active = JOBS.get(job_id)
                if not active:
                    return
                if phase == "search":
                    active["search_eta_sec"] = eta_sec

        if job.get("auto_search"):
            with JOB_LOCK:
                if "tested_urls" not in job or not isinstance(job.get("tested_urls"), set):
                    job["tested_urls"] = set()
            query = job.get("search_query") or ""
            max_pages = job.get("search_max_pages") or 0
            seller_filter = job.get("seller_filter") or ""
            search_settings = job.get("search_settings") or {}

            def inline_test(driver, url):
                return check_current_page(driver, url)

            def on_inline_result(result: CheckResult) -> None:
                with JOB_LOCK:
                    active = JOBS.get(job_id)
                    if not active:
                        return
                    payload = serialize_result(result)
                    verdict, verdict_reason, debug_info = evaluate_result(
                        result, active.get("rules") or {}
                    )
                    payload["verdict"] = verdict
                    payload["verdict_reason"] = verdict_reason
                    if DEBUG_WEB:
                        payload["debug"] = debug_info
                    active["results"].append(payload)
                    active["done"] += 1
                    tested_urls = active.get("tested_urls")
                    if isinstance(tested_urls, set):
                        tested_urls.add(result.url)
                    if active.get("pending_urls") and result.url in active["pending_urls"]:
                        active["pending_urls"].remove(result.url)

            try:
                urls = collect_search_urls(
                    query,
                    seller_filter=seller_filter,
                    max_pages=max_pages,
                    scrolls=search_settings.get("scrolls"),
                    load_wait_sec=search_settings.get("load_wait_sec"),
                    scroll_wait_sec=search_settings.get("scroll_wait_sec"),
                    stable_hits=search_settings.get("stable_hits"),
                    stable_pause_sec=search_settings.get("stable_pause_sec"),
                    clean_profile=bool(search_settings.get("fresh_profile")),
                    progress_cb=on_progress,
                    raw_cb=on_search_raw,
                    seller_progress_cb=on_seller_progress,
                    eta_cb=on_eta,
                    match_test_cb=None if job.get("search_only") else inline_test,
                    match_result_cb=None if job.get("search_only") else on_inline_result,
                    phase_cb=on_phase,
                    cancel_check=is_cancelled,
                )
            except Exception as e:
                with JOB_LOCK:
                    job["status"] = "stopped"
                    job["finished_at"] = time.time()
                    job["error"] = f"Ошибка поиска: {e}"
                    persist_job(job)
                JOB_QUEUE.task_done()
                continue
            with JOB_LOCK:
                job["urls"] = urls
                job["total"] = len(urls)
                job["pending_urls"] = list(urls)
                job["collected_count"] = len(urls)
                job["search_done"] = True
                job["seller_filter_applied"] = bool(seller_filter)
                if not job.get("search_urls"):
                    job["search_urls"] = list(urls)
                    job["search_total"] = len(urls)
                if seller_filter:
                    job["seller_kept"] = len(urls)

        if job.get("search_only"):
            with JOB_LOCK:
                job["done"] = job.get("total", 0)
                job["status"] = "done"
                job["current_url"] = None
                job["finished_at"] = time.time()
                persist_job(job)
            JOB_QUEUE.task_done()
            continue

        with JOB_LOCK:
            job["phase"] = "testing"
            tested_urls = job.get("tested_urls")
            if isinstance(tested_urls, set) and len(tested_urls) >= len(job["urls"]):
                job["done"] = len(job["urls"])
                job["status"] = "done"
                job["current_url"] = None
                job["finished_at"] = time.time()
                persist_job(job)
                JOB_QUEUE.task_done()
                continue

        for url in job["urls"]:
            with JOB_LOCK:
                tested_urls = job.get("tested_urls")
                if isinstance(tested_urls, set) and url in tested_urls:
                    if job.get("pending_urls") and url in job["pending_urls"]:
                        job["pending_urls"].remove(url)
                    continue
            with JOB_LOCK:
                if job.get("cancelled"):
                    job["status"] = "stopped"
                    job["finished_at"] = time.time()
                    job["current_url"] = None
                    persist_job(job)
                    break
            with JOB_LOCK:
                job["current_url"] = url
            try:
                result = check_url(url)
            except Exception as e:
                with JOB_LOCK:
                    job["done"] += 1
                    if job.get("pending_urls") and url in job["pending_urls"]:
                        job["pending_urls"].remove(url)
                    job["results"].append(
                        {
                            "url": url,
                            "verdict": "error",
                            "verdict_reason": "Ошибка проверки карточки",
                            "ok": False,
                            "has_label": False,
                            "seller_ok": None,
                            "seller_name": None,
                            "label_text": "",
                            "error": str(e),
                        }
                    )
                continue
            if not job.get("seller_filter_applied"):
                seller_filter = job.get("seller_filter") or ""
                if not seller_matches(seller_filter, result.seller_name, result.seller_ok):
                    with JOB_LOCK:
                        job["done"] += 1
                        if job.get("pending_urls") and url in job["pending_urls"]:
                            job["pending_urls"].remove(url)
                    continue
            verdict, verdict_reason, debug_info = evaluate_result(result, job.get("rules") or {})
            with JOB_LOCK:
                payload = serialize_result(result)
                payload["verdict"] = verdict
                payload["verdict_reason"] = verdict_reason
                if DEBUG_WEB:
                    payload["debug"] = debug_info
                job["results"].append(payload)
                job["done"] += 1
                if job.get("pending_urls") and url in job["pending_urls"]:
                    job["pending_urls"].remove(url)
            if DEBUG_WEB:
                print(
                    f"[DEBUG] {url} verdict={verdict} reason={verdict_reason} "
                    f"label='{result.label_text}' ok_rules={debug_info.get('ok_conditions')} "
                    f"err_rules={debug_info.get('error_conditions')}"
                )
        with JOB_LOCK:
            if job.get("status") != "stopped":
                job["status"] = "done"
                job["current_url"] = None
                job["finished_at"] = time.time()
                persist_job(job)
        JOB_QUEUE.task_done()


worker_thread = threading.Thread(target=worker_loop, daemon=True)
worker_thread.start()


@app.route("/")
def index():
    ts_list = list_ts_configs()
    default_ts = get_ts_config(DEFAULT_TS_ID) or (ts_list[0] if ts_list else None)
    default_ts_id = default_ts["id"] if default_ts else ""
    presets = load_ts_presets(default_ts_id)
    seller_aliases = {}
    try:
        aliases_path = BASE_DIR / "data" / "seller_aliases.json"
        if aliases_path.exists():
            seller_aliases = json.loads(aliases_path.read_text(encoding="utf-8") or "{}")
    except Exception:
        seller_aliases = {}
    page_config = {
        "marketplaces": MARKETPLACES,
        "ts_list": ts_list,
        "default_ts_id": default_ts_id,
        "presets": presets,
        "seller_aliases": seller_aliases,
    }
    return render_template("index.html", page_config=json.dumps(page_config, ensure_ascii=False))


@app.route("/api/presets/<ts_id>", methods=["GET"])
def ts_presets(ts_id: str):
    presets = load_ts_presets(ts_id)
    return jsonify({"ok": True, "presets": presets})


@app.route("/check", methods=["POST"])
def check():
    payload = request.get_json(silent=True) or {}
    url = (payload.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "URL не указан."}), 400
    if "ozon.ru/product/" not in url:
        return jsonify({"ok": False, "error": "Нужна ссылка на карточку Ozon (/product/...)."}), 400

    result = check_url(url)
    rules = payload.get("rules") or {}
    verdict, verdict_reason, debug_info = evaluate_result(result, rules)
    return jsonify(
        {
            "ok": result.ok,
            "url": result.url,
            "has_label": result.has_label,
            "seller_ok": result.seller_ok,
            "seller_name": result.seller_name,
            "label_text": result.label_text,
            "error": result.error,
            "verdict": verdict,
            "verdict_reason": verdict_reason,
            "debug": debug_info if DEBUG_WEB else None,
        }
    )


def normalize_urls(raw: str) -> list[str]:
    urls = []
    seen = set()
    for line in raw.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        urls.append(candidate)
    return urls


@app.route("/batch", methods=["POST"])
def batch():
    payload = request.get_json(silent=True) or {}
    raw = payload.get("urls") or ""
    rules = payload.get("rules") or {}
    meta = payload.get("meta") or {}
    urls = normalize_urls(raw)
    if not urls:
        return jsonify({"ok": False, "error": "Список ссылок пуст."}), 400

    invalid = [u for u in urls if "ozon.ru/product/" not in u]
    if invalid:
        return jsonify(
            {
                "ok": False,
                "error": "В списке есть ссылки не на карточку Ozon.",
                "invalid": invalid,
            }
        ), 400

    prune_jobs()
    job_id = uuid.uuid4().hex
        job = {
        "id": job_id,
        "status": "queued",
        "created_at": time.time(),
        "started_at": None,
        "finished_at": None,
        "total": len(urls),
        "done": 0,
        "current_url": None,
        "urls": urls,
        "pending_urls": list(urls),
        "collected_count": len(urls),
        "results": [],
        "rules": rules,
        "meta": meta,
        "cancelled": False,
        "search_done": True,
        "seller_filter_applied": False,
        "search_urls": list(urls),
        "search_total": len(urls),
        "seller_kept": len(urls),
        "seller_checked": 0,
        "seller_total": 0,
            "search_eta_sec": None,
            "phase_started_at": None,
            "tested_urls": set(),
        }

    with JOB_LOCK:
        JOBS[job_id] = job

    JOB_QUEUE.put(job_id)
    return jsonify({"ok": True, "job_id": job_id, "total": len(urls)})


@app.route("/auto-batch", methods=["POST"])
def auto_batch():
    payload = request.get_json(silent=True) or {}
    search_query = (payload.get("search") or "").strip()
    if not search_query:
        return jsonify({"ok": False, "error": "Поиск не указан."}), 400
    rules = payload.get("rules") or {}
    meta = payload.get("meta") or {}
    seller_filter = (payload.get("seller") or "").strip()
    search_settings = payload.get("search_settings") or {}

    prune_jobs()
    job_id = uuid.uuid4().hex
        job = {
        "id": job_id,
        "status": "queued",
        "created_at": time.time(),
        "started_at": None,
        "finished_at": None,
        "total": 0,
        "done": 0,
        "current_url": None,
        "urls": [],
        "pending_urls": [],
        "collected_count": 0,
        "results": [],
        "rules": rules,
        "meta": meta,
        "auto_search": True,
        "search_query": search_query,
        "seller_filter": seller_filter,
        "search_settings": search_settings,
        "cancelled": False,
        "search_done": False,
        "seller_filter_applied": False,
        "search_only": False,
        "phase": None,
        "phase_count": 0,
        "search_urls": [],
        "search_total": 0,
        "seller_kept": 0,
        "seller_checked": 0,
        "seller_total": 0,
            "search_eta_sec": None,
            "phase_started_at": None,
            "tested_urls": set(),
        }

    with JOB_LOCK:
        JOBS[job_id] = job

    JOB_QUEUE.put(job_id)
    return jsonify({"ok": True, "job_id": job_id, "total": 0})


@app.route("/search-only", methods=["POST"])
def search_only():
    payload = request.get_json(silent=True) or {}
    search_query = (payload.get("search") or "").strip()
    if not search_query:
        return jsonify({"ok": False, "error": "Поиск не указан."}), 400
    meta = payload.get("meta") or {}
    seller_filter = (payload.get("seller") or "").strip()
    search_settings = payload.get("search_settings") or {}

    prune_jobs()
    job_id = uuid.uuid4().hex
        job = {
        "id": job_id,
        "status": "queued",
        "created_at": time.time(),
        "started_at": None,
        "finished_at": None,
        "total": 0,
        "done": 0,
        "current_url": None,
        "urls": [],
        "pending_urls": [],
        "collected_count": 0,
        "results": [],
        "rules": {},
        "meta": meta,
        "auto_search": True,
        "search_query": search_query,
        "seller_filter": seller_filter,
        "search_settings": search_settings,
        "cancelled": False,
        "search_done": False,
        "search_only": True,
        "seller_filter_applied": False,
        "phase": None,
        "phase_count": 0,
        "search_urls": [],
        "search_total": 0,
        "seller_kept": 0,
        "seller_checked": 0,
        "seller_total": 0,
            "search_eta_sec": None,
            "phase_started_at": None,
            "tested_urls": set(),
        }

    with JOB_LOCK:
        JOBS[job_id] = job

    JOB_QUEUE.put(job_id)
    return jsonify({"ok": True, "job_id": job_id, "total": 0})


@app.route("/jobs", methods=["GET"])
def jobs():
    prune_jobs()
    with JOB_LOCK:
        items = [
            {
                "id": job["id"],
                "status": job["status"],
                "total": job["total"],
                "done": job["done"],
                "created_at": job["created_at"],
            }
            for job in JOBS.values()
        ]
    items.sort(key=lambda x: x["created_at"], reverse=True)
    return jsonify({"ok": True, "jobs": items[:20]})


@app.route("/jobs/<job_id>", methods=["GET"])
def job_status(job_id: str):
    prune_jobs()
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Задача не найдена."}), 404
        payload = {
            "ok": True,
            "id": job["id"],
            "status": job["status"],
            "total": job["total"],
            "done": job["done"],
            "current_url": job.get("current_url"),
            "pending_urls": job.get("pending_urls") or [],
            "collected_count": job.get("collected_count"),
            "search_done": job.get("search_done", False),
            "search_only": job.get("search_only", False),
            "started_at": job.get("started_at"),
            "phase": job.get("phase"),
            "phase_count": job.get("phase_count"),
            "search_total": job.get("search_total"),
            "seller_kept": job.get("seller_kept"),
            "search_eta_sec": job.get("search_eta_sec"),
            "seller_checked": job.get("seller_checked"),
            "seller_total": job.get("seller_total"),
            "phase_started_at": job.get("phase_started_at"),
            "error": job.get("error"),
            "results": job["results"],
        }
    return jsonify(payload)


@app.route("/jobs/<job_id>/stop", methods=["POST"])
def job_stop(job_id: str):
    prune_jobs()
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Задача не найдена."}), 404
        job["cancelled"] = True
        job["status"] = "stopped"
        job["current_url"] = None
        job["finished_at"] = time.time()
        persist_job(job)
    return jsonify({"ok": True})


@app.route("/jobs/<job_id>/csv", methods=["GET"])
def job_csv(job_id: str):
    prune_jobs()
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Задача не найдена."}), 404
        results = list(job["results"])

    output = []
    header = [
        "url",
        "verdict",
        "verdict_reason",
        "ok",
        "has_label",
        "seller_ok",
        "seller_name",
        "label_text",
        "error",
    ]
    output.append(header)
    rows = results
    if not rows:
        for url in job.get("pending_urls") or []:
            rows.append({"url": url, "verdict": "pending"})
    for item in rows:
        output.append(
            [
                item.get("url", ""),
                item.get("verdict", ""),
                item.get("verdict_reason", ""),
                item.get("ok", False),
                item.get("has_label", False),
                item.get("seller_ok", None),
                item.get("seller_name", ""),
                item.get("label_text", ""),
                item.get("error", ""),
            ]
        )

    csv_lines = []
    for row in output:
        csv_lines.append(
            ",".join(
                [
                    '"{}"'.format(str(val).replace('"', '""'))
                    for val in row
                ]
            )
        )

    payload = "\n".join(csv_lines)
    return Response(
        payload,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=ozon_job_{job_id}.csv"},
    )


@app.route("/jobs/<job_id>/xlsx", methods=["GET"])
def job_xlsx(job_id: str):
    prune_jobs()
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Задача не найдена."}), 404
        results = list(job["results"])

    verdict_filter = request.args.get("verdict", "")
    verdicts = [v.strip() for v in verdict_filter.split(",") if v.strip()] if verdict_filter else []
    if verdicts:
        results = [item for item in results if (item.get("verdict") or "unknown") in verdicts]

    wb = Workbook()
    ws = wb.active
    ws.title = "Results"
    headers = [
        "url",
        "verdict",
        "verdict_reason",
        "ok",
        "has_label",
        "seller_ok",
        "seller_name",
        "label_text",
        "error",
    ]
    ws.append(headers)
    rows = results
    if not rows:
        for url in job.get("pending_urls") or []:
            rows.append({"url": url, "verdict": "pending"})
    for item in rows:
        ws.append(
            [
                item.get("url", ""),
                item.get("verdict", ""),
                item.get("verdict_reason", ""),
                item.get("ok", False),
                item.get("has_label", False),
                item.get("seller_ok", None),
                item.get("seller_name", ""),
                item.get("label_text", ""),
                item.get("error", ""),
            ]
        )

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    ts = time.localtime()
    suffix = f"{ts.tm_min:02d}{ts.tm_hour:02d}{ts.tm_mday:02d}{ts.tm_mon:02d}{ts.tm_year}"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"ozon_job_{suffix}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/jobs/<job_id>/search-xlsx", methods=["GET"])
def job_search_xlsx(job_id: str):
    prune_jobs()
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Задача не найдена."}), 404
        search_urls = list(job.get("search_urls") or [])
    wb = Workbook()
    ws = wb.active
    ws.title = "Search"
    ws.append(["url"])
    for url in search_urls:
        ws.append([url])
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    ts = time.localtime()
    suffix = f"{ts.tm_min:02d}{ts.tm_hour:02d}{ts.tm_mday:02d}{ts.tm_mon:02d}{ts.tm_year}"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"ozon_search_{suffix}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/jobs/<job_id>/search-csv", methods=["GET"])
def job_search_csv(job_id: str):
    prune_jobs()
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Задача не найдена."}), 404
        search_urls = list(job.get("search_urls") or [])
    output = ["url"]
    output += [str(url) for url in search_urls]
    payload = "\n".join(output)
    return Response(
        payload,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=ozon_search_{job_id}.csv"},
    )


if __name__ == "__main__":
    host = os.getenv("OZON_WEB_HOST", "0.0.0.0")
    port = int(os.getenv("OZON_WEB_PORT", "8000"))
    app.run(host=host, port=port, debug=False)
