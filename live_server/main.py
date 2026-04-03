"""
BulletTrade live web server.

Start with:
    uvicorn live_server.main:app --reload --reload-exclude live_runs --port 8000
"""

import asyncio
import json
import os
import sys
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from typing import Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from web_server.runner import TaskStatus, get_task, run_backtest

from .live_runner import (
    LiveStatus,
    get_live_state,
    get_live_task,
    get_live_task_view,
    list_live_tasks,
    start_live,
    start_live_test,
    stop_live,
    restart_live,
    delete_live,
)
from .result_parser import parse_result

app = FastAPI(title="BulletTrade Live Web")

BASE_DIR = Path(__file__).resolve().parent.parent
STRATEGY_DIR = BASE_DIR / "strategy"
STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ── 日志等待文件出现的最大轮询次数 ──────────────────────────────────────────
_LOG_WAIT_RETRIES = 40          # 等待日志文件出现，最多 40 × 0.25s = 10s
_LOG_POLL_INTERVAL = 0.25       # 秒，缩短轮询间隔让日志更实时


class RunRequest(BaseModel):
    code: str
    start: str = "2024-01-01"
    end: str = "2024-06-30"
    cash: int = 100_000
    benchmark: str = "000300.XSHG"
    frequency: str = "day"


class LiveRequest(BaseModel):
    code: str
    broker: str = "simulator"
    name: str = ""


# ── 路径校验辅助 ─────────────────────────────────────────────────────────────

def _resolve_strategy_path(filename: str) -> Path:
    """解析并校验策略文件路径，防止路径穿越攻击。"""
    path = Path(filename)
    if path.name != filename or path.suffix.lower() != ".py":
        raise HTTPException(status_code=400, detail="invalid strategy filename")

    strategy_root = STRATEGY_DIR.resolve()
    strategy_path = (strategy_root / path.name).resolve()
    try:
        strategy_path.relative_to(strategy_root)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid strategy filename")

    if not strategy_path.is_file():
        raise HTTPException(status_code=404, detail="strategy file not found")
    return strategy_path


def _sse(event: str, data: dict, event_id: Optional[int] = None) -> str:
    """构造 SSE 帧字符串。"""
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append(f"data: {json.dumps(data, ensure_ascii=False)}")
    return "\n".join(lines) + "\n\n"


# ── 静态页面 ── ✅ 已修复样式加载问题 ────────────────────────────────────────

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")

@app.get("/live")
async def live_page():
    return FileResponse(STATIC_DIR / "live.html")

@app.get("/live/task")
async def live_task_page():
    return FileResponse(STATIC_DIR / "live_task.html")


# ── 策略文件 API ─────────────────────────────────────────────────────────────

@app.get("/api/strategies")
async def api_strategies():
    if not STRATEGY_DIR.exists():
        return {"files": []}
    files = sorted(p.name for p in STRATEGY_DIR.iterdir() if p.is_file() and p.suffix == ".py")
    return {"files": files}


@app.get("/api/strategy/{filename:path}")
async def api_strategy(filename: str):
    strategy_path = _resolve_strategy_path(filename)   # 抛 HTTPException → FastAPI 自动处理
    code = strategy_path.read_text(encoding="utf-8", errors="replace")
    return {"filename": strategy_path.name, "name": strategy_path.stem, "code": code}


# ── 回测 API ─────────────────────────────────────────────────────────────────

@app.post("/api/run")
async def api_run(req: RunRequest):
    task_id = await run_backtest(
        code=req.code,
        start=req.start,
        end=req.end,
        cash=req.cash,
        benchmark=req.benchmark,
        frequency=req.frequency,
    )
    return {"task_id": task_id}


@app.get("/api/status/{task_id}")
async def api_status(task_id: str):
    task = get_task(task_id)
    if not task:
        return {"status": "not_found"}
    return {"status": task["status"], "returncode": task.get("returncode")}


@app.get("/api/result/{task_id}")
async def api_result(task_id: str):
    task = get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    if task["status"] != TaskStatus.DONE:
        raise HTTPException(status_code=409, detail=f"task status: {task['status']}")
    return parse_result(task["output_dir"])


@app.get("/api/report/{task_id}")
async def api_report(task_id: str):
    task = get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    report_path = Path(task["output_dir"]) / "report.html"
    if not report_path.exists():
        raise HTTPException(status_code=404, detail="report not generated")
    return FileResponse(report_path)


# ── 回测日志 WebSocket ────────────────────────────────────────────────────────

@app.websocket("/ws/log/{task_id}")
async def ws_log(websocket: WebSocket, task_id: str):
    await websocket.accept()
    task = get_task(task_id)
    if not task:
        await websocket.send_text("[ERROR] task not found")
        await websocket.close()
        return

    log_path = Path(task["output_dir"]) / "backtest.log"
    for _ in range(_LOG_WAIT_RETRIES):
        if log_path.exists():
            break
        await asyncio.sleep(_LOG_POLL_INTERVAL)

    try:
        with log_path.open("r", encoding="utf-8", errors="replace") as f:
            while True:
                line = f.readline()
                if line:
                    await websocket.send_text(line.rstrip())
                    continue

                current = get_task(task_id)
                if current and current["status"] in (TaskStatus.DONE, TaskStatus.ERROR):
                    for item in f.read().splitlines():
                        if item:
                            await websocket.send_text(item)
                    await websocket.send_text("__DONE__")
                    break
                await asyncio.sleep(0.3)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try:
            await websocket.send_text(f"[LOG_READ_ERROR] {exc}")
        except Exception:
            pass


# ── 实盘 API ─────────────────────────────────────────────────────────────────

@app.post("/api/live/start")
async def api_live_start(req: LiveRequest):
    loop = asyncio.get_event_loop()
    task_id = await loop.run_in_executor(
        None,
        lambda: start_live(code=req.code, broker=req.broker, name=req.name),
    )
    return {"task_id": task_id, "broker": req.broker, "task": get_live_task_view(task_id)}


@app.post("/api/live/test")
async def api_live_test():
    loop = asyncio.get_event_loop()
    task_id = await loop.run_in_executor(None, start_live_test)
    return {"task_id": task_id, "broker": "simulator", "task": get_live_task_view(task_id)}


@app.post("/api/live/stop/{task_id}")
async def api_live_stop(task_id: str):
    loop = asyncio.get_event_loop()
    ok = await loop.run_in_executor(None, lambda: stop_live(task_id))
    if not ok:
        raise HTTPException(status_code=404, detail="task not found or already stopped")
    return {"ok": True, "task_id": task_id, "task": get_live_task_view(task_id)}


@app.post("/api/live/restart/{task_id}")
async def api_live_restart(task_id: str):
    loop = asyncio.get_event_loop()
    task = get_live_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    if task["status"] in (LiveStatus.RUNNING, LiveStatus.STARTING):
        raise HTTPException(status_code=409, detail="task already running")

    new_task_id = await loop.run_in_executor(None, lambda: restart_live(task_id))
    if not new_task_id:
        raise HTTPException(status_code=500, detail="failed to restart task")
    return {"task_id": new_task_id, "task": get_live_task_view(new_task_id)}


@app.post("/api/live/delete/{task_id}")
async def api_live_delete(task_id: str):
    loop = asyncio.get_event_loop()
    ok = await loop.run_in_executor(None, lambda: delete_live(task_id))
    if not ok:
        raise HTTPException(status_code=404, detail="task not found")
    return {"ok": True, "task_id": task_id, "task": get_live_task_view(task_id)}


@app.get("/api/live/status/{task_id}")
async def api_live_status(task_id: str):
    task = get_live_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    return {
        "task_id": task_id,
        "name": task.get("name", task_id),
        "status": task["status"],
        "broker": task["broker"],
        "started_at": task["started_at"],
        "stopped_at": task.get("stopped_at"),
        "pid": task.get("pid"),
        "returncode": task.get("returncode"),
        "live_state": get_live_state(task_id),
    }


@app.get("/api/live/list")
async def api_live_list():
    return {"tasks": list_live_tasks()}


def _find_env_file() -> Optional[Path]:
    candidates = [
        Path(__file__).resolve().parent.parent / '.env',
        Path(__file__).resolve().parent.parent / 'env.live.example',
        Path(__file__).resolve().parent.parent / 'env.example',
    ]
    for p in candidates:
        if p.exists():
            return p

    current = Path.cwd()
    for _ in range(8):
        c = current / '.env'
        if c.exists():
            return c
        if current == current.parent:
            break
        current = current.parent

    return None


def _parse_env_file(env_path: Path) -> list[dict]:
    entries = []
    if not env_path.exists():
        return entries

    for line in env_path.read_text(encoding='utf-8').splitlines():
        raw = line.rstrip('\n')
        stripped = raw.strip()
        if not stripped:
            continue

        is_commented = stripped.startswith('#')
        content = stripped[1:].strip() if is_commented else stripped
        if '=' not in content:
            continue

        key, val = content.split('=', 1)
        key = key.strip()
        if not key:
            continue

        default = val.strip()
        env_value = os.environ.get(key, default)

        entries.append({
            'key': key,
            'value': env_value,
            'default': default,
            'enabled': not is_commented,
        })
    return entries


@app.get("/api/live/config")
async def api_live_config():
    env_path = _find_env_file()
    if not env_path:
        return JSONResponse({"entries": []})
    entries = _parse_env_file(env_path)
    return JSONResponse({"entries": entries})


@app.post("/api/live/config")
async def api_live_config_save(data: dict):
    env_path = _find_env_file()
    if not env_path:
        raise HTTPException(status_code=404, detail=".env file not found")

    values = data.get('values', {})
    if not isinstance(values, dict):
        raise HTTPException(status_code=400, detail='invalid payload')

    lines = []
    existing_keys = set()
    for line in env_path.read_text(encoding='utf-8').splitlines():
        raw = line.rstrip('\n')
        stripped = raw.strip()
        if not stripped or '=' not in stripped:
            lines.append(raw)
            continue

        is_commented = stripped.startswith('#')
        content = stripped[1:].strip() if is_commented else stripped
        if '=' not in content:
            lines.append(raw)
            continue

        key, old_val = content.split('=', 1)
        key = key.strip()
        if key in values:
            entry = values.get(key, {})
            val = str(entry.get('value', '')).strip()
            enabled = bool(entry.get('enabled', True))
            new_line = f"{key}={val}" if enabled else f"#{key}={val}"
            lines.append(new_line)
            existing_keys.add(key)
        else:
            lines.append(raw)

    for key, entry in values.items():
        if key not in existing_keys:
            val = str(entry.get('value', '')).strip()
            enabled = bool(entry.get('enabled', True))
            lines.append(f"{key}={val}" if enabled else f"#{key}={val}")

    env_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    for key, entry in values.items():
        if not isinstance(entry, dict):
            continue
        os.environ[key] = str(entry.get('value', ''))

    return JSONResponse({'ok': True})


# ── 实盘 SSE 日志流 ───────────────────────────────────────────────────────────

@app.get("/api/stream/live/{task_id}")
async def sse_live(task_id: str, request: Request):
    task = get_live_task(task_id)
    if not task:
        async def _err():
            yield _sse("stream-error", {"msg": "task not found"})
        return StreamingResponse(_err(), media_type="text/event-stream")

    log_file = Path(task["log_file"])

    try:
        resume_offset = max(int(request.headers.get("last-event-id", "0")), 0)
    except (TypeError, ValueError):
        resume_offset = 0

    async def _generate():
        # 等待日志文件出现
        for _ in range(_LOG_WAIT_RETRIES):
            if log_file.exists():
                break
            if await request.is_disconnected():
                return
            yield ": ping\n\n"
            await asyncio.sleep(_LOG_POLL_INTERVAL)

        status_tick = 0
        ping_tick = 0
        try:
            with log_file.open("r", encoding="utf-8", errors="replace") as f:
                if resume_offset:
                    try:
                        f.seek(resume_offset)
                    except (OSError, ValueError):
                        f.seek(0)

                last_offset = f.tell()

                while True:
                    # 批量读取当前可用行
                    while line := f.readline():
                        last_offset = f.tell()
                        if line.strip():
                            yield _sse("log", {"text": line.rstrip()}, event_id=last_offset)

                    # 定期推送状态
                    status_tick += 1
                    if status_tick >= 17:
                        status_tick = 0
                        current = get_live_task(task_id)
                        if current:
                            yield _sse(
                                "status",
                                {
                                    "status": current["status"],
                                    "pid": current.get("pid"),
                                    "started_at": current.get("started_at"),
                                },
                                event_id=last_offset,
                            )

                    # 心跳保活
                    ping_tick += 1
                    if ping_tick >= 10:
                        ping_tick = 0
                        yield ": ping\n\n"

                    if await request.is_disconnected():
                        return

                    # 任务已终止 → 刷尾行后退出
                    current = get_live_task(task_id)
                    if current and current["status"] in (LiveStatus.STOPPED, LiveStatus.ERROR):
                        while line := f.readline():
                            last_offset = f.tell()
                            if line.strip():
                                yield _sse("log", {"text": line.rstrip()}, event_id=last_offset)
                        yield _sse(
                            "stopped",
                            {
                                "status": current["status"],
                                "returncode": current.get("returncode"),
                            },
                            event_id=f.tell(),
                        )
                        return

                    await asyncio.sleep(0.15)

        except Exception as exc:
            yield _sse("stream-error", {"msg": str(exc)}, event_id=last_offset)

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )