"""
runner.py - live trading task management for the web UI.
"""

import asyncio
import json
import logging
import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

_log = logging.getLogger(__name__)


class TaskStatus:
    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"


_tasks: Dict[str, dict] = {}

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_ROOT = _PROJECT_ROOT / "live_results"
TASK_WORK_ROOT = Path(tempfile.gettempdir()) / "bullet_trade_live_tasks"
LIVE_TASK_STATE = "live_task_state.json"


def _persist_state(output_dir: Path, status: str) -> None:
    try:
        p = output_dir / LIVE_TASK_STATE
        p.write_text(
            json.dumps({"status": status, "updated_at": datetime.now().isoformat()}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError as exc:
        _log.warning("Failed to write %s: %s", output_dir / LIVE_TASK_STATE, exc)


def _append_log(output_dir: Path, message: str) -> None:
    try:
        with open(output_dir / "live.log", "a", encoding="utf-8", errors="replace") as fp:
            fp.write(message.rstrip() + "\n")
    except OSError:
        pass


def _format_log_message(message: str, level: str = "INFO") -> str:
    text = str(message).strip()
    if not text:
        return ""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"{ts} - {level} - {text}"


def _emit_task_event(task_id: str, event: Dict[str, Any]) -> None:
    task = _tasks.get(task_id)
    if not task:
        return

    backlog = task.setdefault("event_backlog", [])
    backlog.append(event)
    if len(backlog) > 1000:
        del backlog[:len(backlog) - 1000]

    loop = task.get("loop")
    listeners = list(task.get("listeners", []))
    if not loop:
        return

    def _publish() -> None:
        alive = []
        for queue in listeners:
            try:
                queue.put_nowait(event)
                alive.append(queue)
            except Exception:
                continue
        task["listeners"] = alive

    loop.call_soon_threadsafe(_publish)


def get_status_for_api(task_id: str) -> Dict[str, Any]:
    t = _tasks.get(task_id)
    if t:
        return {"status": t["status"], "broker": t.get("broker", "simulator")}

    output_dir = (RESULTS_ROOT / task_id).resolve()
    if not output_dir.is_dir():
        return {"status": "not_found", "broker": None}

    state_path = output_dir / LIVE_TASK_STATE
    if state_path.exists():
        try:
            data = json.loads(state_path.read_text(encoding="utf-8"))
            return {"status": data["status"], "broker": data.get("broker")}
        except (json.JSONDecodeError, OSError):
            pass

    return {"status": TaskStatus.RUNNING, "broker": None}


def get_task(task_id: str) -> Optional[dict]:
    return _tasks.get(task_id)


def list_tasks() -> List[Dict[str, Any]]:
    return [
        {"task_id": task_id, "status": task["status"], "broker": task.get("broker", "simulator")}
        for task_id, task in _tasks.items()
    ]


def subscribe_task_events(task_id: str) -> tuple[Optional[list[Dict[str, Any]]], Optional[asyncio.Queue]]:
    task = _tasks.get(task_id)
    if not task:
        return None, None
    queue: asyncio.Queue = asyncio.Queue()
    task.setdefault("listeners", []).append(queue)
    return list(task.get("event_backlog", [])), queue


def unsubscribe_task_events(task_id: str, queue: asyncio.Queue) -> None:
    task = _tasks.get(task_id)
    if not task:
        return
    task["listeners"] = [item for item in task.get("listeners", []) if item is not queue]


def stop_task(task_id: str) -> Dict[str, Any]:
    task = _tasks.get(task_id)
    if not task:
        return {"ok": False, "status": "not_found"}

    status = task.get("status")
    if status in (TaskStatus.STOPPED, TaskStatus.ERROR):
        return {"ok": False, "status": status}

    if task.get("stop_requested"):
        return {"ok": True, "status": "stopping"}

    task["stop_requested"] = True
    message = "[WARN] 已收到停止请求，正在停止实盘交易"
    _append_log(Path(task["output_dir"]).resolve(), message)
    _emit_task_event(task_id, {"type": "log", "message": message})
    return {"ok": True, "status": "stopping"}


async def start_live_trading(
    code: str,
    broker: str,
    cash: int,
) -> str:
    task_id = uuid.uuid4().hex[:12]
    output_dir = (RESULTS_ROOT / task_id).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    task_work_dir = (TASK_WORK_ROOT / task_id).resolve()
    task_work_dir.mkdir(parents=True, exist_ok=True)

    strategy_path = task_work_dir / "strategy.py"
    coding_header = "# -*- coding: utf-8 -*-\n"
    if not code.lstrip().startswith("# -*- coding"):
        code = coding_header + code
    strategy_path.write_text(code, encoding="utf-8")
    (output_dir / "strategy.txt").write_text(code, encoding="utf-8")
    (output_dir / "live.log").write_text(
        "[INFO] 任务已创建，准备启动实盘交易...\n"
        f"[INFO] 输出目录: {output_dir}\n"
        f"[INFO] Broker: {broker}\n",
        encoding="utf-8",
    )

    loop = asyncio.get_running_loop()
    _tasks[task_id] = {
        "status": TaskStatus.PENDING,
        "output_dir": str(output_dir),
        "strategy_path": str(strategy_path),
        "broker": broker,
        "stop_requested": False,
        "listeners": [],
        "event_backlog": [],
        "loop": loop,
        "async_task": None,
    }
    _persist_state(output_dir, TaskStatus.PENDING)

    coro = _run(task_id, strategy_path, output_dir, broker, cash)
    task = asyncio.create_task(coro)
    _tasks[task_id]["async_task"] = task
    return task_id


async def _run(task_id, strategy_path, output_dir, broker, cash):
    output_dir = Path(output_dir).resolve()
    strategy_path = Path(strategy_path).resolve()

    task = _tasks[task_id]
    task["status"] = TaskStatus.RUNNING
    _persist_state(output_dir, TaskStatus.RUNNING)

    def _push_log(message: str) -> None:
        text = _format_log_message(message)
        if not text:
            return
        _append_log(output_dir, text)
        _emit_task_event(task_id, {"type": "log", "message": text})

    def _stop_checker() -> bool:
        current = _tasks.get(task_id)
        return bool(current and current.get("stop_requested"))

    try:
        _push_log(f"[INFO] 启动实盘交易: broker={broker}, cash={cash}")
        _push_log(f"[INFO] 策略文件: {strategy_path}")

        # TODO: 实际启动 bullet-trade live 进程
        _push_log("[INFO] 实盘交易已启动（模拟模式）")

        # 模拟运行
        while not _stop_checker():
            await asyncio.sleep(1)

        _push_log("[INFO] 实盘交易已停止")
        task["status"] = TaskStatus.STOPPED

    except Exception as exc:
        _push_log(f"[ERROR] 启动实盘交易失败: {type(exc).__name__}: {exc}")
        task["status"] = TaskStatus.ERROR

    _persist_state(output_dir, task["status"])
    _emit_task_event(task_id, {"type": "done", "status": task["status"]})
