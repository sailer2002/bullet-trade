"""
Process management for live trading tasks.
"""

import json
import logging
import os
import re as _re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

_log = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
LIVE_ROOT = _PROJECT_ROOT / "live_runs"

_live_tasks: Dict[str, dict] = {}
_persisted_loaded = False
_TEST_SYMBOL = "000001.XSHE"
_TEST_STRATEGY_CODE = f"""from jqdata import *


def initialize(context):
    g.test_signal_logged = False
    g.handle_data_logged = False
    subscribe("{_TEST_SYMBOL}", "tick")
    log.info("[TEST] initialize completed")
    log.info("[TEST] subscribed tick: {_TEST_SYMBOL}")


def before_trading_start(context):
    log.info("[TEST] before_trading_start triggered")


def handle_data(context, data):
    if g.handle_data_logged:
        return
    g.handle_data_logged = True
    log.info(f"[TEST] handle_data triggered at {{context.current_dt}}")


def handle_tick(context, tick):
    if g.test_signal_logged:
        return
    g.test_signal_logged = True
    sid = tick.get("sid") or tick.get("security") or "unknown"
    price = tick.get("last_price")
    log.info(f"[TEST] tick received: sid={{sid}} price={{price}}")
    log.info("[TEST] signal fired: off-market live probe is working")
    unsubscribe_all()
    log.info("[TEST] tick probe finished; you can stop this task after checking the log")
"""


class LiveStatus:
    STARTING = "starting"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"


def _utf8_env(extra_env: Optional[Dict[str, str]] = None) -> dict:
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONLEGACYWINDOWSSTDIO"] = "0"
    # 关键：强制子进程 stdout/stderr 无缓冲，确保 print() 实时可见。
    # 不设置此项时，通过 PIPE 捕获的 stdout 会变为全缓冲（8KB），
    # 导致 print() 内容卡住直到缓冲区满或进程退出才能刷出。
    env["PYTHONUNBUFFERED"] = "1"
    if extra_env:
        env.update({key: str(value) for key, value in extra_env.items()})
    return env


def _find_exe() -> List[str]:
    exe = shutil.which("bullet-trade")
    if exe:
        return [exe]
    return [sys.executable, "-X", "utf8", "-m", "bullet_trade"]


def _popen_kwargs() -> dict:
    kwargs = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        kwargs["stdin"] = subprocess.DEVNULL
    return kwargs


def _build_test_market_periods(now: Optional[datetime] = None) -> str:
    current = now or datetime.now()
    start_dt = current.replace(second=0, microsecond=0) - timedelta(minutes=1)
    end_dt = current.replace(second=0, microsecond=0) + timedelta(minutes=3)
    day_start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = current.replace(hour=23, minute=59, second=0, microsecond=0)
    if start_dt < day_start:
        start_dt = day_start
    if end_dt <= start_dt:
        end_dt = min(day_end, start_dt + timedelta(minutes=1))
    elif end_dt > day_end:
        end_dt = day_end
    return f"{start_dt:%H:%M}-{end_dt:%H:%M}"


def _build_test_env(name: str) -> Dict[str, str]:
    return {
        "STRATEGY_NAME": name,
        "SCHEDULER_MARKET_PERIODS": _build_test_market_periods(),
        "CALENDAR_SKIP_WEEKEND": "false",
        "CALENDAR_RETRY_MINUTES": "1",
        "TICK_SYNC_INTERVAL": "1",
        "ACCOUNT_SYNC_INTERVAL": "5",
        "ORDER_SYNC_INTERVAL": "5",
    }


def _write_strategy(output_dir: Path, code: str) -> Path:
    path = output_dir / "strategy.py"
    header = "# -*- coding: utf-8 -*-\n"
    if not code.lstrip().startswith("# -*- coding"):
        code = header + code
    path.write_text(code, encoding="utf-8")
    return path


def _persist(task_id: str) -> None:
    task = _live_tasks.get(task_id)
    if not task:
        return

    output_dir = Path(task["output_dir"])
    meta = {
        "task_id": task_id,
        "name": task.get("name", task_id),
        "status": task["status"],
        "broker": task["broker"],
        "started_at": task["started_at"],
        "stopped_at": task.get("stopped_at"),
        "pid": task.get("pid"),
        "returncode": task.get("returncode"),
    }
    (output_dir / "live_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _monitor(task_id: str, proc: subprocess.Popen) -> None:
    proc.wait()
    task = _live_tasks.get(task_id)
    if not task:
        return

    if task["status"] in (LiveStatus.RUNNING, LiveStatus.STARTING):
        task["status"] = (
            LiveStatus.ERROR
            if proc.returncode not in (0, -15, -2)
            else LiveStatus.STOPPED
        )
    task["stopped_at"] = datetime.now().isoformat(timespec="seconds")
    task["returncode"] = proc.returncode
    _persist(task_id)
    _log.info("live task exited: task=%s returncode=%s", task_id, proc.returncode)


def _load_task_from_meta(meta_path: Path) -> Optional[dict]:
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _log.warning("failed to load live meta %s: %s", meta_path, exc)
        return None

    task_id = meta.get("task_id") or meta_path.parent.name
    output_dir = meta_path.parent
    log_file = output_dir / "logs" / "live.log"
    runtime_dir = output_dir / "runtime"
    strategy_path = output_dir / "strategy.py"

    return {
        "status": meta.get("status", LiveStatus.ERROR),
        "broker": meta.get("broker", "simulator"),
        "name": meta.get("name", task_id),
        "output_dir": str(output_dir),
        "log_file": str(log_file),
        "runtime_dir": str(runtime_dir),
        "strategy_path": str(strategy_path),
        "started_at": meta.get("started_at"),
        "stopped_at": meta.get("stopped_at"),
        "returncode": meta.get("returncode"),
        "pid": meta.get("pid"),
        "_proc": None,
        "_restored": True,
        "task_id": task_id,
    }


def _ensure_persisted_tasks_loaded(force: bool = False) -> None:
    global _persisted_loaded
    if _persisted_loaded and not force:
        return

    if LIVE_ROOT.exists():
        for meta_path in LIVE_ROOT.glob("*/live_meta.json"):
            task = _load_task_from_meta(meta_path)
            if not task:
                continue
            task_id = task["task_id"]
            current = _live_tasks.get(task_id)
            if current and current.get("_proc") is not None:
                continue
            _live_tasks[task_id] = task

    _persisted_loaded = True


# ── 修复核心：统一日志聚合器 ────────────────────────────────────────────────────
#
# 问题根因：
#   bullet-trade 进程通过 --log-dir 将策略日志写入框架自己的日志文件
#   （例如 logs/live.log 或 logs/<date>.log），而不是写到 stdout。
#   原来的 _pipe_to_log 只读 proc.stdout，因此捞不到任何策略打印内容。
#
# 修复方案：
#   1. 同时转发 stdout（进程启动错误信息）
#   2. 持续 tail logs/ 目录下所有 *.log 文件（框架写入的策略日志）
#   3. 所有内容统一追加到 live.log，SSE 端无需改动
# ──────────────────────────────────────────────────────────────────────────────

_LOG_LINE_RE = _re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \[")


def _pipe_stdout_to_log(proc: subprocess.Popen, log_file: Path) -> None:
    """
    按行读取子进程 stdout，实时追加到 live.log。

    必须按行读取（readline）而非按块读取（read(N)）：
    - read(N) 在内容不足 N 字节时会阻塞等待，导致日志延迟
    - readline() 配合 PYTHONUNBUFFERED=1 可以做到 print() 后立即可见
    每行加 [STDOUT] 前缀，方便与框架日志区分。

    框架格式化日志行（如 "2026-04-03 09:20:13 [INFO] ..."）会同时写入 app.log，
    由 _tail_log_dir 负责采集，此处跳过以避免重复。
    """
    if proc.stdout is None:
        return
    try:
        with log_file.open("a", encoding="utf-8", errors="replace") as fp:
            for raw_line in proc.stdout:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                if _LOG_LINE_RE.match(line):
                    continue  # 已由 _tail_log_dir 从 app.log 采集，跳过避免重复
                fp.write(f"[STDOUT] {line}\n")
                fp.flush()
    except Exception as exc:
        _log.warning("stdout pipe error: %s", exc)


def _tail_log_dir(task_id: str, proc: subprocess.Popen,
                  log_dir: Path, live_log: Path) -> None:
    """
    持续监视 log_dir 下所有 *.log 文件（排除 live.log 自身），
    将新增内容实时追加到 live.log。

    bullet-trade 框架把策略的 log.info() 写到它自己在 --log-dir 下创建的
    日志文件（文件名由框架决定），所以必须 tail 整个目录而非固定文件名。
    """
    # 记录每个被跟踪文件的当前读取位置
    file_positions: Dict[str, int] = {}

    def _collect_new_log_files() -> List[Path]:
        """扫描 log_dir，返回除 live.log 外所有 .log 文件。"""
        if not log_dir.exists():
            return []
        return [
            p for p in log_dir.glob("*.log")
            if p.name != live_log.name and p.is_file()
        ]

    def _flush_file(src: Path, dest_fp) -> None:
        """读取 src 文件的新增内容并写入 dest_fp。"""
        pos = file_positions.get(str(src), 0)
        try:
            with src.open("rb") as sf:
                sf.seek(pos)
                data = sf.read()
                if data:
                    dest_fp.write(data)
                    dest_fp.flush()
                    file_positions[str(src)] = pos + len(data)
        except Exception as exc:
            _log.debug("tail error for %s: %s", src, exc)

    try:
        with live_log.open("ab") as fp:
            while True:
                # 轮询所有框架日志文件
                for log_path in _collect_new_log_files():
                    _flush_file(log_path, fp)

                # 进程已退出：再做一次完整扫描确保尾部日志不丢
                if proc.poll() is not None:
                    for log_path in _collect_new_log_files():
                        _flush_file(log_path, fp)
                    break

                time.sleep(0.2)
    except Exception as exc:
        _log.warning("log tail thread error task=%s: %s", task_id, exc)


def start_live(
    code: str,
    broker: str = "simulator",
    name: str = "",
    log_dir: Optional[str] = None,
    runtime_dir: Optional[str] = None,
    env_overrides: Optional[Dict[str, str]] = None,
) -> str:
    del log_dir, runtime_dir

    task_id = uuid.uuid4().hex[:12]
    output_dir = (LIVE_ROOT / task_id).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    strategy_path = _write_strategy(output_dir, code)

    task_log_dir = output_dir / "logs"
    task_runtime_dir = output_dir / "runtime"
    task_log_dir.mkdir(exist_ok=True)
    task_runtime_dir.mkdir(exist_ok=True)

    # live.log 是聚合后给 SSE 读的统一日志文件
    live_log = task_log_dir / "live.log"
    live_log.write_text(
        f"[INFO] Live task created: task_id={task_id}\n"
        f"[INFO] broker={broker}\n"
        f"[INFO] output_dir={output_dir}\n"
        f"[INFO] Waiting for process startup...\n",
        encoding="utf-8",
    )

    cmd = [
        *_find_exe(),
        "live",
        str(strategy_path),
        "--broker",
        broker,
        "--log-dir",
        str(task_log_dir),
        "--runtime-dir",
        str(task_runtime_dir),
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(_PROJECT_ROOT),
        env=_utf8_env(env_overrides),
        **_popen_kwargs(),
    )

    _live_tasks[task_id] = {
        "status": LiveStatus.STARTING,
        "broker": broker,
        "name": name or task_id,
        "output_dir": str(output_dir),
        "log_file": str(live_log),       # SSE 读这个聚合文件
        "runtime_dir": str(task_runtime_dir),
        "strategy_path": str(strategy_path),
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "stopped_at": None,
        "returncode": None,
        "pid": proc.pid,
        "_proc": proc,
        "_restored": False,
    }

    # 线程1：转发 stdout（捕获框架启动失败的错误信息）
    threading.Thread(
        target=_pipe_stdout_to_log,
        args=(proc, live_log),
        daemon=True,
        name=f"stdout-{task_id}",
    ).start()

    # 线程2：tail log_dir 下框架写入的策略日志文件
    threading.Thread(
        target=_tail_log_dir,
        args=(task_id, proc, task_log_dir, live_log),
        daemon=True,
        name=f"tail-{task_id}",
    ).start()

    # 线程3：监控进程退出并更新状态
    threading.Thread(
        target=_monitor,
        args=(task_id, proc),
        daemon=True,
        name=f"monitor-{task_id}",
    ).start()

    # 等待进程稳定后更新状态
    time.sleep(0.8)
    if proc.poll() is not None and proc.returncode not in (None, 0):
        _live_tasks[task_id]["status"] = LiveStatus.ERROR
    else:
        _live_tasks[task_id]["status"] = LiveStatus.RUNNING

    _persist(task_id)
    return task_id


def start_live_test() -> str:
    name = f"休市信号测试-{datetime.now():%H%M%S}"
    return start_live(
        code=_TEST_STRATEGY_CODE,
        broker="simulator",
        name=name,
        env_overrides=_build_test_env(name),
    )


def stop_live(task_id: str) -> bool:
    _ensure_persisted_tasks_loaded()
    task = _live_tasks.get(task_id)
    if not task:
        return False

    proc: Optional[subprocess.Popen] = task.get("_proc")
    if proc is None or proc.poll() is not None:
        task["status"] = LiveStatus.STOPPED
        _persist(task_id)
        return True

    try:
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
    except (ProcessLookupError, PermissionError):
        pass

    for _ in range(12):
        time.sleep(0.5)
        if proc.poll() is not None:
            break
    else:
        proc.kill()

    task["status"] = LiveStatus.STOPPED
    task["stopped_at"] = datetime.now().isoformat(timespec="seconds")
    task["returncode"] = proc.returncode
    _persist(task_id)
    return True


def delete_live(task_id: str) -> bool:
    _ensure_persisted_tasks_loaded()
    task = _live_tasks.get(task_id)
    if not task:
        return False

    if task.get("_proc") and task["status"] in (LiveStatus.RUNNING, LiveStatus.STARTING):
        stop_live(task_id)

    output_dir = Path(task.get("output_dir", ""))
    if output_dir.exists() and output_dir.is_dir():
        try:
            shutil.rmtree(output_dir)
        except Exception:
            pass

    _live_tasks.pop(task_id, None)
    return True


def restart_live(task_id: str) -> Optional[str]:
    task = get_live_task(task_id)
    if not task:
        return None

    if task["status"] in (LiveStatus.RUNNING, LiveStatus.STARTING):
        return None

    strategy_path = task.get("strategy_path")
    if not strategy_path or not Path(strategy_path).is_file():
        return None

    try:
        code = Path(strategy_path).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None

    name = task.get("name", task_id)
    broker = task.get("broker", "simulator")
    new_task_id = start_live(code=code, broker=broker, name=name)
    return new_task_id


def get_live_task(task_id: str) -> Optional[dict]:
    _ensure_persisted_tasks_loaded()
    return _live_tasks.get(task_id)


def _public_task(task_id: str, task: dict) -> Dict[str, Any]:
    can_stop = bool(task.get("_proc")) and task["status"] in (
        LiveStatus.RUNNING, LiveStatus.STARTING
    )
    return {
        "task_id": task_id,
        "name": task.get("name", task_id),
        "status": task["status"],
        "broker": task["broker"],
        "started_at": task["started_at"],
        "stopped_at": task.get("stopped_at"),
        "pid": task.get("pid"),
        "can_stop": can_stop,
    }


def get_live_task_view(task_id: str) -> Optional[Dict[str, Any]]:
    task = get_live_task(task_id)
    if not task:
        return None
    return _public_task(task_id, task)


def list_live_tasks() -> List[Dict[str, Any]]:
    _ensure_persisted_tasks_loaded()
    result = []
    for task_id, task in _live_tasks.items():
        result.append(_public_task(task_id, task))
    return result


def get_live_state(task_id: str) -> dict:
    _ensure_persisted_tasks_loaded()
    task = _live_tasks.get(task_id)
    if not task:
        return {}
    state_path = Path(task["runtime_dir"]) / "live_state.json"
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}