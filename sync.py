#!/usr/bin/env python3
"""Sync selected Bambu filament rolls, using slice estimates and an idempotent ledger.

The Bambu network configuration is decrypted in memory only. The only cloud
mutation is a partial update of netWeight for an explicitly configured roll.
"""
from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from urllib import error, parse, request

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "ledger.sqlite3"
CONFIG_PATH = ROOT / "config.json"
REPORT_PATH = ROOT / "同步状态.md"
LOCK_PATH = ROOT / "sync.lock"


class SyncError(RuntimeError):
    pass


def now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def normalize_color(value) -> str:
    return str(value or "").strip().lstrip("#").upper()


def milligrams(value) -> int:
    try:
        d = Decimal(str(value))
        if not d.is_finite() or d < 0:
            raise ValueError()
        return int((d * 1000).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError, TypeError):
        raise SyncError("打印记录中存在无效重量") from None


def grams_integer(mg: int) -> int:
    return max(0, int((Decimal(mg) / 1000).quantize(Decimal("1"), rounding=ROUND_HALF_UP)))


def load_config(require_spools=True) -> dict:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if cfg.get("api_base") not in {"https://api.bambulab.cn", "https://api.bambulab.com"}:
        raise SyncError("云端地址不在允许列表中")
    spools = cfg.get("spools")
    if not isinstance(spools, list) or (require_spools and not spools):
        raise SyncError("请先在配置中登记料卷")
    ids = [str(s["id"]) for s in spools]
    if any(not sid.isdecimal() for sid in ids):
        raise SyncError("料卷 ID 必须填写云端返回的数字 ID")
    if len(ids) != len(set(ids)):
        raise SyncError("配置中存在重复料卷 ID")
    return cfg


def studio_token() -> str:
    # Supply the token only through a protected runtime secret. Never log it
    # or persist it in the checkout.
    token = os.environ.get("BAMBU_CLOUD_TOKEN", "")
    if not token:
        raise SyncError("缺少 BAMBU_CLOUD_TOKEN 运行密钥")
    return token


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Cloud:
    def __init__(self, base: str):
        self.base = base
        self.token = studio_token()
        self.opener = request.build_opener(NoRedirect())

    def call(self, method: str, path: str, params=None, payload=None):
        url = self.base + path
        if params:
            url += "?" + parse.urlencode(params)
        headers = {"Authorization": "Bearer " + self.token,
                   "User-Agent": "BambuStudio/02.08.02.61",
                   "Accept": "application/json"}
        body = None
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(url, data=body, headers=headers, method=method)
        try:
            with self.opener.open(req, timeout=25) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            raise SyncError(f"拓竹云端 HTTP {exc.code}；凭据可能已过期或接口发生变化") from None
        except (error.URLError, TimeoutError, OSError):
            raise SyncError("拓竹云端连接失败或超时") from None
        except (ValueError, UnicodeError):
            raise SyncError("拓竹云端返回了无法解析的数据") from None

    def history(self):
        return self._pages("/v1/user-service/my/tasks", require_total=True)

    def spools(self):
        return self._pages("/v1/design-user-service/my/filament/v2")

    def _pages(self, path, require_total=False):
        result, seen = [], set()
        limit, offset = 50, 0
        expected_total = None
        for _ in range(200):
            page = self.call("GET", path, {"offset": offset, "limit": limit})
            hits = page.get("hits") if isinstance(page, dict) else None
            if not isinstance(hits, list):
                raise SyncError("云端列表结构发生变化，已停止同步")
            total = page.get("total")
            if isinstance(total, int):
                if expected_total is None:
                    expected_total = total
                elif total != expected_total:
                    raise SyncError("读取期间云端列表发生变化，请重试")
            for item in hits:
                ident = str(item.get("id", ""))
                if not ident or ident in seen:
                    raise SyncError("云端列表含重复或缺失 ID，已停止同步")
                seen.add(ident)
                result.append(item)
            offset += len(hits)
            if len(hits) < limit or (expected_total is not None and offset >= expected_total):
                break
        else:
            raise SyncError("云端列表页数过多，已停止同步")
        if require_total and expected_total is None:
            raise SyncError("打印历史缺少总数，无法确认列表完整")
        if expected_total is not None and len(result) != expected_total:
            raise SyncError("云端列表不完整，已停止同步")
        return result

    def update_weight(self, spool: dict, grams: int):
        # The service's update shape requires the spool name as well as its ID.
        if not spool.get("filamentName"):
            raise SyncError("云端料卷缺少名称，无法安全更新")
        self.call("PUT", "/v1/design-user-service/my/filament/v2", payload={
            "id": int(spool["id"]), "filamentName": spool["filamentName"],
            "netWeight": grams})


@contextlib.contextmanager
def single_instance():
    LOCK_PATH.touch(exist_ok=True)
    with LOCK_PATH.open("r+b") as lock:
        if lock.seek(0, 2) == 0:
            lock.write(b"0")
            lock.flush()
        lock.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise SyncError("另一个同步正在运行") from None
        try:
            yield
        finally:
            lock.seek(0)
            if os.name == "nt":
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def connect(path=DB_PATH):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.executescript("""
      PRAGMA foreign_keys=ON;
      CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS spools(
        id TEXT PRIMARY KEY, label TEXT NOT NULL, material TEXT NOT NULL,
        initial_mg INTEGER NOT NULL, expected_cloud_g INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS jobs(
        id TEXT PRIMARY KEY, title TEXT NOT NULL, start_time TEXT,
        status INTEGER NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS usage(
        job_id TEXT NOT NULL REFERENCES jobs(id), part INTEGER NOT NULL,
        spool_id TEXT NOT NULL REFERENCES spools(id), amount_mg INTEGER NOT NULL,
        PRIMARY KEY(job_id,part));
      CREATE TABLE IF NOT EXISTS operations(
        id INTEGER PRIMARY KEY AUTOINCREMENT, spool_id TEXT NOT NULL REFERENCES spools(id),
        before_g INTEGER NOT NULL, after_g INTEGER NOT NULL,
        status TEXT NOT NULL, created_at TEXT NOT NULL, confirmed_at TEXT);
      CREATE TABLE IF NOT EXISTS calibrations(
        id INTEGER PRIMARY KEY AUTOINCREMENT, spool_id TEXT NOT NULL REFERENCES spools(id),
        before_initial_mg INTEGER NOT NULL, after_initial_mg INTEGER NOT NULL,
        before_expected_g INTEGER NOT NULL, accepted_cloud_g INTEGER NOT NULL,
        created_at TEXT NOT NULL);
    """)
    return con


def observed_weight(spool) -> int:
    value = spool.get("netWeight")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0 or int(value) != value:
        raise SyncError("云端料卷重量无效，已停止同步")
    return int(value)


def check_spools(cfg, cloud_spools):
    indexed = {str(s.get("id")): s for s in cloud_spools}
    for s in cfg["spools"]:
        current = indexed.get(str(s["id"]))
        if not current or str(current.get("filamentType", "")).upper() != s["material"]:
            raise SyncError("配置中的料卷不存在或材料类型已变化")
        observed_weight(current)
    return indexed


def historical_match(part, cfg):
    material = str(part.get("targetFilamentType") or part.get("filamentType") or "").upper()
    color = normalize_color(part.get("targetColor"))
    fid = str(part.get("filamentId") or "")
    candidates = []
    for spool in cfg["spools"]:
        if material != spool["material"]:
            continue
        if color in spool.get("historical_target_colors", []) or (fid and fid in spool.get("historical_filament_ids", [])):
            candidates.append(str(spool["id"]))
    return candidates[0] if len(candidates) == 1 else None


def current_match(part, cfg, cloud_spools):
    """Require one matching cloud roll, plus its current AMS slot binding.

    Cloud history does not contain an immutable spool ID, so this is an
    explicitly recorded inference; ambiguous or unbound rolls are deferred.
    """
    material = str(part.get("targetFilamentType") or "").upper()
    color = normalize_color(part.get("targetColor"))
    if not material or not color:
        return None
    matches = [s for s in cloud_spools if str(s.get("filamentType", "")).upper() == material
               and normalize_color(s.get("color")) == color]
    if len(matches) != 1:
        return None
    spool = matches[0]
    sid = str(spool.get("id"))
    if sid not in {str(s["id"]) for s in cfg["spools"]}:
        return None
    ams, slot = part.get("amsId"), part.get("slotId")
    if ams is None or str(ams) in ("254", "255") or slot is None:
        return None
    if not spool.get("inPrinter") or str(spool.get("amsId")) != str(ams) or str(spool.get("slotId")) != str(slot):
        return None
    return sid


def job_parts(job):
    parts = job.get("amsDetailMapping")
    return parts if isinstance(parts, list) else []


def save_job(con, job, state, reason):
    con.execute("INSERT INTO jobs(id,title,start_time,status,state,reason) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET title=excluded.title,status=excluded.status,state=excluded.state,reason=excluded.reason",
                (str(job["id"]), str(job.get("title") or "未命名"), job.get("startTime"), int(job.get("status", -1)), state, reason))


def record_terminal(con, job, cfg, cloud_spools, historical=False):
    status = int(job.get("status", -1))
    parts = job_parts(job)
    mapper = (lambda p: historical_match(p, cfg)) if historical else (lambda p: current_match(p, cfg, cloud_spools))
    mapped = [(i, mapper(p), p) for i, p in enumerate(parts)]
    if status == 3:
        override = next((x for x in cfg.get("manual_estimates", []) if str(x["job_id"]) == str(job["id"])), None)
        if override:
            # An explicit user estimate is applied only if the record still
            # maps entirely to the specific identified roll.
            sid = str(override["spool_id"])
            fraction = Decimal(str(override["fraction"]))
            if not parts or not (0 < fraction <= 1) or any(match != sid for _, match, _ in mapped):
                save_job(con, job, "review", "用户估计与打印记录的材料归属不一致，未扣减")
                return
            try:
                amounts = [(i, int((Decimal(milligrams(p.get("weight"))) * fraction).quantize(Decimal("1"), rounding=ROUND_HALF_UP))) for i, _, p in mapped]
            except SyncError:
                save_job(con, job, "review", "用户估计对应的耗材重量无效，未扣减")
                return
            save_job(con, job, "applied", str(override["reason"]))
            for index, amount in amounts:
                con.execute("INSERT INTO usage(job_id,part,spool_id,amount_mg) VALUES(?,?,?,?)", (str(job["id"]), index, sid, amount))
            return
        relevant = any(sid for _, sid, _ in mapped)
        if not historical:
            relevant = True
        save_job(con, job, "review" if relevant else "ignored", "失败或取消；实际消耗未知，未扣减" if relevant else "基准前的其他任务")
        return
    if status != 2:
        save_job(con, job, "running", "尚未完成，暂不扣减")
        return
    if not parts:
        save_job(con, job, "review", "缺少按材料分配的用量，未扣减")
        return
    # For a future multicolor job, never partially guess an unidentified roll.
    if not historical and any(sid is None for _, sid, _ in mapped):
        save_job(con, job, "review", "耗材颜色或 AMS 槽位无法唯一确认，未扣减")
        return
    if historical and not any(sid for _, sid, _ in mapped):
        save_job(con, job, "ignored", "基准前的其他任务")
        return
    if historical and any(sid is None for _, sid, _ in mapped):
        save_job(con, job, "review", "历史多材料任务无法完整归属，未扣减")
        return
    try:
        amounts = [(i, sid, milligrams(part.get("weight"))) for i, sid, part in mapped]
        if any(amount == 0 for _, _, amount in amounts):
            raise SyncError("耗材重量为空或为零")
    except SyncError:
        save_job(con, job, "review", "耗材重量无效，未扣减")
        return
    save_job(con, job, "applied", "切片用量估计；历史颜色映射" if historical else "切片用量估计；当前颜色和槽位推断")
    for index, sid, amount in amounts:
        con.execute("INSERT INTO usage(job_id,part,spool_id,amount_mg) VALUES(?,?,?,?)", (str(job["id"]), index, sid, amount))


def initialize(con, cfg, cloud_spools, jobs):
    if con.execute("SELECT 1 FROM meta WHERE k='initialized_at'").fetchone():
        raise SyncError("台账已初始化，不能重复初始化")
    indexed = check_spools(cfg, cloud_spools)
    with con:
        con.execute("INSERT INTO meta VALUES('initialized_at',?)", (now(),))
        for s in cfg["spools"]:
            current = indexed[str(s["id"])]
            con.execute("INSERT INTO spools VALUES(?,?,?,?,?)", (str(s["id"]), s["label"], s["material"], milligrams(s["initial_grams"]), observed_weight(current)))
        for job in jobs:
            record_terminal(con, job, cfg, cloud_spools, historical=True)


def ingest(con, cfg, cloud_spools, jobs):
    if not con.execute("SELECT 1 FROM meta WHERE k='initialized_at'").fetchone():
        raise SyncError("台账尚未初始化")
    check_spools(cfg, cloud_spools)
    with con:
        for job in jobs:
            old = con.execute("SELECT state,status FROM jobs WHERE id=?", (str(job["id"]),)).fetchone()
            if old and old["state"] != "running":
                # A terminal result is immutable locally. Changed cloud results
                # require review, not a silent refund or second debit.
                if old["status"] != int(job.get("status", -1)):
                    con.execute("UPDATE jobs SET state='review_changed', reason=reason || '；云端状态后来发生变化，请检查' WHERE id=? AND reason NOT LIKE '%云端状态后来%'", (str(job["id"]),))
                continue
            record_terminal(con, job, cfg, cloud_spools, historical=False)


def desired(con, spool):
    used = con.execute("SELECT COALESCE(SUM(amount_mg),0) FROM usage WHERE spool_id=?", (spool["id"],)).fetchone()[0]
    return grams_integer(spool["initial_mg"] - used), used


def get_spool(cloud, sid):
    matches = [s for s in cloud.spools() if str(s.get("id")) == sid]
    if len(matches) != 1:
        raise SyncError("目标料卷不存在或重复")
    return matches[0]


def complete_operation(con, op, current):
    with con:
        con.execute("UPDATE operations SET status='confirmed',confirmed_at=? WHERE id=?", (now(), op["id"]))
        con.execute("UPDATE spools SET expected_cloud_g=? WHERE id=?", (current, op["spool_id"]))
    checkpoint(con)


def execute_pending(con, cloud, op):
    current = get_spool(cloud, op["spool_id"])
    observed = observed_weight(current)
    if observed == op["after_g"]:
        complete_operation(con, op, observed)
        return
    if observed != op["before_g"]:
        raise SyncError(f"料卷 {op['spool_id']} 的云端重量被其他操作修改，已暂停")
    cloud.update_weight(current, op["after_g"])
    # The cloud can be eventually consistent. A pending journal is deliberately
    # kept if verification does not yet see the target.
    for delay in (0, 1, 2):
        if delay:
            time.sleep(delay)
        latest = get_spool(cloud, op["spool_id"])
        observed = observed_weight(latest)
        if observed == op["after_g"]:
            complete_operation(con, op, observed)
            return
        if observed != op["before_g"]:
            raise SyncError(f"料卷 {op['spool_id']} 写回后出现冲突，已暂停")
    raise SyncError(f"料卷 {op['spool_id']} 写回尚未读回确认，下次将安全核对")


def push(con, cloud):
    warnings = []
    for spool in con.execute("SELECT * FROM spools ORDER BY rowid").fetchall():
        sid = spool["id"]
        try:
            op = con.execute("SELECT * FROM operations WHERE spool_id=? AND status='pending' ORDER BY id LIMIT 1", (sid,)).fetchone()
            if op:
                execute_pending(con, cloud, op)
                spool = con.execute("SELECT * FROM spools WHERE id=?", (sid,)).fetchone()
            goal, used = desired(con, spool)
            if used > spool["initial_mg"]:
                warnings.append(f"{spool['label']} 的估计消耗超过初始重量，已限制为 0 g，请称重校准。")
            current = get_spool(cloud, sid)
            actual = observed_weight(current)
            if actual != spool["expected_cloud_g"]:
                raise SyncError(f"{spool['label']} 的云端重量从 {spool['expected_cloud_g']} g 变为 {actual} g，已暂停该卷")
            if goal == actual:
                continue
            with con:
                cur = con.execute("INSERT INTO operations(spool_id,before_g,after_g,status,created_at) VALUES(?,?,?,'pending',?)", (sid, actual, goal, now()))
            # The pending operation must be durable in cloud storage BEFORE
            # the Bambu write. A failure here prohibits that write.
            checkpoint(con)
            op = con.execute("SELECT * FROM operations WHERE id=?", (cur.lastrowid,)).fetchone()
            execute_pending(con, cloud, op)
        except SyncError as exc:
            warnings.append(str(exc))
    return warnings


def checkpoint(con):
    """Durably replicate the ledger before and after each cloud mutation.

    Git rejects non-fast-forward pushes, preventing silent replacement of a
    newer ledger. The workflow serializes jobs independently; local execution
    must be switched off before cloud activation.
    """
    if os.environ.get("BAMBU_PERSIST") != "git":
        return
    con.commit()
    # Recheck immediately before persisting private print-history data.
    verify_private_repository()
    branch = os.environ.get("BAMBU_STATE_BRANCH", "")
    if not branch or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-/" for ch in branch) or branch.startswith("-"):
        raise SyncError("云端账本分支配置无效")
    def run_git(*args, allowed=(0,)):
        try:
            result = subprocess.run(["git", *args], cwd=ROOT, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, timeout=90, check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise SyncError("云端账本持久化失败，本轮已停止写入") from None
        if result.returncode not in allowed:
            # Git stderr may contain credential-bearing remote URLs.
            raise SyncError("云端账本推送失败或存在并发冲突，本轮已停止写入")
        return result.returncode
    run_git("add", "--", "ledger.sqlite3")
    changed = run_git("diff", "--cached", "--quiet", "--", "ledger.sqlite3", allowed=(0, 1))
    if changed == 1:
        run_git("commit", "--only", "-m", "Record filament sync checkpoint", "--", "ledger.sqlite3")
    # Always push even when nothing is staged: a previous push may have
    # failed after the commit was created.
    run_git("push", "origin", "HEAD:refs/heads/" + branch)


def verify_private_repository():
    """Refuse cloud operation unless GitHub reports this repo is private."""
    if os.environ.get("BAMBU_PERSIST") != "git":
        return
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GH_STATE_TOKEN", "")
    pieces = repo.split("/")
    valid = lambda x: bool(x) and all(c.isalnum() or c in ".-_" for c in x)
    if len(pieces) != 2 or not all(valid(x) for x in pieces) or not token:
        raise SyncError("缺少云端私有仓库验证信息")
    req = request.Request("https://api.github.com/repos/" + repo,
        headers={"Authorization": "Bearer " + token, "User-Agent": "bambu-filament-sync",
                 "Accept": "application/vnd.github+json"})
    try:
        with request.build_opener(NoRedirect()).open(req, timeout=20) as response:
            metadata = json.load(response)
    except Exception:
        raise SyncError("无法确认云端仓库的隐私设置，已停止运行") from None
    if metadata.get("private") is not True or metadata.get("full_name", "").lower() != repo.lower():
        raise SyncError("仓库不是预期的私有仓库，已停止运行")


def accept_cloud_weight(con, cloud, spool_id):
    """Explicitly accept a user's manually calibrated cloud weight as a new anchor."""
    spool = con.execute("SELECT * FROM spools WHERE id=?", (spool_id,)).fetchone()
    if not spool:
        raise SyncError("此料卷不在自动同步范围内")
    if con.execute("SELECT 1 FROM operations WHERE spool_id=? AND status='pending'", (spool_id,)).fetchone():
        raise SyncError("该料卷仍有待确认写回，不能校准")
    observed = observed_weight(get_spool(cloud, spool_id))
    _, used = desired(con, spool)
    new_initial = observed * 1000 + used
    with con:
        con.execute("INSERT INTO calibrations(spool_id,before_initial_mg,after_initial_mg,before_expected_g,accepted_cloud_g,created_at) VALUES(?,?,?,?,?,?)",
                    (spool_id, spool["initial_mg"], new_initial, spool["expected_cloud_g"], observed, now()))
        con.execute("UPDATE spools SET initial_mg=?,expected_cloud_g=? WHERE id=?", (new_initial, observed, spool_id))


def backup(con):
    path = ROOT / "ledger-backup.sqlite3"
    tmp = ROOT / "ledger-backup.sqlite3.tmp"
    if tmp.exists():
        tmp.unlink()
    target = sqlite3.connect(tmp)
    try:
        con.backup(target)
    finally:
        target.close()
    tmp.replace(path)


def local_time(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone().strftime("%Y-%m-%d %H:%M")
    except (AttributeError, ValueError):
        return value or "时间未知"


def report(con, warnings, error_message=None):
    lines = ["# 拓竹耗材自动同步状态", "", f"更新时间：{now()}", "",
             "重量来自拓竹打印记录的切片估计，并非称重；启动排料、失败任务和换卷可能带来误差。", "",
             "| 料卷 | 估计已使用 | 估计剩余 | 上次确认云端重量 |", "| --- | ---: | ---: | ---: |"]
    for s in con.execute("SELECT * FROM spools ORDER BY rowid"):
        goal, used = desired(con, s)
        lines.append(f"| {s['label']} | {used / 1000:.2f} g | {goal} g | {s['expected_cloud_g']} g |")
    reviews = con.execute("SELECT * FROM jobs WHERE state IN ('review','review_changed') ORDER BY start_time DESC").fetchall()
    active = con.execute("SELECT * FROM jobs WHERE state='running' ORDER BY start_time DESC").fetchall()
    pending = con.execute("SELECT * FROM operations WHERE status='pending'").fetchall()
    lines += ["", f"待核对任务：{len(reviews)}；进行中任务：{len(active)}；待确认写回：{len(pending)}。"]
    for row in reviews:
        title = row["title"].replace("\n", " ").replace("|", " ")
        lines.append(f"- {local_time(row['start_time'])}：{title}；{row['reason']}。")
    for row in active:
        lines.append(f"- 进行中：{row['title'].replace(chr(10), ' ')}；完成后再扣减。")
    if error_message:
        warnings = list(warnings) + [error_message]
    if warnings:
        lines.extend(["", "## 需要处理", ""])
        lines.extend("- " + warning for warning in warnings)
    lines += ["", "每次云端写入前核对上次确认的重量；存在差异时暂停对应料卷。", ""]
    tmp = REPORT_PATH.with_suffix(".tmp")
    tmp.write_text("\n".join(lines), encoding="utf-8")
    tmp.replace(REPORT_PATH)
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Synchronize selected Bambu spool estimates")
    parser.add_argument("action", choices=["list-spools", "preview", "initialize", "sync", "report", "accept-cloud"])
    parser.add_argument("--spool-id", help="料卷 ID；仅 accept-cloud 使用")
    args = parser.parse_args(argv)
    verify_private_repository()
    with single_instance():
        con = connect()
        try:
            cfg = load_config(require_spools=args.action != "list-spools")
            if args.action == "report":
                print(REPORT_PATH.read_text(encoding="utf-8") if REPORT_PATH.exists() else report(con, []))
                return 0
            cloud = Cloud(cfg["api_base"])
            if args.action == "list-spools":
                for spool in cloud.spools():
                    print(json.dumps({k: spool.get(k) for k in ("id", "filamentName", "filamentType", "color", "netWeight", "totalNetWeight", "inPrinter", "amsId", "slotId")}, ensure_ascii=False))
                return 0
            if args.action == "accept-cloud":
                if not args.spool_id:
                    raise SyncError("accept-cloud 需要 --spool-id")
                check_spools(cfg, cloud.spools())
                accept_cloud_weight(con, cloud, args.spool_id)
                checkpoint(con)
                backup(con)
                print(report(con, []))
                return 0
            spools, jobs = cloud.spools(), cloud.history()
            if args.action == "preview":
                tmp = connect(":memory:")
                initialize(tmp, cfg, spools, jobs)
                for s in tmp.execute("SELECT * FROM spools ORDER BY rowid"):
                    goal, used = desired(tmp, s)
                    print(f"{s['label']}: 当前云端 {s['expected_cloud_g']} g；历史估计消耗 {used / 1000:.2f} g；拟写回 {goal} g")
                print("待核对任务:", tmp.execute("SELECT COUNT(*) FROM jobs WHERE state='review'").fetchone()[0])
                print("进行中任务:", tmp.execute("SELECT COUNT(*) FROM jobs WHERE state='running'").fetchone()[0])
                return 0
            if args.action == "initialize":
                initialize(con, cfg, spools, jobs)
            else:
                ingest(con, cfg, spools, jobs)
            checkpoint(con)
            warnings = push(con, cloud)
            text_report = report(con, warnings)
            checkpoint(con)
            if os.environ.get("BAMBU_CLOUD_MODE") == "1":
                reviews = con.execute("SELECT COUNT(*) FROM jobs WHERE state IN ('review','review_changed')").fetchone()[0]
                pending = con.execute("SELECT COUNT(*) FROM operations WHERE status='pending'").fetchone()[0]
                print(f"同步结束；待核对任务 {reviews}，待确认写回 {pending}，同步警告 {len(warnings)}。")
                for warning in warnings:
                    print("同步警告：", warning)
                for review in con.execute("SELECT id,reason FROM jobs WHERE state IN ('review','review_changed') ORDER BY start_time DESC"):
                    print("待核对任务：", review["id"], review["reason"])
            else:
                print(text_report)
            backup(con)
            return 2 if warnings else 0
        except Exception as exc:
            if isinstance(exc, SyncError):
                message = str(exc)
            else:
                # Avoid including secrets or request headers in exceptions.
                message = "同步异常：" + type(exc).__name__
            try:
                report(con, [], message)
            except Exception:
                pass
            print(message, file=sys.stderr)
            return 1
        finally:
            con.close()


if __name__ == "__main__":
    sys.exit(main())
