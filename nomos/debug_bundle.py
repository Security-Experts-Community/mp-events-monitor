"""Диагностический бандл — архив для передачи после ручного прогона.

Состав:
* ``logs/nomos.log`` и ``logs/{run_id}.jsonl``;
* ``debug/{run_id}/`` — HTTP-дампы (если включались, уже редактированные);
* ``configs/.config.env`` — С МАСКИРОВКОЙ секретов;
* используемые json-конфиги (assets_filters, event_policies);
* ``environment.json`` — версии Python/пакетов, ОС;
* ``*_stat.json`` из out/ — итоговая статистика прогона;
* ``query_fixtures.json`` — план запросов (если включался ``dump_queries``).

Секреты редактируются повторно на входе в архив (защита в глубину:
даже если какой-то канал записал секрет, в бандл он не попадёт).
"""

from __future__ import annotations

import importlib.metadata
import json
import logging
import platform
import sys
import zipfile
from datetime import datetime
from pathlib import Path

from nomos.compat import UTC
from nomos.redact import redact, redact_env_text

logger = logging.getLogger("nomos.bundle")

_TEXT_SUFFIXES = {".log", ".jsonl", ".json", ".txt", ".env"}

_PACKAGES_OF_INTEREST = (
    "requests",
    "aiohttp",
    "pydantic",
    "pydantic-settings",
    "xlsxwriter",
    "urllib3",
    "backoff",
    "tqdm",
    "pandas",
)


def _environment_info() -> dict:
    versions = {}
    for package in _PACKAGES_OF_INTEREST:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    return {
        "python": sys.version,
        "platform": platform.platform(),
        "packages": versions,
        "created": datetime.now(UTC).isoformat(),
    }


def _add_file(zf: zipfile.ZipFile, path: Path, arcname: str) -> None:
    if path.suffix.lower() in _TEXT_SUFFIXES:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as err:
            logger.warning(f"пропускаю {path}: {err}")
            return
        if path.name.endswith(".env"):
            text = redact_env_text(text)
        else:
            text = redact(text)
        zf.writestr(arcname, text)
    else:
        zf.write(path, arcname)


def make_debug_bundle(
    run_id: str | None = None,
    logs_dir: Path | str = Path("logs"),
    debug_dir: Path | str = Path("debug"),
    out_dir: Path | str = Path("out"),
    configs_dir: Path | str = Path("configs"),
    bundle_dir: Path | str = Path("."),
) -> Path:
    """Собирает zip и возвращает путь к нему."""
    logs_dir, debug_dir = Path(logs_dir), Path(debug_dir)
    out_dir, configs_dir = Path(out_dir), Path(configs_dir)

    if run_id is None:
        jsonls = sorted(logs_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime)
        if not jsonls:
            raise FileNotFoundError(
                f"в {logs_dir} нет *.jsonl — не было ни одного прогона с журналированием"
            )
        run_id = jsonls[-1].stem
        logger.info(f"run_id не задан, беру последний: {run_id}")

    bundle_path = Path(bundle_dir) / f"nomos_debug_{run_id}.zip"
    with zipfile.ZipFile(bundle_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "environment.json",
            json.dumps(_environment_info(), ensure_ascii=False, indent=2),
        )
        for name in ("nomos.log", f"{run_id}.jsonl"):
            path = logs_dir / name
            if path.exists():
                _add_file(zf, path, f"logs/{name}")
        run_debug = debug_dir / run_id
        if run_debug.is_dir():
            for path in sorted(run_debug.rglob("*")):
                if path.is_file():
                    _add_file(zf, path, f"debug/{run_id}/{path.relative_to(run_debug)}")
        env_file = configs_dir / ".config.env"
        if env_file.exists():
            _add_file(zf, env_file, "configs/config.env.redacted")
        for name in ("assets_filters.json", "event_policies.json"):
            path = configs_dir / name
            if path.exists():
                _add_file(zf, path, f"configs/{name}")
        if out_dir.is_dir():
            for path in sorted(out_dir.glob("*_stat.json")):
                _add_file(zf, path, f"out/{path.name}")
            plan = out_dir / "query_fixtures.json"
            if plan.is_file():
                _add_file(zf, plan, f"out/{plan.name}")
            unified = out_dir / "unified_report.json"
            if unified.exists():
                _add_file(zf, unified, "out/unified_report.json")

    logger.info(f"диагностический бандл: {bundle_path.resolve()}")
    return bundle_path
