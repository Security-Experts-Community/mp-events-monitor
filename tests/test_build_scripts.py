"""Скрипты сборки поставки (запрос оператора 15.09).

Сами сборки здесь не запускаются — это минуты и сеть. Проверяется то,
что ломается молча и обнаруживается уже на машине оператора: кодировка
файлов, переводы строк, состав шагов.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
PS1 = ROOT / "build_cli.ps1"
SH = ROOT / "build_cli.sh"


class TestFilesAreUsableOnTargetOs:
    def test_powershell_script_has_bom(self):
        """Windows PowerShell 5.1 без BOM читает .ps1 как ANSI — кириллица портится."""
        assert PS1.read_bytes()[:3] == b"\xef\xbb\xbf"

    def test_shell_script_has_no_carriage_returns(self):
        r"""bash спотыкается о '\r': "command not found" на каждой строке."""
        assert b"\r" not in SH.read_bytes()

    def test_shell_script_starts_with_shebang(self):
        assert SH.read_bytes().startswith(b"#!/usr/bin/env bash")

    def test_both_are_valid_text(self):
        for path in (PS1, SH):
            path.read_text(encoding="utf-8")  # не должно бросить

    def test_gitattributes_keeps_line_endings(self):
        rules = (ROOT / ".gitattributes").read_text(encoding="utf-8")
        assert "*.sh text eol=lf" in rules


class TestBothDoTheSameThing:
    @pytest.fixture(params=["ps1", "sh"])
    def script(self, request):
        return (PS1 if request.param == "ps1" else SH).read_text(encoding="utf-8")

    def test_creates_local_venv(self, script):
        assert ".venv" in script

    def test_installs_requirements_and_pyinstaller(self, script):
        assert "requirements.txt" in script
        assert "pyinstaller" in script

    def test_installs_dev_tools_for_the_test_gate(self, script):
        """Сборщик прогоняет тесты: без [dev] шаг падает на 'No module named pytest'."""
        assert '".[dev]"' in script

    def test_calls_the_release_script(self, script):
        assert "make_cli_release.py" in script

    def test_passes_arguments_through(self, script):
        """--no-test и --out должны доходить до сборщика."""
        assert "@args" in script or '"$@"' in script

    def test_forces_utf8_for_child_processes(self, script):
        assert "PYTHONIOENCODING" in script
        assert "PYTHONUTF8" in script

    def test_works_from_any_current_directory(self, script):
        assert "$PSScriptRoot" in script or 'cd "$(dirname "$0")"' in script

    def test_explains_missing_python(self, script):
        assert "python.org" in script


class TestShellScriptPortability:
    def test_handles_both_venv_layouts(self):
        """Windows кладёт python в .venv/Scripts, остальные — в .venv/bin."""
        source = SH.read_text(encoding="utf-8")
        assert ".venv/Scripts/python.exe" in source
        assert ".venv/bin/python" in source

    def test_stops_on_first_error(self):
        assert "set -euo pipefail" in SH.read_text(encoding="utf-8")

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="на Windows 'bash' — это Git Bash или заглушка WSL: проверка ненадёжна",
    )
    def test_syntax_is_valid(self):
        import shutil

        from procs import run

        if shutil.which("bash") is None:
            pytest.skip("bash недоступен")
        result = run(["bash", "-n", str(SH)])
        assert result.returncode == 0, result.stderr
