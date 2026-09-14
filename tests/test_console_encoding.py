"""Кодировка вывода на Windows (репорт оператора 15.09).

В Git Bash сборщик CLI-поставки печатал «▒▒▒▒▒▒▒ ▒▒▒▒▒▒: 448» вместо
русского текста и падал на чтении вывода pytest:

    UnicodeDecodeError: 'utf-8' codec can't decode byte 0xf0 ...

Причина одна: Python на Windows берёт кодировку локали (cp1251/cp866),
если вывод идёт не в настоящую консоль, — и для своего stdout, и для
stdout дочернего процесса.
"""

import io
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import envcheck  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

# Инструменты, печатающие русский текст: их вывод читают люди
RUSSIAN_OUTPUT_TOOLS = [
    "tools/make_cli_dist.py",
    "tools/kb_config_generator.py",
    "tools/make_debug_bundle.py",
    "tools/golden.py",
]


class FakeStream:
    """Поток с кодировкой, как у Windows-локали (StringIO её не даёт менять)."""

    def __init__(self, encoding="cp1251"):
        self.encoding = encoding
        self.reconfigured = None
        self.written = []

    def reconfigure(self, encoding=None, errors=None):
        self.reconfigured = (encoding, errors)
        self.encoding = encoding

    def write(self, text):
        self.written.append(text)

    def flush(self):
        pass


class TestConsoleEncoding:
    def test_locale_encoding_is_replaced_with_utf8(self, monkeypatch):
        stream = FakeStream("cp1251")
        monkeypatch.setattr(sys, "stdout", stream)
        monkeypatch.setattr(sys, "stderr", FakeStream("cp866"))
        envcheck.configure_console()
        assert stream.reconfigured == ("utf-8", "replace")

    @pytest.mark.parametrize("encoding", ["utf-8", "UTF-8", "utf8"])
    def test_utf8_streams_are_left_alone(self, encoding, monkeypatch):
        """Настоящую консоль cmd.exe трогать незачем."""
        stream = FakeStream(encoding)
        monkeypatch.setattr(sys, "stdout", stream)
        monkeypatch.setattr(sys, "stderr", FakeStream(encoding))
        envcheck.configure_console()
        assert stream.reconfigured is None

    def test_streams_without_reconfigure_do_not_break(self, monkeypatch):
        monkeypatch.setattr(sys, "stdout", io.StringIO())
        monkeypatch.setattr(sys, "stderr", io.StringIO())
        envcheck.configure_console()  # не должно бросить

    def test_errors_replace_so_output_never_kills_the_program(self, monkeypatch):
        stream = FakeStream("cp1251")
        monkeypatch.setattr(sys, "stdout", stream)
        monkeypatch.setattr(sys, "stderr", FakeStream("cp1251"))
        envcheck.configure_console()
        assert stream.reconfigured[1] == "replace"


class TestChildProcesses:
    def test_child_environment_forces_utf8(self):
        env = envcheck.child_environment({"PATH": "/usr/bin"})
        assert env["PYTHONIOENCODING"] == "utf-8"
        assert env["PYTHONUTF8"] == "1"
        assert env["PATH"] == "/usr/bin", "окружение родителя должно сохраняться"

    def test_helper_reads_russian_output(self, tmp_path):
        """Сквозная проверка: дочерний Python печатает кириллицу."""
        from procs import run

        script = tmp_path / "child.py"
        script.write_text("print('Проверка кодировки: ✓')", encoding="utf-8")
        result = run([sys.executable, str(script)])
        assert result.returncode == 0
        assert "Проверка кодировки" in result.stdout

    def test_no_raw_subprocess_without_encoding_guard(self):
        """Новый subprocess.run должен идти через общий помощник."""
        offenders = []
        for folder in ("tools", "tests"):
            for path in (ROOT / folder).rglob("*.py"):
                if path.name == "procs.py":
                    continue
                source = path.read_text(encoding="utf-8")
                for match in re.finditer(r"subprocess\.run\((.{0,400})", source, re.S):
                    call = match.group(1)
                    if "errors=" not in call or "env=" not in call:
                        offenders.append(path.name)
        assert not offenders, (
            f"subprocess.run без errors=/env= (на Windows упадёт): {set(offenders)}"
        )


class TestToolsConfigureConsole:
    @pytest.mark.parametrize("tool", RUSSIAN_OUTPUT_TOOLS)
    def test_tool_fixes_its_console(self, tool):
        path = ROOT / tool
        if not path.exists():
            pytest.skip(f"{tool} не входит в эту поставку")
        source = path.read_text(encoding="utf-8")
        assert "configure_console()" in source, f"{tool}: русский вывод сломается в Git Bash"

    @pytest.mark.parametrize("entry", ["Nomos.py", "nomos_web.py"])
    def test_entry_points_fix_console_through_envcheck(self, entry):
        path = ROOT / entry
        if not path.exists():
            pytest.skip(f"{entry} не входит в эту поставку")
        assert "ensure_supported_python()" in path.read_text(encoding="utf-8")
        source = (ROOT / "envcheck.py").read_text(encoding="utf-8")
        guard = source.index("def ensure_supported_python")
        assert "configure_console()" in source[guard:], "проверка версии не чинит консоль"
