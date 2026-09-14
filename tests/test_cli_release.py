"""Сборка поставки «единый exe + конфиги» (запрос оператора 15.09).

Сам PyInstaller здесь не запускается — это минуты и целевая ОС. Тесты
проверяют то, что ломается молча: состав поставки, отсутствие секрета в
архиве, читаемые имена внутри zip и то, что exe ищет конфиги рядом с
собой, а не в текущей директории.
"""

import importlib.util
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "tools" / "make_cli_release.py"


@pytest.fixture(scope="module")
def release():
    spec = importlib.util.spec_from_file_location("make_cli_release", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def fake_exe(tmp_path):
    exe = tmp_path / "NomosCLI"
    exe.write_bytes(b"MZ\x00\x00")  # не настоящий, но собирать его и не надо
    return exe


class TestReleaseLayout:
    def test_exe_and_configs_lie_side_by_side(self, release, fake_exe, tmp_path):
        target = release.assemble(fake_exe, tmp_path / "NomosCLI_test")
        assert (target / fake_exe.name).is_file()
        assert (target / "configs" / "event_policies.json").is_file()
        assert (target / "configs" / "assets_filters.json").is_file()
        assert (target / "configs" / "example.config.env").is_file()

    def test_secret_is_never_packed(self, release, fake_exe, tmp_path, monkeypatch):
        """configs/.config.env — доступ к стенду, ему в архиве не место."""
        configs = tmp_path / "configs"
        configs.mkdir()
        (configs / "event_policies.json").write_text("{}", encoding="utf-8")
        (configs / ".config.env").write_text("PERSONAL_TOKEN=pat_secret", encoding="utf-8")
        monkeypatch.setattr(release, "ROOT", tmp_path)
        target = release.assemble(fake_exe, tmp_path / "release")
        assert not (target / "configs" / ".config.env").exists()
        assert (target / "configs" / "event_policies.json").is_file()

    def test_readme_tells_how_to_start(self, release, fake_exe, tmp_path):
        target = release.assemble(fake_exe, tmp_path / "NomosCLI_test")
        readme = (target / "README.txt").read_text(encoding="utf-8")
        assert ".config.env" in readme
        assert "configs" in readme
        assert fake_exe.name in readme

    def test_rebuild_does_not_mix_with_previous(self, release, fake_exe, tmp_path):
        target_dir = tmp_path / "NomosCLI_test"
        release.assemble(fake_exe, target_dir)
        stale = target_dir / "configs" / "старый_конфиг.json"
        stale.write_text("{}", encoding="utf-8")
        release.assemble(fake_exe, target_dir)
        assert not stale.exists(), "папка поставки не очищается перед сборкой"


class TestArchive:
    def test_names_are_readable_on_windows(self, release, fake_exe, tmp_path):
        target = release.assemble(fake_exe, tmp_path / "NomosCLI_test")
        (target / "Отчёт.txt").write_text("проверка", encoding="utf-8")
        archive = release.pack(target, tmp_path / "out.zip")
        with zipfile.ZipFile(archive) as zf:
            cyrillic = [i for i in zf.infolist() if any(ord(c) > 127 for c in i.filename)]
            assert cyrillic, "не на чем проверить флаг"
            for item in cyrillic:
                assert item.flag_bits & 0x800, "Проводник покажет имена как «Ъ©...»"

    def test_archive_root_is_one_folder(self, release, fake_exe, tmp_path):
        target = release.assemble(fake_exe, tmp_path / "NomosCLI_test")
        archive = release.pack(target, tmp_path / "out.zip")
        roots = {name.split("/")[0] for name in zipfile.ZipFile(archive).namelist()}
        assert roots == {"NomosCLI_test"}, "распаковка не должна сорить в текущую папку"

    def test_contents_are_complete(self, release, fake_exe, tmp_path):
        target = release.assemble(fake_exe, tmp_path / "NomosCLI_test")
        names = zipfile.ZipFile(release.pack(target, tmp_path / "out.zip")).namelist()
        assert f"NomosCLI_test/{fake_exe.name}" in names
        assert "NomosCLI_test/README.txt" in names
        assert any(name.startswith("NomosCLI_test/configs/") for name in names)


class TestBuildRecipe:
    def test_spec_is_a_real_file_not_generated_text(self):
        spec = (ROOT / "nomos_cli.spec").read_text(encoding="utf-8")
        assert '"Nomos.py"' in spec
        assert "NomosCLI" in spec
        for web_package in ("fastapi", "uvicorn", "starlette"):
            assert web_package in spec, f"{web_package} должен быть в excludes"

    def test_script_checks_pyinstaller_before_building(self, release):
        source = SCRIPT.read_text(encoding="utf-8")
        assert "def check_pyinstaller" in source
        assert "pip install pyinstaller" in source

    def test_missing_pytest_is_explained_not_just_reported(self):
        """«No module named pytest» не должно выглядеть как красные тесты."""
        source = SCRIPT.read_text(encoding="utf-8")
        assert "def check_pytest" in source
        assert '.[dev]' in source
        assert "--no-test" in source

    def test_script_runs_tests_by_default(self):
        source = SCRIPT.read_text(encoding="utf-8")
        assert "--no-test" in source
        assert "тесты не прошли" in source

    def test_frozen_cli_works_in_its_own_folder(self):
        """Иначе запуск ярлыком не найдёт configs рядом с exe."""
        source = (ROOT / "Nomos.py").read_text(encoding="utf-8")
        assert 'getattr(sys, "frozen", False)' in source
        assert "os.chdir" in source

    def test_no_builtin_exit_in_frozen_paths(self):
        """В exe нет site-builtins: exit() там — NameError на запуске.

        Именно на этом собранный бинарник падал в первой же проверке:
        NameError: name 'exit' is not defined. Разбираем AST, а не текст,
        чтобы упоминания exit() в комментариях не мешали.
        """
        import ast

        offenders = []
        for name in ("Nomos.py", "lib", "nomos"):
            path = ROOT / name
            files = [path] if path.is_file() else sorted(path.rglob("*.py"))
            for file in files:
                tree = ast.parse(file.read_text(encoding="utf-8"))
                for node in ast.walk(tree):
                    if (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id in {"exit", "quit"}
                    ):
                        offenders.append(f"{file.name}:{node.lineno}")
        assert not offenders, f"вызовы exit()/quit(), в exe их нет: {offenders}"
