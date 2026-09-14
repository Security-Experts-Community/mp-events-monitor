"""Пути в собранном exe (запрос оператора 11.09).

PyInstaller onefile распаковывает код во временную папку, поэтому
статика ищется в бандле, а configs/out/logs — рядом с exe.
"""

import sys
from pathlib import Path

import pytest
from webcheck import requires_web

from nomos import paths

ROOT = Path(__file__).resolve().parent.parent


class TestFromSources:
    def test_base_dir_is_repo_root(self):
        assert paths.app_base_dir() == ROOT

    def test_not_frozen(self):
        assert not paths.is_frozen()

    @requires_web
    def test_resource_path_points_into_repo(self):
        assert paths.resource_path("nomos/web/static").is_dir()

    def test_use_app_base_dir_does_not_chdir_from_sources(self, tmp_path, monkeypatch):
        """Разработке cwd менять нельзя: тесты и CLI запускают откуда угодно."""
        monkeypatch.chdir(tmp_path)
        paths.use_app_base_dir()
        assert Path.cwd() == tmp_path


class TestFrozen:
    @pytest.fixture()
    def frozen(self, tmp_path, monkeypatch):
        exe_dir = tmp_path / "release"
        exe_dir.mkdir()
        bundle = tmp_path / "_MEI12345"
        (bundle / "nomos" / "web" / "static").mkdir(parents=True)
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        monkeypatch.setattr(sys, "executable", str(exe_dir / "Nomos.exe"), raising=False)
        monkeypatch.setattr(sys, "_MEIPASS", str(bundle), raising=False)
        return exe_dir, bundle

    def test_base_dir_is_exe_folder(self, frozen):
        exe_dir, _ = frozen
        assert paths.app_base_dir() == exe_dir.resolve()

    def test_static_comes_from_bundle(self, frozen):
        _, bundle = frozen
        assert paths.resource_path("nomos/web/static") == bundle / "nomos/web/static"

    def test_working_dir_follows_the_exe(self, frozen, tmp_path, monkeypatch):
        """Запуск из чужой директории не должен менять, где искать configs."""
        other = tmp_path / "somewhere"
        other.mkdir()
        monkeypatch.chdir(other)
        base = paths.use_app_base_dir()
        exe_dir, _ = frozen
        assert base == exe_dir.resolve()
        assert Path.cwd() == exe_dir.resolve()


@requires_web
class TestBuildRecipe:
    def test_spec_bundles_static_and_hidden_imports(self):
        spec = (ROOT / "nomos.spec").read_text(encoding="utf-8")
        assert '("nomos/web/static", "nomos/web/static")' in spec
        assert 'collect_submodules("uvicorn")' in spec
        assert '"nomos_web.py"' in spec

    def test_server_resolves_static_through_bundle(self):
        source = (ROOT / "nomos/web/server.py").read_text(encoding="utf-8")
        assert 'resource_path("nomos/web/static")' in source
        assert "if not STATIC_DIR.is_dir():" in source, "нет отката для запуска из исходников"

    def test_entrypoint_switches_to_exe_folder(self):
        source = (ROOT / "nomos_web.py").read_text(encoding="utf-8")
        assert "use_app_base_dir()" in source

    def test_instruction_exists_and_covers_configs(self):
        doc = (ROOT / "docs/BUILD_EXE.md").read_text(encoding="utf-8")
        for expected in ("pyinstaller nomos.spec", "configs", ".config.env", "hiddenimports"):
            assert expected in doc, expected

    def test_release_script_does_not_ship_secrets(self):
        script = (ROOT / "build_exe.ps1").read_text(encoding="utf-8")
        assert "example.config.env" in script
        assert ".config.env НЕ копируем" in script
