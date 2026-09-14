"""Пайплайн CI (запрос оператора 15.09).

Файл .gitlab-ci.yml проверяется здесь, а не на четвёртой минуте сборки:
опечатка в имени spec-файла или забытый шаг видны сразу.
"""

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
CI_FILE = ROOT / ".gitlab-ci.yml"

pytestmark = pytest.mark.skipif(
    not CI_FILE.exists(), reason="CI живёт в репозитории, в поставку не входит"
)


@pytest.fixture(scope="module")
def ci():
    return yaml.safe_load(CI_FILE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def raw():
    return CI_FILE.read_text(encoding="utf-8")


class TestStructure:
    def test_yaml_is_valid(self, ci):
        assert isinstance(ci, dict)

    def test_stages_unchanged(self, ci):
        assert ci["stages"] == ["version", "checks", "fix", "build"]

    @pytest.mark.parametrize(
        "job",
        ["ruff", "pytest", "vermin", "auto_fix", "Version",
         "Build:debian", "Build:win", "Build:cli-sources"],
    )
    def test_job_present(self, ci, job):
        assert job in ci

    def test_release_jobs_are_gated_by_build_settings(self, ci):
        for job in ("Version", "Build:debian", "Build:win", "Build:cli-sources"):
            assert ".build_settings" in ci[job]["extends"], job


class TestChecks:
    def test_linters_match_the_project(self, raw):
        """Проект перешёл на ruff; black и isort больше не в зависимостях."""
        assert "ruff check" in raw
        assert "black" not in raw
        assert "isort" not in raw

    def test_tests_are_not_allowed_to_fail(self, ci):
        """Собирать релиз с красными тестами незачем."""
        assert ci["pytest"].get("allow_failure") is not True
        assert ".check_rules" not in ci["pytest"].get("extends", [])

    def test_tests_run_for_release_too(self, ci):
        rules = str(ci[".test_rules"]["rules"])
        assert "release" in rules and "ci info" in rules

    def test_build_waits_for_tests(self, ci):
        for job in ("Build:debian", "Build:win", "Build:cli-sources"):
            assert "pytest" in ci[job]["needs"], job

    def test_compatibility_guard_is_in_pipeline(self, raw):
        assert "vermin -t=3.10" in raw


class TestArtifacts:
    def test_builds_use_repo_spec_files(self, raw):
        """Те же рецепты, что у локальной сборки, — иначе сборки разъедутся."""
        assert "pyinstaller nomos_cli.spec" in raw
        assert "pyinstaller nomos.spec" in raw
        assert (ROOT / "nomos_cli.spec").is_file()

    def test_configs_ship_next_to_the_binary(self, raw):
        assert "cp -r configs build/dist/configs" in raw
        assert "Copy-Item -Path .\\configs\\*" in raw

    def test_secret_is_stripped_from_both_artifacts(self, raw):
        """configs/.config.env — доступ к стенду, в артефакт он не попадает."""
        assert "rm -f build/dist/configs/.config.env" in raw
        assert "Remove-Item .\\build\\dist\\configs\\.config.env" in raw

    def test_windows_version_resource_is_passed_through_env(self, raw):
        """--version-file нельзя дать вместе со .spec, только переменной."""
        assert "NOMOS_VERSION_FILE" in raw
        spec = (ROOT / "nomos_cli.spec").read_text(encoding="utf-8")
        assert 'os.environ.get("NOMOS_VERSION_FILE")' in spec
        assert "version=VERSION_FILE" in spec

    def test_source_distribution_is_published(self, raw):
        assert "tools/make_cli_dist.py" in raw
        assert (ROOT / "tools" / "make_cli_dist.py").is_file()

    def test_upload_uses_crossbuilder_as_before(self, raw):
        assert raw.count("crossbuilder run PackAndUpload") >= 2
        assert raw.count("run UploadArtifacts") >= 2


class TestReferencedFiles:
    @pytest.mark.parametrize(
        "name",
        ["VERSION", "requirements.txt", "requirements_build.txt",
         "version_info.txt", "version_info_debian.txt", "README_SHORT.txt"],
    )
    def test_file_exists(self, name):
        assert (ROOT / name).is_file(), f"{name} упоминается в CI, но его нет в репозитории"

    def test_version_is_parseable(self):
        version = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
        parts = version.split(".")
        assert len(parts) == 3 and all(part.isdigit() for part in parts), version

    def test_build_requirements_include_pyinstaller(self):
        text = (ROOT / "requirements_build.txt").read_text(encoding="utf-8")
        assert "pyinstaller" in text
        assert "-r requirements.txt" in text

    def test_version_templates_have_placeholders(self):
        for name in ("version_info.txt", "version_info_debian.txt"):
            text = (ROOT / name).read_text(encoding="utf-8")
            assert "{{BUILD_VERSION}}" in text, name
