# Сборка консольной поставки Nomos: venv -> зависимости -> exe -> архив
#
#   powershell -ExecutionPolicy Bypass -File build_cli.ps1
#   powershell -ExecutionPolicy Bypass -File build_cli.ps1 --no-test
#
# Всё делается в локальном .venv рядом со скриптом: глобальный Python не
# трогаем, и состав exe не зависит от того, что у вас установлено.

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Русский текст в консоли и в выводе дочерних процессов
try { [Console]::OutputEncoding = [Text.Encoding]::UTF8 } catch {}
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"

function Find-Python {
    # py -3.12 надёжнее, чем python из PATH: там может оказаться алиас Store
    foreach ($candidate in @("3.12", "3.13", "3.11", "3.10")) {
        & py -$candidate --version *> $null
        if ($LASTEXITCODE -eq 0) { return @("py", "-$candidate") }
    }
    $found = Get-Command python -ErrorAction SilentlyContinue
    if ($found) { return @("python") }
    throw "Python не найден. Поставьте Python 3.12 с python.org, отметив 'Add python.exe to PATH'"
}

$python = Find-Python
Write-Host "Интерпретатор: $($python -join ' ')" -ForegroundColor Cyan

if (-not (Test-Path ".venv")) {
    Write-Host "Создаю .venv" -ForegroundColor Cyan
    & $python[0] $python[1..($python.Length - 1)] -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "не удалось создать виртуальное окружение" }
}

$venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) { throw "не найден $venvPython" }

Write-Host "Ставлю зависимости" -ForegroundColor Cyan
& $venvPython -m pip install --upgrade pip --quiet
& $venvPython -m pip install -r requirements.txt --quiet
# Сборщик прогоняет тесты, значит нужен pytest и остальное из [dev]
& $venvPython -m pip install -e ".[dev]" --quiet
& $venvPython -m pip install pyinstaller --quiet
if ($LASTEXITCODE -ne 0) { throw "не удалось установить зависимости" }

Write-Host "Собираю поставку" -ForegroundColor Cyan
& $venvPython tools\make_cli_release.py @args
if ($LASTEXITCODE -ne 0) { throw "сборка не удалась" }

Write-Host ""
Write-Host "Архив лежит в dist\" -ForegroundColor Green
