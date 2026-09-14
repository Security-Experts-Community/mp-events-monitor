# Nomos CLI

Консольная поставка: без веб-интерфейса, только прогон и xlsx-отчёты.
Доменная логика, конфиги и отладочная обвязка — те же, что в полной
версии.

## Установка

Нужен **Python 3.10 или новее** — верхней границы нет, подойдёт тот, что
уже стоит на машине. Проверены 3.10, 3.12 и 3.14. На более свежих Nomos
предупредит, но запустится; если установка пакетов там упадёт (у
pydantic-core может не быть колёс), он подскажет, что делать.

```powershell
py -3.12 -m pip install -r requirements.txt
copy configs\example.config.env configs\.config.env   :: заполнить HOST и токен
```

Зависимостей меньше, чем в полной версии: FastAPI и uvicorn не нужны.

Если `pip` не находится — вызывайте через интерпретатор
(`py -3.12 -m pip`). Если pip в самой установке Python сломан
(`No module named 'pip._vendor...'`) — `py -3.12 -m ensurepip --upgrade`.

## Запуск

```bash
python Nomos.py                                  # режим по умолчанию
python Nomos.py mode=Assets_filters time_delta_hours=168
python Nomos.py --help                           # все параметры
```

Отчёты — в `out/<папка прогона>/*.xlsx`, журналы — в `logs/`.

## Отладка

Те же флаги, что и в полной версии:

```bash
python Nomos.py debug_dump=true          # сырые ответы API в debug/
python Nomos.py record_fixtures=true     # запись фикстур для offline-регрессии
python Nomos.py dump_queries=true        # план запросов + out/query_fixtures.json
python Nomos.py logging_level=DEBUG      # трассировка HTTP
```

Диагностический бандл для разбора: `python tools/make_debug_bundle.py`.

## Тесты

```bash
python -m pytest tests -q
```

Тесты веб-интерфейса в эту поставку не входят; остальные — те же.

## Единый exe одной командой

```powershell
powershell -ExecutionPolicy Bypass -File build_cli.ps1
```

или в Git Bash:

```bash
./build_cli.sh
```

Скрипт сам создаст `.venv`, поставит зависимости и PyInstaller, прогонит
тесты и соберёт `dist\NomosCLI_<дата>.zip`: внутри `NomosCLI.exe` и
папка `configs\` рядом с ним. Python на целевой машине не нужен,
остаётся создать `configs\.config.env` из `example.config.env`.

Ключи передаются насквозь: `./build_cli.sh --no-test`,
`--out ФАЙЛ`. Собирать нужно на Windows — PyInstaller не
кросс-компилирует.
