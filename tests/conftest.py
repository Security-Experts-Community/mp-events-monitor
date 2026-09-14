"""Общая настройка тестов.

В CLI-сборке (без nomos/web) файлы с тестами веба не собираются: их
импорт требует fastapi, которого в CLI-поставке нет.
"""

from webcheck import WEB_PRESENT

WEB_ONLY_TESTS = [
    "test_frontend_static.py",
    "test_filter_reports.py",
    "test_kb_visibility.py",
    "test_pause_and_incremental.py",
    "test_run_config.py",
    "test_storage_and_api.py",
]

collect_ignore = [] if WEB_PRESENT else list(WEB_ONLY_TESTS)
