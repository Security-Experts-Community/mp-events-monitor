import logging
import shutil
import re
from pathlib import Path
from uuid import UUID
from pydantic import Field, SecretStr, model_validator, AliasChoices, FilePath
from pydantic_settings import BaseSettings, SettingsConfigDict
import time
import sys
from datetime import datetime, timedelta, date
old_python = False
if sys.version.find('3.7.') == 0:
    old_python = True
    from typing_extensions import Literal
    from typing import List as list, Optional, Union
    base_params = {}
else:
    from typing import Literal, Optional, Union
    base_params = {"cli_parse_args": True, "cli_prog_name": "python event_checker.py"}



class Settings(BaseSettings, **base_params):
    """
    Параметры запуска скрипта.
    Обязательные параметры для запуска:
    1. mpx_host
    2. Поля учетной записи, любой из трех вариантов:
        * personal_token - будет использоваться личный API токен (Bearer-токен)
        * login, password - будет использоваться учетная запись и cookies, а не Bearer-токен
        * login, password, mpx_secret - (устаревший метод начиная с 27.3) будет получен Bearer токен на основе секретов
                                        пользователя и MaxPatrol
        При указании всех 4 полей использоваться будет только personal_token, как наиболее правильный.
        Использование только login password хоть и реализовано, работает, но не рекомендуется.

    Параметры рекомендуется передавать через файл: `configs/.config.env`, так как при таком подходе парольно-кодовая
    информация не сохранится в логе запусков приложений ОС
    """
    logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", description="Уровень логирования",
        validation_alias=AliasChoices('l', "logging", "logging_level"))
    time_delta_hours: int = Field(default=168,
                                  validation_alias=AliasChoices('d','time_delta_hours', 'time_from_delta_hours'),
                                  description="Период анализа событий в часах. "
                                              "Дельта для расчета времени откуда начинать сбор")
    reconnect_times: int = Field(default=5, description="Количество попыток переподключения при ошибках "
                                                        "(интервал 5 сек)", ge=1, le=2166,
                                 validation_alias=AliasChoices('r', "reconnect_times"))
    max_uuids_in_siem_query: int = Field(
        default=1000,
        description="Количество event_src.asset в фильтрах по событиям. "
                    "Фактически пачками по сколько активов будут запрашивать данные по событиям.",
        ge=10, le=9999,
        validation_alias=AliasChoices('u', 'max_uuids', 'max_uuids_in_siem_query')
    )
    max_threads_for_siem_api: int = Field(
        default=11,
        validation_alias=AliasChoices('t', 'max_threads_for_siem_api', 'max_threads'),
        description="Количество потоков при запросе данных из SIEM.",
        ge=1, le=100)
    out_folder: Path = Field(default=Path("out"),
                             validation_alias=AliasChoices('o','out_folder', 'out_dir'),
                             description="Папка для результатов (абсолютный или относительный путь, "
                                         "в JSON используйте \\)")
    clear_mode: Literal[
        "full", "today", "day-1", "day-2", "not_clear"
    ] = Field(default="full", validation_alias=AliasChoices('c', 'clear', 'clear_mode'),
              description="Подходит только для режима работы Assets_filters (так как остальные создают только один "
                          "файл). Выбор режима очистки папки из out_folder. full - чистить всегда полностью, "
                          "today - очистить только отчеты созданные не сегодня и выполнить фильтры только для "
                          "недостающих, day-1 - оставить вчерашнее, day-2 - оставить позавчерашние, not_clear - "
                          "не чистить отчеты, создать недостающие.")
    mode: Literal[
        "Assets_filters", "ALL_events", "ALL_assets", "Dynamic_Groups_events", "Dynamic_Groups_assets", "Asset_IDs"
    ] = Field(default="Assets_filters", description="Режим работы скрипта (см. раздел \"Режимы работы\")")
    pdql_assets: str = Field(default="select(@Host, Host.@id as asset_id, Host.@audittime) | LIMIT(0)",
                             description="PDQL-запрос для режимов с активами (кроме Assets_filters)",
                             validation_alias=AliasChoices('pdql_assets', 'pdql'))
    event_policies: str = Field(default="w os ", validation_alias=AliasChoices("e", "event_policies"),
                                description="Регулярное выражение для политик сбора событий, для режимов работы, "
                                            "кроме Assets_filters")
    mpx_group: Union[str, list[str]] = Field(default="-1", validation_alias=AliasChoices('g','mpx_group'),
                                       description="Динамическая группа для запроса. Совместимо с режимами работы: "
                                       "ALL_events, ALL_assets. В остальных режимах игнорируется, так задается в "
                                       "управляющих файлах режима. На вход принимает значения UUID или '-1'")
    event_policies_file: FilePath = Field(
        default=Path("configs/event_policies.json"),
        validation_alias=AliasChoices('p', 'event_policies_file'),
        description="Путь к файлу с политиками событий"
    )
    asset_filters_file: FilePath = Field(
        default=Path("configs/assets_filters.json"),
        validation_alias=AliasChoices('a', 'asset_filters_file'),
        description="Путь к файлу с запросами к активам и описанием какие политики для этих активов надо проверить"
    )
    mpx_host: str = Field(description="FQDN MaxPatrol", validation_alias=AliasChoices('mpx_host', 'host'))
    personal_token: Optional[SecretStr] = Field(
        default=None, validation_alias=AliasChoices('personal_token', 'mc_token'),
        description="Личный токен для аутентификации в API. Лучший вариант для использования скрипта. "
                    "Инструкция по получению: 1. Слева в переключении между продуктами нажмите \"Management and Configuration\" "
                    "ИЛИ справа нажмите на значок профиля, а далее \"Профиль\"; 2. Слева внизу нажмите на значок "
                    "профиля; 3. Токены доступа; 4. Справа вверху \"Создать\"; 5. Заполните \"Имя\", в привилегиях "
                    "можно оставить только MaxPatrol 10 (MaxPatrol SIEM, MaxPatrol VM); 6. Нажмите \"Создать\"; "
                    "7. Скопируйте токен и вставьте в файл конфигурации скрипта.")
    login: Optional[str] = Field(default=None, description="Логин учетной записи в MaxPatrol")
    password: Optional[SecretStr] = Field(default=None, description="Пароль учетной записи в MaxPatrol")
    mpx_secret: Optional[SecretStr] = Field(
        default=None,
        validation_alias=AliasChoices('mpx_secret', 'client_secret'),
        description="Секрет MaxPatrol, получаемый согласно "
                    "https://doc.ptsecurity.com/ru-RU/projects/mp10/27.2/help/4598558347"
                    " устаревший параметр, начиная с 27.3")
    model_config = SettingsConfigDict(env_file=Path("configs/.config.env"), extra="ignore")

    @model_validator(mode="after")
    def validate_secrets(self):
        if self.personal_token:
            self.login = None
            self.password = None
            self.mpx_secret = None
        elif self.mpx_secret and self.login and self.password:
            pass
        elif self.login and self.password:
            pass
        else:
            raise ValueError("Негодник, ну-ка, иди сюда, нехороший человек, а? "
                             "Сдуру решил скрипт без чтения ридми запустить? Плут непутёвый, ну? "
                             "Ну, иди сюда, попробуй его запустить — он тебя сам запустит, балбес, умник, "
                             "да сколько можно! "
                             "Иди, несмышлёныш, читай ридми и вноси креды в .config.env!")
            # raise ValueError("Ошибка заполнения параметров. "
            #                  "personal_token ИЛИ login, password ИЛИ login, password, mpx_secret "
            #                  "должны быть заполнены. Прочитайте описание скрипта.")
        return self

    @model_validator(mode="after")
    def valid_group_and_folder_prepare(self):
        if not check_group_id(self.mpx_group, "in configs/.config.env mpx_group"):
            exit(1)
        logging.basicConfig(level=self.logging_level)
        logger = logging.getLogger("MaxPatrolEventsMonitor")
        if self.mode == "Assets_filters" and self.clear_mode != "full":
            if not self.out_folder.exists():
                self.out_folder.mkdir()
            elif self.clear_mode == "not_clear":
               pass
            else:
                date_dict = {"today": timedelta(days=0),
                             "day-1": timedelta(days=1),
                             "day-2": timedelta(days=2)}
                if self.clear_mode not in date_dict.keys():
                    logger.error(f"{self.clear_mode} not in default dict use today mode.")
                    self.clear_mode = "today"
                min_date = (date.today() - date_dict[self.clear_mode])
                logger.info(f"Preparing folder {self.out_folder.absolute()}. Delete all reports older then {min_date}")
                for excel_report in self.out_folder.glob("*.xlsx"):
                    create_find = re.search("^\\d{4}-\\d{2}-\\d{2}", excel_report.name)
                    if create_find:
                        create_find = create_find.group(0)
                        create_date = datetime.strptime(create_find, "%Y-%m-%d").date()
                        if create_date < min_date:
                            excel_report.unlink()
                logger.info("Remove all folders which have no excel report")
                for folder in self.out_folder.iterdir():
                    if folder.is_dir():
                        have_report = False
                        for _ in self.out_folder.glob(f'*-{folder.name}-*.xlsx'):
                            have_report = True
                        if not have_report:
                            if not folder_prepare(folder, 5, logger, False):
                                exit(1)
        else:
            if not folder_prepare(self.out_folder, self.reconnect_times, logger):
                exit(1)

def check_group_id(group_id, where, logger: Optional[logging.Logger] = None):
    if group_id != "-1":
        try:
            UUID(group_id, version=4)
            return True
        except ValueError:
            if not logger:
                logging.basicConfig(level=10)
                logger = logging.getLogger("MaxPatrolEventsMonitor")
            logger.error(f"{group_id} not a UUID, check {where}")
            return False
    else:
        return True

def folder_prepare(folder_path: Path, max_reties: int, logger: logging.Logger, need_create: bool = True):
    logger = logging.getLogger("MaxPatrolEventsMonitor")
    for retry_num in range(max_reties):
        try:
            if folder_path.exists():
                shutil.rmtree(folder_path)
            if need_create:
                logger.info(f"Create folder: {folder_path.absolute()}")
                folder_path.mkdir()
            return True
        except PermissionError as Err:
            logger.error(f"Error while \"{str(folder_path.absolute())} \" clear. Try number {retry_num + 1} of {max_reties} tries")
            logger.error(f"Error: {Err}.")
            logger.error("Close file in 10 seconds")
            time.sleep(10)
    logger.error(f"Can't clear folder {folder_path}")
    return False

def test():
    import sys
    argv = sys.argv
    try:
        sys.argv = ['event_checker.py', '--help']
        Settings()
    except SystemExit as e:
        print(e)
    try:
        sys.argv = argv
        my_set = Settings()
        print(my_set.model_dump_json(indent=4))

    except (Exception) as Err:
        print(Err)


if __name__ == "__main__":
    test()
