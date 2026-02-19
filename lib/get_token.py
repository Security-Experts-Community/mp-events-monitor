import logging
import re
import time
from collections import deque
from datetime import datetime
from typing import Any

import backoff
import requests
import urllib3
from pydantic import BaseModel

try:
    from .settings_checker import Settings
except:
    from settings_checker import Settings


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class TokenInfo(BaseModel):
    id: str
    name: str
    description: str = None
    lastUsage: datetime
    expirationDate: datetime


class MPXAuthenticator:
    cookies: requests.session().cookies
    headers: requests.session().headers
    session: requests.session
    token_info: TokenInfo | None

    def __init__(self, logger):
        self.logger: logging.Logger = logger
        self.token_info = None
        self.privileges: list[str] = []
        self.cookies = {}
        self.headers = {}
        self.auth_mode = ""

    def authenticate(self, settings: Settings):
        if settings.personal_token:
            self.auth_mode = "pat"
            self.logger.info(f"Use personal_token for host: {settings.mpx_host}")
            if not self._check_mc_token(settings):
                raise ConnectionError(
                    "Проверьте personal_token, адрес сервера, помолитесь Омниссии"
                )
            else:
                self.logger.info("Successfully authenticated")
                if settings.check_privileges:
                    self.get_token_info(settings)
        elif settings.mpx_secret:
            self.auth_mode = "mpx_secret"
            self.logger.info(
                f"Use creds and client secret  for host: {settings.mpx_host}"
            )
            if not self._get_token(settings):
                raise ConnectionError(
                    "Проверьте правильность ввода адреса сервера, логина, пароля и ClientSecret"
                )
            else:
                self.logger.info("Successfully authenticated")
        else:
            self.auth_mode = "user"
            self.logger.info(f"Use creds for host: {settings.mpx_host}")
            self._get_token_ui(settings)
            self.logger.info("Successfully authenticated")
        if settings.check_privileges:
            if not self._check_privileges(settings):
                raise AttributeError(
                    "Not all needed privileges. Use new token or update Account. "
                    "If it's right to KnowledgeBase then disable kb_check_mode."
                )
        if self.auth_mode == "user":
            # куки от ПТКБ
            self.logger.info("Get KnowledgeBasePortalCookies cookie")
            mc_login_url = f"https://{settings.mpx_host}:8091/account/login"
            page_mc = {"returnUrl": f"https://{settings.mpx_host}:8091/"}
            self._mpx_cookies(mc_login_url, page_mc, "KnowledgeBasePortalCookies")
        return True

    def _check_privileges(self, settings: Settings):
        kb_privilgeges = {
            # "GetContentDatabaseTopRevision": "Получение номера последней ревизии для БД (PT KB Базы данных API)",
            "UiViewAnyContentDatabase": "UI просмотр содержимого любой БД (PT KB Базы данных UI)",
            "UiViewContentDatabase": "просмотр содержимого БД (PT KB Базы данных UI)",
            "kb.access.allow": "kb.access.allow (PT KB Другое)",
            # "GetLocales": "Получение списка локалей (PT KB Профиль)",
            "GetSiemData": "Просмотра данных SIEM (PT KB SIEM  API)",
            "UiGetReservedTaxons": "UI просмотр зарезервированных таксонов (PT KB SIEM UI)",
            "UiViewSiemData": "UI просмотр данных SIEM (PT KB SIEM UI)",
            "UiViewTaxonParams": "UI просмотр параметров таксона (PT KB SIEM UI)",
        }
        common_privileges = {
            # "TM.Hierarchy.View": "Просмотр иерархии площадок (Management and Configuration Иерархия площадок)",
            # "MC.ApplicationLinks.View": "Просмотр правил репликации и связей для распределенного поиска (Management "
            #                            "and Configuration Связи приложений)",  # TODO ?
            "vulners": "Уязвимости (MaxPatrol 10 Активы)",
            "assets": "Создание, просмотр, изменение, удаление (MaxPatrol 10 Активы)",
            "access.allow": "Доступ к приложению (MaxPatrol 10 Общее)",
            # "events.monitoring.read": "Просмотр (MaxPatrol 10 Сбор данных Мониторинг источников)",
            # "events.monitoring": "Создание, просмотр, изменение, удаление (MaxPatrol 10 Сбор данных Мониторинг "
            #  "источников)",
            "infrastructure": "Инфраструктура (MaxPatrol 10 Сбор данных)",
            # "siem.correlation_rules.read": "Просмотр (MaxPatrol 10 Правила и табличные списки Правила корреляции)",
            # "siem.enrichment_rules.read": "Просмотр (MaxPatrol 10 Правила и табличные списки Правила обогащения)",
            # "siem.table_lists.read": "Просмотр (MaxPatrol 10 Правила и табличные списки Табличные списки)",
            # "access_rights": "Права доступа (MaxPatrol 10 Система)",
            # "events.read": "Просмотр (MaxPatrol 10 События)",
            "incidents.access_to_unlinked_incidents": "Доступ к непривязанным инцидентам (MaxPatrol 10 Инциденты)",
            "incidents": "Инциденты (MaxPatrol 10 Инциденты)",
        }
        if settings.kb_check_mode:
            for kb_privilege_code, kb_privilege_desc in kb_privilgeges.items():
                common_privileges.update({kb_privilege_code: kb_privilege_desc})
        all_privileges = True
        for privilege_code, privilege_desc in common_privileges.items():
            if privilege_code not in self.privileges:
                self.logger.error(
                    f"Have no privilege with code: {privilege_code}. Description: {privilege_desc}"
                )
                all_privileges = False
        return all_privileges

    def _check_mc_token(self, settings: Settings):
        self.cookies = {}
        self.headers = {
            "Authorization": f"Bearer {settings.personal_token.get_secret_value()}",
            "Content-Type": "application/json",
        }
        connected = False
        url = f"https://{settings.mpx_host}/api/scopes/v2/scopes"
        for try_number in range(settings.reconnect_times):
            self.logger.info(
                f"try connect to: {settings.mpx_host}, attempt: {try_number + 1} "
                f"of {settings.reconnect_times}"
            )
            try:
                self.session = requests.session()
                scopes = self.session.get(
                    url, headers=self.headers, cookies=self.cookies, verify=False
                )
                if scopes.status_code == 200 and scopes.json()[0]["id"]:
                    connected = True
                    break
                elif scopes.status_code == 401:
                    self.logger.error("Personal Token expired")
                    exit(1)
                else:
                    raise requests.exceptions.ConnectionError(
                        f"Code {scopes.status_code}, Content: {scopes.content}"
                    )
            except Exception as Err:
                self.logger.warning(f"Проблемы при проверке токена: {Err}")
                time.sleep(5)
        return connected

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=3,
        raise_on_giveup=True,
        logger="MaxPatrolEventsMonitor:_get_requester",
    )
    def _get_requester_pat(self, url):
        response = self.session.get(
            url, verify=False, headers=self.headers, cookies=self.cookies
        )
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(
        backoff.expo,
        (ValueError, KeyError),
        max_tries=1,
        raise_on_giveup=True,
        logger="MaxPatrolEventsMonitor:get_token_info",
    )
    def get_token_info(self, settings: Settings):
        self.logger.info("Check privileges")
        url = f"https://{settings.mpx_host}:3334/api/iam/v1/personal_access_tokens"
        tokens_json = self._get_requester_pat(url)["items"]
        for token in tokens_json:
            token = TokenInfo(**token)
            if not self.token_info or token.lastUsage > self.token_info.lastUsage:
                self.token_info = token
        if not self.token_info:
            raise ValueError("no last token, but have. Shit.")
        url = f"https://{settings.mpx_host}:3334/api/iam/v1/personal_access_tokens/{self.token_info.id}"
        full_info_p = self._get_requester_pat(url)
        tree_privileges = full_info_p["applicationsPrivileges"]
        for app in tree_privileges:
            self.privileges.extend(app["privileges"])

    def _get_token(self, settings: Settings):
        """
        Важный нюанс для данного метода аутентификации не нужно проверять роли.
        Он deprecated, именно потому что ClientSecret подписывает ультимативный токен, на котором права root
        """
        connected = False
        url = f"https://{settings.mpx_host}:3334/connect/token"
        self.cookies = {}
        for try_number in range(settings.reconnect_times):
            self.logger.info(
                f"try connect to: {settings.mpx_host}, attempt: {try_number + 1} "
                f"of {settings.reconnect_times}"
            )
            try:
                headers = {"Content-Type": "application/x-www-form-urlencoded"}
                auth_data = {
                    "client_id": "mpx",
                    "client_secret": settings.mpx_secret.get_secret_value(),
                    "grant_type": "password",
                    "response_type": "code id_token",
                    "scope": "authorization offline_access mpx.api idmgr.api ptkb.api",
                    "username": settings.login,
                    "password": settings.password.get_secret_value(),
                }
                if auth_data["username"].find("@") != -1:
                    auth_data.update({"amr": "ldap"})
                r = requests.post(url, data=auth_data, headers=headers, verify=False)
                access_token = r.json()["access_token"]
                self.headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                }
                connected = True
                break
            except Exception as Err:
                self.logger.warning(f"Проблемы при получении токена: {Err}")
        return connected

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=3,
        raise_on_giveup=True,
        logger="MaxPatrolEventsMonitor:_get_requester",
    )
    def _requester_ui(
        self, url, body: dict = None, data: dict = None, post_method: bool = False
    ):
        if post_method:
            response = self.session.post(url, verify=False, json=body, data=data)
        else:
            response = self.session.get(url, verify=False, json=body)
        if response.status_code == 400:
            self.logger.error("400 Client Error. Check creds.")
            exit(1)
        response.raise_for_status()
        return response

    def _ui_login(self, settings: Settings):
        self.logger.info(f"try take first response to: {settings.mpx_host}")
        self.session = requests.Session()
        url = f"https://{settings.mpx_host}:3334/ui/login"
        auth_type = 1 if settings.login.find("@") != -1 else 0
        auth_data = {
            "username": settings.login,
            "password": settings.password.get_secret_value(),
            "authType": auth_type,
        }
        self._requester_ui(url, auth_data, {}, True)
        self.logger.info("Step check creds - successful")

    @backoff.on_exception(
        backoff.expo,
        (AttributeError, ConnectionError),
        max_tries=3,
        raise_on_giveup=True,
        logger="MaxPatrolEventsMonitor:_mpx_cookies",
    )
    def _mpx_cookies(self, url_auth, param_auth, mode):
        response = self._requester_ui(url_auth, param_auth)
        if response.ok:
            self.logger.info(
                f"Step Get redirects for {mode} - Accessing account successful"
            )
        else:
            raise ConnectionError(f"Step Get redirects not ok for {mode}")
        form_action, form_data = _form_data_handler(response)
        if not form_action:
            raise AttributeError(
                f"Step Get redirects not ok for {mode}. No form_action"
            )
        response = self._requester_ui(form_action, {}, form_data, True)
        if response.ok:
            self.logger.info(f"Step Get {mode} cookies - successful")
        else:
            raise ConnectionError(f"Step Get {mode} cookies - error")
        self.headers = self.session.headers
        self.cookies = self.session.cookies

    def _privileges_unpacker(self, privileges_info: list[dict[str, Any]]):
        queue: deque[dict[str, Any]] = deque()
        for app in privileges_info:
            if app["privileges"]:
                queue.extend(app["privileges"])
        while queue:
            privilege = queue.popleft()
            if privilege.get("code"):
                self.privileges.append(privilege["code"])
            else:
                if privilege.get("groups"):
                    queue.extend(privilege["groups"])
                if privilege.get("privileges"):
                    queue.extend(privilege["privileges"])

    def _get_token_ui(self, settings):
        self._ui_login(settings)
        url_auth = "https://{}:443/account/login".format(settings.mpx_host)
        param_auth = {"returnUrl": "/#/authorization/landing"}
        self._mpx_cookies(url_auth, param_auth, "MPX")
        if settings.check_privileges:
            self.logger.info("Get McPortalCookie cookie")
            mc_login_url = f"https://{settings.mpx_host}:3334/account/login"
            page_mc = {"returnUrl": f"https://{settings.mpx_host}:3334/mc"}
            self._mpx_cookies(mc_login_url, page_mc, "McPortalCookie")
            url = f"https://{settings.mpx_host}:3334/ptms/api/sso/v2/account"
            privileges_info = self._requester_ui(url).json()
            if not "privileges" in privileges_info.keys():
                self.logger.warning("no privileges")
            else:
                self._privileges_unpacker(privileges_info["privileges"])


def _form_data_handler(response: requests.Response):
    form_action_match = re.search(r'action=["\']([^"\']*)["\']', response.text)
    if not form_action_match:
        return False, False
    form_action = form_action_match.group(1)
    form_data = {}
    for item in re.finditer(
        r'name=["\']([^"\']*)["\'] value=["\']([^"\']*)["\']', response.text
    ):
        form_data[item.group(1)] = item.group(2)
    return form_action, form_data
