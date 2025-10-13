import logging
from multiprocessing.context import AuthenticationError

import requests
import time
import re
import urllib3
from requests import Request
from .settings_checker import Settings

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MPXAuthenticator:
    cookies: requests.session().cookies
    headers: requests.session().headers
    session: requests.session

    def __init__(self, logger):
        self.logger: logging.Logger = logger

    def authenticate(self, settings: Settings):
        if settings.personal_token:
            self.logger.info(f"Use personal_token for host: {settings.mpx_host}")
            if not self._check_mc_token(settings):
                raise AuthenticationError("Проверьте personal_token, адрес сервера, помолитесь Омниссии")
            else:
                self.logger.info("Successfully authenticated")
                return True
        elif settings.mpx_secret:
            self.logger.info(f"Use creds and client secret  for host: {settings.mpx_host}")
            if not self._get_token(settings):
                raise AuthenticationError("Проверьте правильность ввода адреса сервера, логина, пароля и ClientSecret")
            else:
                self.logger.info("Successfully authenticated")
                return True
        else:
            self.logger.info(f"Use creds for host: {settings.mpx_host}")
            if not self._get_token_ui(settings):
                raise AuthenticationError("Проверьте правильность ввода адреса сервера, логина, пароля")
            else:
                self.logger.info("Successfully authenticated")
                return True

    def _check_mc_token(self, settings: Settings):
        self.cookies = {}
        self.headers = {'Authorization': f'Bearer {settings.personal_token.get_secret_value()}',
                        'Content-Type': 'application/json'}
        connected = False
        url = f'https://{settings.mpx_host}/api/scopes/v2/scopes'
        for try_number in range(settings.reconnect_times):
            self.logger.info(f"try connect to: {settings.mpx_host}, attempt: {try_number + 1} "
                             f"of {settings.reconnect_times}")
            try:
                scopes = requests.get(url, headers=self.headers, cookies=self.cookies, verify=False)
                if scopes.status_code == 200 and scopes.json()[0]["id"]:
                    connected = True
                    break
                elif scopes.status_code == 401:
                    self.logger.error("Personal Token expired")
                    exit(1)
                else:
                    raise requests.exceptions.ConnectionError(f"Code {scopes.status_code}, Content: {scopes.content}")
            except Exception as Err:
                self.logger.warning(f"Проблемы при проверке токена: {Err}")
                time.sleep(5)
        return connected

    def _get_token(self, settings: Settings):
        connected = False
        url = f'https://{settings.mpx_host}:3334/connect/token'
        self.cookies = {}
        for try_number in range(settings.reconnect_times):
            self.logger.info(f"try connect to: {settings.mpx_host}, attempt: {try_number + 1} "
                             f"of {settings.reconnect_times}")
            try:
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
                auth_data = {
                    'client_id': 'mpx',
                    'client_secret': settings.mpx_secret.get_secret_value(),
                    'grant_type': 'password',
                    'response_type': 'code id_token',
                    'scope': 'authorization offline_access mpx.api idmgr.api ptkb.api',
                    'username': settings.login,
                    'password': settings.password.get_secret_value(),
                }
                if auth_data["username"].find("@") != -1:
                    auth_data.update({"amr": "ldap"})
                r = requests.post(url, data=auth_data, headers=headers, verify=False)
                access_token = r.json()["access_token"]
                self.headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
                connected = True
                break
            except Exception as Err:
                self.logger.warning(f"Проблемы при получении токена: {Err}")
        return connected

    def _get_token_ui(self, settings):
        for try_number in range(settings.reconnect_times):
            try:
                self.logger.info(f"try take first response to: {settings.mpx_host}, attempt: {try_number + 1} "
                                 f"of {settings.reconnect_times}")
                session = requests.Session()
                url = 'https://{}:3334/ui/login'.format(settings.mpx_host)
                authType = 1 if settings.login.find("@") != -1 else 0
                auth_data = {
                    'username': settings.login,
                    'password': settings.password.get_secret_value(),
                    'authType': authType
                }
                response = session.post(url, json=auth_data, verify=False)
                if response.status_code == 400:
                    self.logger.error("400 Client Error. Check creds.")
                    exit(1)

                response.raise_for_status()  # Проверка на другие HTTP ошибки
                self.logger.info("Step 1 - successful")
            except Exception as Err:
                self.logger.warning(f"Auth error (Step 1): {Err}")
                continue
            try:
                url_auth = 'https://{}:443/account/login'.format(settings.mpx_host)
                param_auth = {"returnUrl": "/#/authorization/landing"}
                response = session.get(url_auth, json=param_auth, verify=False)
                if response.status_code == 503:  # Обработка ошибки 503
                    self.logger.warning(f"Step 2 - 503 Service Unavailable during account login.")
                    continue
                response.raise_for_status()
                self.logger.info("Step 2 - Accessing account successful")
            except requests.exceptions.HTTPError as Err:
                self.logger.warning(f"Auth error (Step 2): {Err}")
                continue
            form_action_match = re.search(r'action=["\']([^"\']*)["\']', response.text)
            if not form_action_match:
                self.logger.warning("No form action found.")
            form_action = form_action_match.group(1)
            form_data = {}
            for item in re.finditer(r'name=["\']([^"\']*)["\'] value=["\']([^"\']*)["\']', response.text):
                form_data[item.group(1)] = item.group(2)
            response = session.post(form_action, data=form_data, verify=False)
            if response.ok:
                self.logger.info("Step 3 - Final auth successful")
                self.headers = session.headers
                self.cookies = session.cookies
                self.session = session
                return True
            else:
                self.logger.warning(f"Final auth error: {response.status_code} - {response.text}")
        return False




def check_mc_token(settings):
    connected = False
    url = 'https://{}/api/scopes/v2/scopes'.format(settings["HOST"])
    for i in range(settings["RECONNECT"]):
        print("try connect to:", settings["HOST"] + ",", "attempt:", i + 1, "of", settings["RECONNECT"])
        try:
            scopes = requests.get(url, headers=settings["authHeader"], verify=False)
            if scopes.status_code == 200 and scopes.json()[0]["id"]:
                connected = True
                break
            else:
                raise requests.exceptions.ConnectionError
        except Exception:
            print("Проблемы при проверке токена")
            time.sleep(5)
    return connected


def get_auth_header(settings):
    if "RECONNECT" not in settings.keys():
        settings["RECONNECT"] = 10
    if "MC_TOKEN" in settings.keys() and settings["MC_TOKEN"]:
        print("use MC_TOKEN for host:", settings["HOST"])
        settings["authHeader"] = {'Authorization': 'Bearer ' + settings["MC_TOKEN"], 'Content-Type': 'application/json'}
        if not check_mc_token(settings):
            print("Проверьте токен, адрес сервера, помолитесь Омниссии")
            exit(1)
        return settings
    elif "CLIENT_SECRET" in settings.keys() and settings["CLIENT_SECRET"]:
        print("use creds and client secret for host:", settings["HOST"])
        authHeader = 1
        for i in range(settings["RECONNECT"]):
            print("try connect to:", settings["HOST"] + ",", "attempt:", i+1, "of", settings["RECONNECT"])
            authHeader = get_token(settings)
            if authHeader != 1:
                settings["authHeader"] = authHeader
                return settings
            else:
                time.sleep(5)
        if authHeader == 1:
            exit(1)
    else:
        print("use creds for host:", settings["HOST"])
        for i in range(settings["RECONNECT"]):
            session = get_token_ui(settings)
            if session is not None:
                settings["session"] = session
                return settings
            else:
                time.sleep(5)
        exit(1)


def get_token(connect_settings):
    try:
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        auth_data = {
            'client_id': 'mpx',
            'client_secret': connect_settings["CLIENT_SECRET"],
            'grant_type': 'password',
            'response_type': 'code id_token',
            'scope': 'authorization offline_access mpx.api idmgr.api ptkb.api',
            'username': connect_settings["LOGIN"],
            'password': connect_settings["PASSWORD"]
        }
        if connect_settings["LOGIN"].find("@") != -1:
            auth_data.update({"amr": "ldap"})
        url = 'https://{}:3334/connect/token'.format(connect_settings["HOST"])
        r = requests.post(url, data=auth_data, headers=headers, verify=False)
        access_token = r.json()["access_token"]
        authHeader = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/json'}
        print("Токен получен")
        return authHeader
    except Exception:
        print("Токен не получен")
        return 1


def get_token_ui(connect_settings):
    try:
        session = requests.Session()
        url = 'https://{}:3334/ui/login'.format(connect_settings["HOST"])
        authType = 1 if connect_settings["LOGIN"].find("@") != -1 else 0
        auth_data = {
            'username': connect_settings["LOGIN"],
            'password': connect_settings["PASSWORD"],
            'authType': authType
        }
        response = session.post(url, json=auth_data, verify=False)
        response.raise_for_status()  # Проверка на другие HTTP ошибки
    except Exception:
        print("Токен не получен")
        return 1
    try:
        url_auth = 'https://{}:443/account/login'.format(connect_settings["HOST"])
        param_auth = {"returnUrl": "/#/authorization/landing"}
        response = session.get(url_auth, json=param_auth, verify=False)

        if response.status_code == 503:  # Обработка ошибки 503
            print("503 Service Unavailable during account login.")
            return 1

        response.raise_for_status()
        print("Step 2 - Accessing account successful")
    except requests.exceptions.HTTPError as error:
        print("Auth error (Step 2)")
        return 1
    form_action_match = re.search(r'action=["\']([^"\']*)["\']', response.text)
    if not form_action_match:
        print("No form action found.")
        return None

    form_action = form_action_match.group(1)
    form_data = {}
    for item in re.finditer(r'name=["\']([^"\']*)["\'] value=["\']([^"\']*)["\']', response.text):
        form_data[item.group(1)] = item.group(2)

    response = session.post(form_action, data=form_data, verify=False)
    if response.ok:
        print("Step 3 - Final auth successful")
        return session
    else:
        print(f"Final auth error: {response.status_code} - {response.text}")
        return None
