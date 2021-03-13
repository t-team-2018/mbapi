from datetime import datetime, timedelta


import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOGIN_EXPIRE = timedelta(minutes=10)


class MBApiBase():
    def __init__(self, user, passwd, business_number, user_id):
        self._r_session = self._make_request_session()
        self.user = user
        self.passwd = passwd
        self.business_number = business_number
        self.user_id = user_id
        self._datetime = None
        self.login_error_times = 0

    def _make_request_session(self):
        r_session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=(502, 504))
        http_adapter = HTTPAdapter(pool_connections=20, pool_maxsize=50, max_retries=retries)
        r_session.mount("http://", http_adapter)
        r_session.mount("https://", http_adapter)
        return r_session

    @property
    def r_session(self):
        if self._datetime is None or datetime.now() - self._datetime > LOGIN_EXPIRE:
            self._check_login()
        self._datetime = datetime.now()
        return self._r_session

    def _check_login_invalid(self, message):
        """
        "登录信息已超时"为马帮接口登录失效返回信息
        "请重新登录"为镖局接口登录失效返回信息
        """
        words = ["登录信息已超时", "请重新登录"]
        return any(word in message for word in words)

    def _check_login(self):
        raise NotImplementedError

    def request(self, url, method, **kw):
        raise NotImplementedError
