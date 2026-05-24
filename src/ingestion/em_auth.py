"""EastMoney NID auth - monkey-patch requests for EastMoney domains."""
import logging
import random
import re
import time
import requests

logger = logging.getLogger(__name__)

_NID_CACHE: dict = {"nid": None, "expires": 0}


def _fetch_nid() -> str:
    """Get a fresh NID token from EastMoney."""
    url = "https://anonflow2.eastmoney.com/backend/api/webreport"
    data = {
        "deviceType": "web",
        "browser": "Chrome",
        "os": "Windows",
        "screen": "1920x1080",
        "canvasKey": hex(random.getrandbits(64)),
        "webglKey": hex(random.getrandbits(64)),
        "fontKey": hex(random.getrandbits(64)),
        "audioKey": hex(random.getrandbits(64)),
    }
    try:
        r = requests.post(url, json=data, timeout=10)
        for c in r.cookies:
            if c.name == "nid":
                return c.value
    except Exception as e:
        logger.debug("NID fetch: %s", e)
    return ""


def _get_nid() -> str:
    now = time.time()
    if now > _NID_CACHE["expires"]:
        nid = _fetch_nid()
        if nid:
            _NID_CACHE["nid"] = nid
            _NID_CACHE["expires"] = now + 20
    return _NID_CACHE["nid"] or ""


_EASTMONEY_DOMAINS = [
    "eastmoney.com",
    "datacenter-web.eastmoney.com", "reportapi.eastmoney.com",
    "search-api-web.eastmoney.com", "np-weblist.eastmoney.com",
    "anonflow2.eastmoney.com",
]
# push2 不需要 NID，且海外 IP 可能导致 NID 请求超时阻塞数据请求
_PUSH2_DOMAINS = ["push2.eastmoney.com"]
_ORIGINAL_REQUEST = requests.Session.request


def _patched_request(self, method, url, *args, **kwargs):
    if isinstance(url, str):
        # push2 — 只需 User-Agent + 超时，不需要 NID
        if any(d in url for d in _PUSH2_DOMAINS):
            headers = kwargs.get("headers", {})
            headers.setdefault("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            kwargs["headers"] = headers
            kwargs.setdefault("timeout", 15)
        # 其他东财域 — 需要 NID 鉴权
        elif any(d in url for d in _EASTMONEY_DOMAINS):
            headers = kwargs.get("headers", {})
            nid = _get_nid()
            if nid:
                headers["Cookie"] = f"nid={nid}"
            headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            kwargs["headers"] = headers
            kwargs.setdefault("timeout", 15)
    return _ORIGINAL_REQUEST(self, method, url, *args, **kwargs)


def patch_requests_session():
    """Monkey-patch requests.Session.request to inject NID for EastMoney."""
    if not getattr(requests.Session, "_patched", False):
        requests.Session.request = _patched_request
        requests.Session._patched = True
        logger.info("EastMoney NID auth patch applied")
