"""Thin client for authenticating against the Superset REST API.

Shared by the seed script (simulates dashboard traffic) and the ETL script (pulls
from /api/v1/log/), so both auth the same way exactly once.
"""
import os

import requests


class SupersetClient:
    def __init__(self, base_url=None, username=None, password=None):
        self.base_url = (base_url or os.environ["SUPERSET_BASE_URL"]).rstrip("/")
        self.username = username or os.environ["SUPERSET_ADMIN_USERNAME"]
        self.password = password or os.environ["SUPERSET_ADMIN_PASSWORD"]
        self.session = requests.Session()
        self.access_token = None
        self._login()

    def _login(self):
        resp = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json={
                "username": self.username,
                "password": self.password,
                "provider": "db",
                "refresh": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        self.access_token = resp.json()["access_token"]
        self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})

        csrf_resp = self.session.get(
            f"{self.base_url}/api/v1/security/csrf_token/", timeout=30
        )
        csrf_resp.raise_for_status()
        csrf_token = csrf_resp.json()["result"]
        self.session.headers.update(
            {"X-CSRFToken": csrf_token, "Referer": self.base_url}
        )

    def get(self, path, timeout=60, **kwargs):
        resp = self.session.get(f"{self.base_url}{path}", timeout=timeout, **kwargs)
        resp.raise_for_status()
        return resp

    def post(self, path, timeout=60, **kwargs):
        resp = self.session.post(f"{self.base_url}{path}", timeout=timeout, **kwargs)
        resp.raise_for_status()
        return resp

    def delete(self, path, timeout=60, **kwargs):
        resp = self.session.delete(f"{self.base_url}{path}", timeout=timeout, **kwargs)
        resp.raise_for_status()
        return resp
