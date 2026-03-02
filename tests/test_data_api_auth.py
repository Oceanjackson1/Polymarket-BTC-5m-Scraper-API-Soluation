from __future__ import annotations

import json
import tempfile
from pathlib import Path
import unittest

from fastapi.testclient import TestClient

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.data_api import create_app  # noqa: E402


def write_keyset_index(publish_dir: Path) -> None:
    path = publish_dir / "meta" / "keysets" / "index.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"datasets": []}), encoding="utf-8")


class DataApiAuthTests(unittest.TestCase):
    def test_bearer_mode_requires_token_for_v1(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            publish_dir = Path(tmpdir)
            write_keyset_index(publish_dir)
            app = create_app(
                publish_dir=publish_dir,
                bearer_token="secret-token",
                auth_mode="bearer",
            )
            client = TestClient(app)

            health = client.get("/health")
            self.assertEqual(200, health.status_code)
            self.assertEqual("bearer", health.json()["auth_mode"])

            unauthenticated = client.get("/v1/keysets/index")
            self.assertEqual(401, unauthenticated.status_code)

            authenticated = client.get(
                "/v1/keysets/index",
                headers={"Authorization": "Bearer secret-token"},
            )
            self.assertEqual(200, authenticated.status_code)
            self.assertEqual({"datasets": []}, authenticated.json())

    def test_none_mode_allows_private_network_only_access(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            publish_dir = Path(tmpdir)
            write_keyset_index(publish_dir)
            app = create_app(
                publish_dir=publish_dir,
                bearer_token="",
                auth_mode="none",
            )
            client = TestClient(app)

            health = client.get("/health")
            self.assertEqual(200, health.status_code)
            self.assertEqual("none", health.json()["auth_mode"])

            response = client.get("/v1/keysets/index")
            self.assertEqual(200, response.status_code)
            self.assertEqual({"datasets": []}, response.json())

    def test_bearer_mode_without_token_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            publish_dir = Path(tmpdir)
            write_keyset_index(publish_dir)
            with self.assertRaisesRegex(ValueError, "bearer token"):
                create_app(
                    publish_dir=publish_dir,
                    bearer_token="",
                    auth_mode="bearer",
                )


if __name__ == "__main__":
    unittest.main()
