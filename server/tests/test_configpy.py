""" tests for config.py """
import tempfile
import unittest
from pathlib import Path

from mypy_util import config


class ConfigTestCase(unittest.TestCase):
    def setUp(self):
        self.original_secrets = getattr(config, "_secrets", None)
        config._secrets = {}

    def tearDown(self):
        config._secrets = self.original_secrets

    def test_get_secret_returns_value_for_group_and_key(self):
        config._secrets = {"service": {"token": "abc123"}}

        result = config.get_secret("SERVICE", "TOKEN")

        self.assertEqual(result, "abc123")

    def test_get_secret_reads_content_when_value_is_path(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write("payload-data")
            temp_path = tmp.name

        try:
            config._secrets = {"files": {"payload": temp_path}}

            result = config.get_secret("FILES", "PAYLOAD")

            self.assertEqual(result, "payload-data")
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_all_group_key_pairs_return_expected_values(self):
        # Ensure every configured group/key returns the correct secret
        config._secrets = {
            "db": {"user": "admin", "pass": "s3cr3t"},
            "api": {"key": "XYZ-999"},
        }

        cases = [
            ("DB", "USER", "admin"),
            ("db", "pass", "s3cr3t"),
            ("API", "KEY", "XYZ-999"),
        ]

        for group, key, expected in cases:
            with self.subTest(group=group, key=key):
                self.assertEqual(config.get_secret(group, key), expected)


if __name__ == "__main__":
    unittest.main()
