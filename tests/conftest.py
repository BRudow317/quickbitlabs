from __future__ import annotations

import os

# Seed the minimum required env vars before any test module is collected.
# settings.py instantiates Settings() at module level, so these must be present
# before the first import.  Individual tests that exercise missing-var behavior
# use monkeypatch to remove them within the test scope, which is cleaned up
# automatically after each test.
os.environ.setdefault("JWT_SECRET", "_test_jwt_secret_")
os.environ.setdefault("UPLOAD_ENCRYPTION_KEY", "_test_upload_key_")
