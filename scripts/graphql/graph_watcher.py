from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, Callable

import msal
import requests


GRAPH_ROOT = "https://graph.microsoft.com/v1.0"


@dataclass(frozen=True)
class ChangedItem:
    name: str
    item_id: str
    etag: str
    last_modified: str
    download_to: Callable[[Path], None]


class GraphAuth:
    def __init__(self, tenant_id: str, client_id: str, cache_path: Path) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.cache_path = cache_path
        self.scopes = ["Files.Read.All", "Sites.Read.All", "offline_access"]

        self.cache = msal.SerializableTokenCache()
        if self.cache_path.exists():
            self.cache.deserialize(self.cache_path.read_text(encoding="utf-8"))

        self.app = msal.PublicClientApplication(
            client_id=self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            token_cache=self.cache,
        )

    def _persist_cache(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(self.cache.serialize(), encoding="utf-8")

    def get_access_token(self) -> str:
        accounts = self.app.get_accounts()
        result = None
        if accounts:
            result = self.app.acquire_token_silent(self.scopes, account=accounts[0])

        if not result:
            flow = self.app.initiate_device_flow(scopes=self.scopes)
            if "user_code" not in flow:
                raise RuntimeError(f"Device flow init failed: {flow}")
            print(flow["message"])
            result = self.app.acquire_token_by_device_flow(flow)

        if "access_token" not in result:
            raise RuntimeError(f"Token error: {result}")

        self._persist_cache()
        return result["access_token"]


class GraphClient:
    def __init__(self, auth: GraphAuth) -> None:
        self.auth = auth
        self.log = logging.getLogger("GraphClient")

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.auth.get_access_token()}"}

    def get_json(self, url: str) -> Dict[str, Any]:
        r = requests.get(url, headers=self._headers(), timeout=60)
        r.raise_for_status()
        return r.json()

    def stream_download(self, url: str, out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = out_path.with_suffix(out_path.suffix + ".part")

        with requests.get(url, headers=self._headers(), stream=True, timeout=300) as r:
            r.raise_for_status()
            with tmp.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        tmp.replace(out_path)


class GraphWatcher:
    """
    Monitors a SharePoint folder via:
      - optional "startup scan" (current children)
      - then delta loop (efficient)
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        drive_id: str,
        folder_item_id: str,
        state_dir: Path,
        poll_seconds: int = 30,
        initial_mode: str = "process_existing",
    ) -> None:
        self.drive_id = drive_id
        self.folder_item_id = folder_item_id
        self.poll_seconds = poll_seconds
        self.initial_mode = initial_mode

        token_cache = state_dir / "msal_cache.json"
        self.auth = GraphAuth(tenant_id=tenant_id, client_id=client_id, cache_path=token_cache)
        self.client = GraphClient(self.auth)

        self.delta_link_path = state_dir / "delta_link.txt"
        self.log = logging.getLogger("GraphWatcher")

    def _folder_children_url(self) -> str:
        return f"{GRAPH_ROOT}/drives/{self.drive_id}/items/{self.folder_item_id}/children?$top=999"

    def _delta_url(self) -> str:
        if self.delta_link_path.exists():
            return self.delta_link_path.read_text(encoding="utf-8").strip()
        return f"{GRAPH_ROOT}/drives/{self.drive_id}/items/{self.folder_item_id}/delta"

    def _persist_delta_link(self, delta_link: str) -> None:
        self.delta_link_path.parent.mkdir(parents=True, exist_ok=True)
        self.delta_link_path.write_text(delta_link, encoding="utf-8")

    @staticmethod
    def _is_excel(name: str) -> bool:
        n = name.lower()
        return n.endswith(".xlsx") or n.endswith(".xlsm")

    def _yield_items_from_listing(self, listing_json: Dict[str, Any]) -> Iterator[ChangedItem]:
        for it in listing_json.get("value", []):
            if "file" not in it:
                continue
            name = it.get("name") or ""
            if not self._is_excel(name):
                continue

            item_id = it.get("id") or ""
            etag = it.get("eTag") or ""
            last_modified = (it.get("fileSystemInfo") or {}).get("lastModifiedDateTime") or ""

            download_url = f"{GRAPH_ROOT}/drives/{self.drive_id}/items/{item_id}/content"

            def _dl(dst: Path, _url=download_url) -> None:
                self.client.stream_download(_url, dst)

            yield ChangedItem(
                name=name,
                item_id=item_id,
                etag=etag,
                last_modified=last_modified,
                download_to=_dl,
            )

    def startup_scan(self) -> Iterator[ChangedItem]:
        """
        One-time scan of current folder items.
        Use when you want to reconcile SharePoint state vs local processed state.
        """
        data = self.client.get_json(self._folder_children_url())
        yield from self._yield_items_from_listing(data)

    def delta_changes(self) -> Iterator[ChangedItem]:
        """
        Delta loop:
          - follows @odata.nextLink pages
          - saves @odata.deltaLink for resume
        """
        url = self._delta_url()
        while True:
            latest_delta = None
            next_url = url

            while next_url:
                data = self.client.get_json(next_url)
                yield from self._yield_items_from_listing(data)

                next_url = data.get("@odata.nextLink")
                latest_delta = data.get("@odata.deltaLink") or latest_delta

            if latest_delta:
                self._persist_delta_link(latest_delta)
                url = latest_delta

            time.sleep(self.poll_seconds)

    def iter_changed_items(self, state) -> Iterator[ChangedItem]:
        """
        initial_mode:
          - process_existing: scan folder on startup and emit items that are NOT already processed
          - ignore_existing: establish/advance delta checkpoint and only emit future changes
        """
        if self.initial_mode == "process_existing":
            self.log.info("Startup scan: enabled (process_existing)")
            for item in self.startup_scan():
                yield item
        else:
            self.log.info("Startup scan: disabled (ignore_existing)")
            # Advance delta checkpoint once without emitting, so "now" becomes baseline.
            self._warm_delta_checkpoint()

        yield from self.delta_changes()

    def _warm_delta_checkpoint(self) -> None:
        """
        Runs one delta traversal to capture deltaLink, without yielding items.
        """
        url = f"{GRAPH_ROOT}/drives/{self.drive_id}/items/{self.folder_item_id}/delta"
        latest_delta = None
        next_url = url
        while next_url:
            data = self.client.get_json(next_url)
            next_url = data.get("@odata.nextLink")
            latest_delta = data.get("@odata.deltaLink") or latest_delta
        if latest_delta:
            self._persist_delta_link(latest_delta)
