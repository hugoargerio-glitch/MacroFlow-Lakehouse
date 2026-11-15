"""Storage and Databricks upload abstractions for MacroFlow edge ingestion."""

from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Optional, Protocol, Sequence

import requests
from pydantic import BaseModel
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def build_retry_session(
    total_retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Create a requests.Session configured with exponential retry/backoff."""
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset({"GET", "PUT", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def write_ndjson(
    records: Sequence[BaseModel | dict[str, Any]],
    output_path: str | Path,
) -> Optional[Path]:
    """Serialize records as strict NDJSON (one JSON object per line, allow_nan=False).

    Returns the written Path, or None without creating a file when records is empty.
    """
    if not records:
        logger.warning("Skipping NDJSON write for %s because payload is empty.", output_path)
        return None

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    for record in records:
        payload = record.model_dump(mode="json") if isinstance(record, BaseModel) else record
        lines.append(json.dumps(payload, ensure_ascii=False, allow_nan=False))

    content = "\n".join(lines) + "\n"

    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp_file:
            tmp_file.write(content)
        os.replace(tmp_name, target)
    except Exception:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise

    return target


class StorageBackend(Protocol):
    """Protocol for persisting partitioned edge extraction files."""

    def write_records(
        self,
        source: str,
        exec_date: date,
        filename: str,
        records: Sequence[BaseModel | dict[str, Any]],
    ) -> Optional[Path]:
        """Persist validated records under a Hive-partitioned date layout."""


class LocalStorageBackend:
    """Local filesystem Hive-partitioned NDJSON storage backend."""

    def __init__(self, base_path: str | Path = "/opt/airflow/raw_data") -> None:
        self.base_path = Path(base_path)

    def partition_dir(self, source: str, exec_date: date) -> Path:
        """Build Hive-style year=/month=/day= partition path for a source and date."""
        return (
            self.base_path
            / source
            / f"year={exec_date.strftime('%Y')}"
            / f"month={exec_date.strftime('%m')}"
            / f"day={exec_date.strftime('%d')}"
        )

    def write_records(
        self,
        source: str,
        exec_date: date,
        filename: str,
        records: Sequence[BaseModel | dict[str, Any]],
    ) -> Optional[Path]:
        """Write records as NDJSON under the Hive-partitioned source directory."""
        target = self.partition_dir(source, exec_date) / filename
        return write_ndjson(records, target)


class DatabricksFilesUploader:
    """Uploads local Hive-partitioned NDJSON files to Databricks UC Volumes or DBFS."""

    def __init__(
        self,
        host: str,
        token: str,
        session: Optional[requests.Session] = None,
        timeout: int = 60,
    ) -> None:
        normalized_host = host.strip().rstrip("/")
        if not normalized_host or not token.strip():
            raise ValueError("Databricks host and token must be non-empty.")
        self.host = normalized_host
        self.token = token.strip()
        self.session = session or build_retry_session()
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def upload_file(self, local_path: str | Path, remote_path: str) -> str:
        """Upload one local file to a UC Volume (/Volumes/...) or DBFS (/mnt/... or dbfs:/...)."""
        src = Path(local_path)
        if not src.is_file():
            raise FileNotFoundError(f"Local file not found for upload: {src}")

        normalized_remote = remote_path.removeprefix("dbfs:")
        if not normalized_remote.startswith("/"):
            normalized_remote = f"/{normalized_remote}"

        raw_bytes = src.read_bytes()

        if normalized_remote.startswith("/Volumes/"):
            url = f"{self.host}/api/2.0/fs/files{normalized_remote}"
            response = self.session.put(
                url,
                headers={**self._headers(), "Content-Type": "application/octet-stream"},
                params={"overwrite": "true"},
                data=raw_bytes,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return normalized_remote

        remote_dir = normalized_remote.rsplit("/", 1)[0]
        mkdir_resp = self.session.post(
            f"{self.host}/api/2.0/dbfs/mkdirs",
            headers=self._headers(),
            json={"path": remote_dir},
            timeout=self.timeout,
        )
        mkdir_resp.raise_for_status()

        put_resp = self.session.post(
            f"{self.host}/api/2.0/dbfs/put",
            headers=self._headers(),
            json={
                "path": normalized_remote,
                "contents": base64.b64encode(raw_bytes).decode("ascii"),
                "overwrite": True,
            },
            timeout=self.timeout,
        )
        put_resp.raise_for_status()
        return normalized_remote

    def sync_partition_date(
        self,
        local_base_path: str | Path,
        exec_date: date,
        remote_base_path: str,
        sources: Sequence[str] = ("bcb", "yfinance"),
    ) -> list[str]:
        """Upload all NDJSON files for a given execution date across sources."""
        local_storage = LocalStorageBackend(local_base_path)
        remote_root = remote_base_path.rstrip("/")
        uploaded: list[str] = []

        for source in sources:
            part_dir = local_storage.partition_dir(source, exec_date)
            if not part_dir.exists():
                continue
            for file_path in sorted(part_dir.glob("*.json")):
                rel_path = file_path.relative_to(local_storage.base_path).as_posix()
                remote_target = f"{remote_root}/{rel_path}"
                uploaded.append(self.upload_file(file_path, remote_target))

        return uploaded


def upload_raw_partition(
    exec_date: date,
    local_base_path: str | Path = "/opt/airflow/raw_data",
    remote_base_path: Optional[str] = None,
    host: Optional[str] = None,
    token: Optional[str] = None,
) -> list[str]:
    """Upload raw NDJSON files for exec_date to Databricks using env defaults when omitted."""
    resolved_host = host or os.environ.get("DATABRICKS_HOST", "")
    resolved_token = token or os.environ.get("DATABRICKS_TOKEN", "")
    resolved_remote = remote_base_path or os.environ.get(
        "DATABRICKS_RAW_BASE_PATH",
        "/Volumes/macroflow/bronze/raw_data",
    )

    uploader = DatabricksFilesUploader(host=resolved_host, token=resolved_token)
    uploaded = uploader.sync_partition_date(
        local_base_path=local_base_path,
        exec_date=exec_date,
        remote_base_path=resolved_remote,
    )
    if not uploaded:
        logger.warning("No local raw files found for %s under %s.", exec_date.isoformat(), local_base_path)
    return uploaded
