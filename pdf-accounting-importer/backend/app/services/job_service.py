# backend/app/services/job_service.py
from __future__ import annotations

import uuid
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from .job_worker import process_job_files


def _utc_iso_z(dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_token(s: Any) -> str:
    return str(s or "").strip().upper()


def _norm_list(xs: Any) -> List[str]:
    if not xs:
        return []
    if isinstance(xs, (list, tuple)):
        out = []
        for x in xs:
            t = _norm_token(x)
            if t:
                out.append(t)
        # unique keep order
        seen = set()
        uniq = []
        for t in out:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        return uniq
    # if string "A,B"
    s = str(xs).strip()
    if not s:
        return []
    if "," in s:
        return _norm_list([p for p in s.split(",") if p.strip()])
    return [_norm_token(s)]


def _safe_cfg(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    ✅ FIX: Normalize cfg to stable shape + preserve strictMode
    
    Expect keys:
      - client_tags: ["SHD","RABBIT","TOPONE",...]
      - client_tax_ids: ["0105...","..."]
      - platforms: ["SHOPEE","SPX",...]
      - strictMode: bool (default False)
    Empty list = allow all
    """
    cfg = cfg or {}
    return {
        "client_tags": _norm_list(cfg.get("client_tags")),
        "client_tax_ids": [str(x).strip() for x in (cfg.get("client_tax_ids") or []) if str(x).strip()],
        "platforms": _norm_list(cfg.get("platforms")),
        "strictMode": bool(cfg.get("strictMode", False)),  # ✅ เพิ่ม strictMode
    }


class JobService:
    """
    ✅ FIX summary:
    - get_payloads() returns 3-tuple (backward compatible with job_worker.py)
    - _safe_cfg() preserves strictMode
    - Removed should_review_by_cfg() (deprecated, use job_worker's _cfg_mismatch instead)
    - Simplified file metadata handling
    """

    def __init__(self) -> None:
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._rows: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.RLock()

        self._threads: Dict[str, threading.Thread] = {}

        self._ttl_seconds: int = 0

    # -------------------------
    # Core lifecycle
    # -------------------------

    def create_job(self, cfg: Optional[Dict[str, Any]] = None) -> str:
        job_id = uuid.uuid4().hex
        now = _utc_iso_z()

        cfg_norm = _safe_cfg(cfg)

        with self._lock:
            self._jobs[job_id] = {
                "job_id": job_id,
                "created_at": now,
                "updated_at": now,
                "state": "queued",  # queued|processing|done|error|cancelled

                "total_files": 0,
                "processed_files": 0,
                "ok_files": 0,
                "review_files": 0,
                "error_files": 0,

                # ✅ job-level filter config (frontend-safe)
                "cfg": cfg_norm,

                # frontend-facing
                "files": [],

                # ✅ internal: (filename, content_type, bytes) - 3-tuple ONLY
                "_payloads": [],

                "_cancel": False,
                "_started_at": "",
                "_finished_at": "",
                "_last_error": "",
            }
            self._rows[job_id] = []

        return job_id

    def add_file(
        self,
        job_id: str,
        filename: str,
        content_type: str,
        content: bytes,
        cfg: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        ✅ FIX: Add file payload - store as 3-tuple (backward compatible)
        cfg is merged into job.cfg if provided
        """
        filename = (filename or "").strip() or "file"
        content_type = (content_type or "").strip() or "application/octet-stream"

        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return

            if job.get("state") in {"processing", "done"}:
                return

            # ✅ Merge cfg into job.cfg if provided
            if cfg:
                job["cfg"] = _safe_cfg({**job.get("cfg", {}), **cfg})

            job["total_files"] = int(job.get("total_files") or 0) + 1
            job["updated_at"] = _utc_iso_z()

            # ✅ Store as 3-tuple (backward compatible)
            job["_payloads"].append((filename, content_type, content))
            
            job["files"].append(
                {
                    "filename": filename,
                    "platform": "unknown",
                    "company": "",
                    "state": "queued",
                    "message": "",
                    "rows_count": 0,
                }
            )

    def start_processing(self, job_id: str, cfg: Optional[Dict[str, Any]] = None) -> None:
        """
        Start background processing thread.
        - cfg optional: if passed, overrides job.cfg (useful when main.py passes cfg)
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return

            if cfg:
                job["cfg"] = _safe_cfg(cfg)

            if job["state"] in {"processing", "done", "cancelled"}:
                return

            job["state"] = "processing"
            job["_started_at"] = _utc_iso_z()
            job["updated_at"] = _utc_iso_z()
            job["_cancel"] = False
            job["_last_error"] = ""

            t = threading.Thread(target=self._run_job, args=(job_id,), daemon=True)
            self._threads[job_id] = t
            t.start()

    def cancel_job(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            if job["state"] not in {"queued", "processing"}:
                return False
            job["_cancel"] = True
            job["state"] = "cancelled"
            job["updated_at"] = _utc_iso_z()
        return True

    def should_cancel(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            return bool(job and job.get("_cancel"))

    # -------------------------
    # Worker runner
    # -------------------------

    def _run_job(self, job_id: str) -> None:
        try:
            process_job_files(self, job_id)

            with self._lock:
                job = self._jobs.get(job_id)
                if not job:
                    return
                if job.get("state") != "cancelled":
                    if job.get("state") == "processing":
                        err = int(job.get("error_files") or 0)
                        job["state"] = "done" if err == 0 else "error"

                job["_finished_at"] = _utc_iso_z()
                job["updated_at"] = _utc_iso_z()

        except Exception as e:
            with self._lock:
                job = self._jobs.get(job_id)
                if job:
                    job["state"] = "error"
                    job["_last_error"] = f"{type(e).__name__}: {e}"
                    job["_finished_at"] = _utc_iso_z()
                    job["updated_at"] = _utc_iso_z()

    # -------------------------
    # ✅ Simplified helpers
    # -------------------------

    def get_cfg(self, job_id: str) -> Dict[str, Any]:
        """Get job config (for worker)"""
        with self._lock:
            job = self._jobs.get(job_id) or {}
            return dict(job.get("cfg") or _safe_cfg(None))

    # -------------------------
    # Mutations used by worker
    # -------------------------

    def update_job(self, job_id: str, patch: Dict[str, Any]) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if job.get("state") == "cancelled":
                patch = dict(patch)
                patch.pop("state", None)

            job.update(patch)
            job["updated_at"] = _utc_iso_z()

    def update_file(self, job_id: str, index: int, patch: Dict[str, Any]) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            files = job.get("files") or []
            if 0 <= index < len(files):
                files[index].update(patch)
                job["updated_at"] = _utc_iso_z()

    def append_rows(self, job_id: str, rows: List[Dict[str, Any]]) -> None:
        """
        ✅ FIX: Simplified - no status parameter (status is in row._status already)
        """
        if not rows:
            return

        with self._lock:
            if job_id not in self._rows:
                return

            for r in rows:
                self._rows[job_id].append(dict(r))

    def get_payloads(self, job_id: str) -> List[Tuple[str, str, bytes]]:
        """
        ✅ FIX: Return 3-tuple (backward compatible with job_worker.py)
        Returns: List[Tuple[filename, content_type, bytes]]
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return []
            # ✅ Return shallow copy of 3-tuples
            return list(job.get("_payloads") or [])

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        ✅ FIX: Added for job_worker's _get_job_cfg() and _get_job_filters()
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None

            # Return clean snapshot without internal fields
            out = {}
            for k, v in job.items():
                if k.startswith("_"):
                    continue
                out[k] = v

            # Make copies of mutable structures
            out["files"] = [dict(x) for x in (out.get("files") or [])]
            out["cfg"] = dict(out.get("cfg") or _safe_cfg(None))
            return out

    # -------------------------
    # Reads (safe snapshots)
    # -------------------------

    def get_rows(self, job_id: str) -> Optional[List[Dict[str, Any]]]:
        with self._lock:
            rows = self._rows.get(job_id)
            if rows is None:
                return None
            return [dict(r) for r in rows]

    # -------------------------
    # Optional: cleanup utilities
    # -------------------------

    def set_ttl_seconds(self, ttl_seconds: int) -> None:
        with self._lock:
            self._ttl_seconds = max(0, int(ttl_seconds))

    def cleanup_expired(self) -> int:
        ttl = int(self._ttl_seconds or 0)
        if ttl <= 0:
            return 0

        now = time.time()
        removed = 0

        with self._lock:
            to_delete: List[str] = []
            for job_id, job in self._jobs.items():
                ts_str = job.get("_finished_at") or job.get("updated_at") or job.get("created_at")
                try:
                    dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                    ts = dt.timestamp()
                except Exception:
                    ts = now

                if (now - ts) > ttl:
                    to_delete.append(job_id)

            for job_id in to_delete:
                self._jobs.pop(job_id, None)
                self._rows.pop(job_id, None)
                self._threads.pop(job_id, None)
                removed += 1

        return removed