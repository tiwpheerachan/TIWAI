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
    # keep Z suffix
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


class JobService:
    """
    In-memory job service (simple + robust) for:
    - upload files (bytes)
    - async processing via job_worker.process_job_files(self, job_id)
    - polling job status + rows (for frontend)

    Key goals (ตามที่คุณต้องการ):
    ✅ thread-safe
    ✅ payloads internal only
    ✅ supports "re-run" / "cancel" / "ttl cleanup" (optional)
    ✅ safe snapshots (get_job/get_rows ไม่ expose internal refs)
    ✅ file state machine: queued -> processing -> done/needs_review/error
    """

    def __init__(self) -> None:
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._rows: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.RLock()

        # job thread tracking
        self._threads: Dict[str, threading.Thread] = {}

        # optional: TTL auto cleanup (seconds). 0/None = no cleanup
        # you can set env in a higher layer; kept simple here.
        self._ttl_seconds: int = 0

    # -------------------------
    # Core lifecycle
    # -------------------------

    def create_job(self) -> str:
        job_id = uuid.uuid4().hex
        now = _utc_iso_z()

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

                # frontend-facing
                "files": [],

                # internal: (filename, content_type, bytes)
                "_payloads": [],

                # internal flags
                "_cancel": False,
                "_started_at": "",
                "_finished_at": "",
                "_last_error": "",
            }
            self._rows[job_id] = []

        return job_id

    def add_file(self, job_id: str, filename: str, content_type: str, content: bytes) -> None:
        """
        Add file payload for a job.
        - Safe to call multiple times before start_processing
        - If job already processing/done, you can decide to block; here: block.
        """
        filename = (filename or "").strip() or "file"
        content_type = (content_type or "").strip() or "application/octet-stream"

        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return

            if job.get("state") in {"processing", "done"}:
                # Do not mutate a running/completed job
                return

            job["total_files"] = int(job.get("total_files") or 0) + 1
            job["updated_at"] = _utc_iso_z()

            job["_payloads"].append((filename, content_type, content))
            job["files"].append(
                {
                    "filename": filename,
                    "platform": "unknown",
                    "state": "queued",       # queued|processing|done|needs_review|error
                    "message": "",
                    "rows_count": 0,
                }
            )

    def start_processing(self, job_id: str) -> None:
        """
        Start background processing thread.
        - Idempotent
        - Won't start if job cancelled/done or already processing
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return

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
        """
        Best-effort cancel:
        - Sets _cancel flag
        - worker should check job_service.should_cancel(job_id)
        (If worker doesn't check, it will still run until end.)
        """
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
        """
        Worker entrypoint.
        Must never raise (keep job state consistent).
        """
        try:
            process_job_files(self, job_id)

            with self._lock:
                job = self._jobs.get(job_id)
                if not job:
                    return
                if job.get("state") != "cancelled":
                    # process_job_files will set "done"/"error" via update_job
                    # If worker didn't, enforce done when no error_files
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
    # Mutations used by worker
    # -------------------------

    def update_job(self, job_id: str, patch: Dict[str, Any]) -> None:
        """
        Update job fields (thread-safe) and refresh updated_at.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            # do not allow worker to revert cancelled
            if job.get("state") == "cancelled":
                # still allow counters/updated_at to change, but state remains cancelled
                patch = dict(patch)
                patch.pop("state", None)

            job.update(patch)
            job["updated_at"] = _utc_iso_z()

    def update_file(self, job_id: str, index: int, patch: Dict[str, Any]) -> None:
        """
        Update file status (thread-safe).
        """
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
        Append extracted rows to the job.
        - stores a shallow copy to avoid accidental external mutation
        """
        if not rows:
            return
        with self._lock:
            if job_id not in self._rows:
                return
            # store copies (avoid reference bugs)
            for r in rows:
                self._rows[job_id].append(dict(r))

    # -------------------------
    # Reads (safe snapshots)
    # -------------------------

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Return a safe snapshot of job (hide _payloads).
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None

            out = {k: v for k, v in job.items() if k != "_payloads"}

            # deep copy list/dicts for safety
            out["files"] = [dict(x) for x in (out.get("files") or [])]

            return out

    def get_rows(self, job_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        Return snapshot list of rows (safe copy).
        """
        with self._lock:
            rows = self._rows.get(job_id)
            if rows is None:
                return None
            return [dict(r) for r in rows]

    def get_payloads(self, job_id: str) -> List[Tuple[str, str, bytes]]:
        """
        Internal: worker reads raw payloads.
        Returns a shallow copy list so iteration is safe.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return []
            return list(job.get("_payloads") or [])

    # -------------------------
    # Optional: cleanup utilities
    # -------------------------

    def set_ttl_seconds(self, ttl_seconds: int) -> None:
        with self._lock:
            self._ttl_seconds = max(0, int(ttl_seconds))

    def cleanup_expired(self) -> int:
        """
        Remove expired jobs to avoid memory leak in long-running server.
        Call this periodically (e.g. every N minutes) from your API layer.
        """
        ttl = int(self._ttl_seconds or 0)
        if ttl <= 0:
            return 0

        now = time.time()
        removed = 0

        with self._lock:
            to_delete: List[str] = []
            for job_id, job in self._jobs.items():
                # prefer finished time; fallback updated_at/created_at
                ts_str = job.get("_finished_at") or job.get("updated_at") or job.get("created_at")
                try:
                    # parse Z iso
                    # "2026-01-09T02:00:00Z"
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
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
