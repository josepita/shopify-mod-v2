import threading
import time
import uuid
from typing import Dict, List, Optional


class Job:
    """Representa un trabajo en segundo plano."""

    def __init__(self, filename: str):
        self.id: str = uuid.uuid4().hex
        self.filename: str = filename
        self.status: str = "pending"  # pending | running | done | error
        self.error_message: Optional[str] = None
        self.created_at: float = time.time()
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self._logs: List[str] = []
        self._lock = threading.Lock()
        # Progreso opcional
        self.total: int = 0
        self.completed: int = 0
        self.eta_seconds: Optional[float] = None

    def append_log(self, text: str) -> None:
        if not text:
            return
        with self._lock:
            # Dividir por líneas y filtrar vacías para evitar ruido
            for line in str(text).splitlines():
                if line.strip() == "":
                    continue
                # Limitar tamaño del buffer para evitar crecimiento infinito
                self._logs.append(line)
                if len(self._logs) > 2000:
                    # Mantener solo las últimas 2000 líneas
                    self._logs = self._logs[-2000:]

    def get_logs(self, tail: int = 200) -> List[str]:
        with self._lock:
            return self._logs[-tail:]

    def set_progress(self, completed: int, total: int, eta_seconds: Optional[float] = None) -> None:
        with self._lock:
            self.completed = max(0, int(completed))
            self.total = max(0, int(total))
            self.eta_seconds = float(eta_seconds) if eta_seconds is not None else None


class JobManager:
    """Gestor simple en memoria para trabajos y logs."""

    def __init__(self) -> None:
        self._jobs: Dict[str, Job] = {}
        self._lock = threading.Lock()

    def create(self, filename: str) -> Job:
        job = Job(filename)
        with self._lock:
            self._jobs[job.id] = job
        return job

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def list_ids(self) -> List[str]:
        with self._lock:
            return list(self._jobs.keys())


# Instancia global sencilla
job_manager = JobManager()
