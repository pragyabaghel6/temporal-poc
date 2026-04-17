import os
from pathlib import Path


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file(Path(__file__).resolve().parents[1] / ".env")

TEMPORAL_SERVER = os.getenv("TEMPORAL_SERVER", "localhost:7233")
TASK_QUEUE = os.getenv("TASK_QUEUE", "fine-task-queue")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
UPLOAD_DIR = os.getenv(
    "UPLOAD_DIR", str(Path(__file__).resolve().parents[1] / "uploads")
)
JSON_DATA_DIR = os.getenv(
    "JSON_DATA_DIR", str(Path(__file__).resolve().parents[1] / "data")
)

POSTGRES_DB = os.getenv("POSTGRES_DB", "finesdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
USE_JSON_FALLBACK = os.getenv("USE_JSON_FALLBACK", "").lower() in {
    "1",
    "true",
    "yes",
} or not POSTGRES_PASSWORD
