from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from pydantic import ConfigDict, Field
from pydantic_settings import BaseSettings

ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH, override=False)


class AppSettings(BaseSettings):
    model_config = ConfigDict(env_file=ENV_PATH, env_file_encoding="utf-8")

    app_name: str = "aegisflow-quant"
    environment: str = "development"
    log_level: str = "INFO"
    ws_connect_timeout_seconds: float = 10.0
    ws_reconnect_delay_seconds: float = 1.0
    ws_reconnect_max_delay_seconds: float = 30.0
    ws_heartbeat_timeout_seconds: float = 20.0
