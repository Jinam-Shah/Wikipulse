"""Central place for settings read from the environment (.env locally, secrets in CI/hosting)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from wikipulse import PROJECT_URL, __version__


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    database_url: str
    contact: str

    @property
    def user_agent(self) -> str:
        """User-Agent sent to every Wikimedia endpoint (see docs/DATA_SOURCES.md)."""
        return f"WikiPulse/{__version__} ({PROJECT_URL}; {self.contact})"


def load_settings() -> Settings:
    return Settings(
        kafka_bootstrap=os.environ.get("WIKIPULSE_KAFKA_BOOTSTRAP", "localhost:19092"),
        database_url=os.environ.get(
            "WIKIPULSE_DATABASE_URL",
            "postgresql://wikipulse:wikipulse@localhost:5432/wikipulse",
        ),
        contact=os.environ.get(
            "WIKIPULSE_CONTACT", "https://github.com/jinam-shah/wikipulse/issues"
        ),
    )
