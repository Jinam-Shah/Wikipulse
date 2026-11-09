from wikipulse import __version__
from wikipulse.config import load_settings


def test_defaults_point_at_local_stack(monkeypatch):
    monkeypatch.delenv("WIKIPULSE_KAFKA_BOOTSTRAP", raising=False)
    monkeypatch.delenv("WIKIPULSE_DATABASE_URL", raising=False)
    s = load_settings()
    assert s.kafka_bootstrap == "localhost:19092"
    assert s.database_url.endswith("@localhost:5432/wikipulse")


def test_user_agent_identifies_project_and_contact(monkeypatch):
    monkeypatch.setenv("WIKIPULSE_CONTACT", "ops@example.org")
    ua = load_settings().user_agent
    assert ua.startswith(f"WikiPulse/{__version__} (")
    assert "github.com/jinam-shah/wikipulse" in ua
    assert "ops@example.org" in ua
