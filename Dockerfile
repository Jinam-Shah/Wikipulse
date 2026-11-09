# WikiPulse pipeline image (producer now; stream processor in Phase 2).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install . && useradd --create-home --uid 10001 wikipulse \
    && mkdir -p /app/data && chown wikipulse /app/data
USER wikipulse

CMD ["python", "-m", "wikipulse.producer"]
