# WikiPulse pipeline image (producer now; stream processor in Phase 2).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# libgomp1: OpenMP runtime that LightGBM needs (not in the slim base image).
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install . && useradd --create-home --uid 10001 wikipulse \
    && mkdir -p /app/data && chown wikipulse /app/data
USER wikipulse

CMD ["python", "-m", "wikipulse.producer"]
