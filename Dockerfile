FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PROBE_TIMEOUT_SECONDS=2.0 \
    JOBS_DIR=/data/jobs

WORKDIR /srv/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Each reduction job runs in a background thread; a single worker keeps
# the in-memory job registry coherent while gunicorn's thread pool keeps
# the HTTP interface responsive during probes.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", \
     "--workers", "1", "--threads", "8", "--timeout", "120", \
     "--access-logfile", "-", "--error-logfile", "-", \
     "app:create_app()"]

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
  CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status==200 else 1)"
