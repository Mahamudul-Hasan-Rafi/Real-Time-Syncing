# connect-guardian: automatic recovery for every connector on the Kafka Connect cluster (see guardian.py).
#
#   docker build -t era/connect-guardian:1.0.0 .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY guardian.py healthcheck.py ./
RUN useradd --uid 1000 --no-create-home guardian && mkdir /data && chown guardian /data
USER guardian
VOLUME /data
EXPOSE 9405
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s CMD ["python", "healthcheck.py"]
CMD ["python", "guardian.py"]
