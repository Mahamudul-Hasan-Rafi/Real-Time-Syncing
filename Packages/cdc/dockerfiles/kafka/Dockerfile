# Single-node Kafka (KRaft, no ZooKeeper). Listeners are configured at run time (see docker-compose.yml).
FROM docker.io/apache/kafka:4.3.2
LABEL description="Kafka broker for the Debezium Oracle CDC pipeline"
# Prometheus JMX exporter java agent (baked in so the target needs no internet at run time); metrics on :9404
ADD --chmod=644 https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar /opt/jmx/jmx_prometheus_javaagent.jar
COPY --chmod=644 jmx-exporter.yml /opt/jmx/jmx-exporter.yml
USER root
RUN chmod 755 /opt/jmx
USER appuser
ENV KAFKA_OPTS="-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=9404:/opt/jmx/jmx-exporter.yml"
EXPOSE 9222 29092 9093 9404
