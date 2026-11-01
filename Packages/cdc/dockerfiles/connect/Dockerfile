# Kafka Connect + Debezium (includes the Oracle connector and its bundled Oracle JDBC driver).
# Everything is already inside the base image, so the target server needs no internet access.
FROM quay.io/debezium/connect:3.7.0.Final
# Prometheus JMX exporter java agent (baked in at build time); metrics on :9404
ADD --chmod=644 https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar /opt/jmx/jmx_prometheus_javaagent.jar
COPY --chmod=644 jmx-exporter.yml /opt/jmx/jmx-exporter.yml
USER root
RUN chmod 755 /opt/jmx
USER kafka
ENV KAFKA_OPTS="-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=9404:/opt/jmx/jmx-exporter.yml"
EXPOSE 8083 9404
