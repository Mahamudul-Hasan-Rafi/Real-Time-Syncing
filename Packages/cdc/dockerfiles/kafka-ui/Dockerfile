# Web UI for the Kafka broker and the Debezium Kafka Connect cluster (no login, as requested).
# Cluster addresses are the container names on the shared docker network (cdc-net).
FROM docker.io/provectuslabs/kafka-ui:latest
ENV KAFKA_CLUSTERS_0_NAME=era-cdc \
    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092 \
    KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME=debezium \
    KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS=http://connect:8083 \
    DYNAMIC_CONFIG_ENABLED=false
EXPOSE 8080
