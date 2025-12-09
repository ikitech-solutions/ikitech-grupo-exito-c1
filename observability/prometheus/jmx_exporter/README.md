# JMX Exporter Setup for Kafka

This directory contains the JMX Prometheus Java Agent for exporting Kafka metrics.

## Setup Instructions

1. Download the JMX Prometheus Java Agent:

```bash
cd observability/prometheus/jmx_exporter
wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar -O jmx_prometheus_javaagent.jar
```

2. The `kafka-broker.yml` configuration file is already provided and will be used by the agent.

3. The Kafka container is configured to use this agent via the `KAFKA_OPTS` environment variable.

## Verification

Once Kafka is running, you can verify the metrics are being exported:

```bash
curl http://localhost:9308/metrics
```

You should see Kafka JMX metrics in Prometheus format.

## Configuration

The `kafka-broker.yml` file defines which JMX metrics to export and how to transform them into Prometheus metrics. You can customize this file to add or remove metrics as needed.

## Metrics Exposed

- Broker metrics (requests, bytes in/out, etc.)
- Network request metrics
- Log metrics (size, offsets, etc.)
- Controller metrics
- Replica manager metrics
- Partition metrics
