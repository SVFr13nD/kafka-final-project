FROM apache/kafka:3.9.1
ADD https://repo.maven.apache.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar /opt/kafka/jmx-exporter.jar
USER root
RUN chmod 644 /opt/kafka/jmx-exporter.jar
USER appuser
