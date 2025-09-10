本项目用于获取 Kafka 消费组的堆积（lag）数据，并以 Prometheus 格式上报至 VictoriaMetrics，支持 SASL/PLAIN 认证方式。适用于通过 Apollo 格式配置的 Kafka 环境。

Apollo相关配置于main.go中的appConfig配置

Apollo配置项

kafka.brokers = broker1:9092

kafka.sasl.user = your_kafka_user

kafka.sasl.password = your_kafka_password

kafka.sasl.mechanism = PLAIN

kafka.sasl.enable = true

version = 0.10.2.1
