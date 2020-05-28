package com.ymmihw.bigdata.flink.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import com.ymmihw.bigdata.flink.model.Backup;
import com.ymmihw.bigdata.flink.schema.BackupSerializationSchema;

public class Producers {

  public static FlinkKafkaProducer<String> createStringProducer(String topic, String kafkaAddress) {
    return new FlinkKafkaProducer<>(kafkaAddress, topic, new SimpleStringSchema());
  }

  public static FlinkKafkaProducer<Backup> createBackupProducer(String topic, String kafkaAddress) {
    return new FlinkKafkaProducer<Backup>(kafkaAddress, topic, new BackupSerializationSchema());
  }
}
