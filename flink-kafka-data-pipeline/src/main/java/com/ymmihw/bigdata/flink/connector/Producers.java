package com.ymmihw.bigdata.flink.connector;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.ymmihw.bigdata.flink.model.Backup;
import com.ymmihw.bigdata.flink.schema.BackupSerializationSchema;

public class Producers {

  public static FlinkKafkaProducer<String> createStringProducer(String topic, String kafkaAddress) {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
    return new FlinkKafkaProducer<>(topic,
        (e, t) -> new ProducerRecord<>(topic, null, e.getBytes(StandardCharsets.UTF_8)), props,
        Semantic.NONE);
  }

  public static FlinkKafkaProducer<Backup> createBackupProducer(String topic, String kafkaAddress) {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);

    return new FlinkKafkaProducer<>(topic, new BackupSerializationSchema(topic), props,
        Semantic.NONE);

  }
}
