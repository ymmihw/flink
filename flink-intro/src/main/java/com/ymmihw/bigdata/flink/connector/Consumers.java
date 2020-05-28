package com.ymmihw.bigdata.flink.connector;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.ymmihw.bigdata.flink.model.InputMessage;
import com.ymmihw.bigdata.flink.schema.InputMessageDeserializationSchema;

public class Consumers {

  public static FlinkKafkaConsumer<String> createStringConsumerForTopic(String topic,
      String kafkaAddress, String kafkaGroup) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", kafkaAddress);
    props.setProperty("group.id", kafkaGroup);
    FlinkKafkaConsumer<String> consumer =
        new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);

    return consumer;
  }

  public static FlinkKafkaConsumer<InputMessage> createInputMessageConsumer(String topic,
      String kafkaAddress, String kafkaGroup) {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", kafkaAddress);
    properties.setProperty("group.id", kafkaGroup);
    FlinkKafkaConsumer<InputMessage> consumer = new FlinkKafkaConsumer<InputMessage>(topic,
        new InputMessageDeserializationSchema(), properties);

    return consumer;
  }
}
