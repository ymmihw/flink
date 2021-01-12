package com.ymmihw.bigdata.flink;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.junit.Test;

public class ConsumeDataFromKafkaTest {
  @Test
  public void testSimpleStringSchema() {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.put("group.id", "flink-kafka-example");
    properties.put("bootstrap.servers", "localhost:9092");

    DataStream<String> inputStream =
        env.addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));
  }

  @Test
  public void testJSONKeyValueDeserializationSchema() {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.put("group.id", "flink-kafka-example");
    properties.put("bootstrap.servers", "localhost:9092");
    DataStream<ObjectNode> inputStream = env.addSource(
        new FlinkKafkaConsumer<>("topic", new JSONKeyValueDeserializationSchema(true), properties));
  }
}
