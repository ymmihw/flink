package com.ymmihw.bigdata.flink.schema;

import java.nio.charset.StandardCharsets;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ymmihw.bigdata.flink.model.Backup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackupSerializationSchema implements KafkaSerializationSchema<Backup> {
  private static final long serialVersionUID = 1L;

  private final String topic;
  private final transient ObjectMapper objectMapper;

  public BackupSerializationSchema(String topic) {
    this.topic = topic;
    this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    this.objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(Backup backupMessage, Long timestamp) {
    try {
      String json = objectMapper.writeValueAsString(backupMessage);
      return new ProducerRecord<>(topic, null, json.getBytes(StandardCharsets.UTF_8));
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      log.error("Failed to parse JSON", e);
    }
    return new ProducerRecord<>(topic, null, new byte[0]);
  }
}
