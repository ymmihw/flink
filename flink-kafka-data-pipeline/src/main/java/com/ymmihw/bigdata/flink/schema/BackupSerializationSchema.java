package com.ymmihw.bigdata.flink.schema;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ymmihw.bigdata.flink.model.Backup;

public class BackupSerializationSchema implements SerializationSchema<Backup> {
  private static final long serialVersionUID = 1L;

  static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

  Logger logger = LoggerFactory.getLogger(BackupSerializationSchema.class);

  @Override
  public byte[] serialize(Backup backupMessage) {
    if (objectMapper == null) {
      objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
      objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }
    try {
      String json = objectMapper.writeValueAsString(backupMessage);
      return json.getBytes();
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      logger.error("Failed to parse JSON", e);
    }
    return new byte[0];
  }
}
