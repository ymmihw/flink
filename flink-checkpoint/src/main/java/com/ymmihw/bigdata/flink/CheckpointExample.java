package com.ymmihw.bigdata.flink;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CheckpointExample {

  private static final String KAFKA_BROKER = "localhost:9092";
  private static final String KAFKA_INPUT_TOPIC = "input-topic";
  private static final String KAFKA_GROUP_ID = "flink-stackoverflow-checkpointer";
  private static final String CLASS_NAME = CheckpointExample.class.getSimpleName();


  public static void main(String[] args) throws Exception {

    // play with them
    boolean checkpointEnable = true;
    long checkpointInterval = 1000;
    CheckpointingMode checkpointMode = CheckpointingMode.EXACTLY_ONCE;

    // ----------------------------------------------------

    log.info(CLASS_NAME + ": starting...");
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // kafka source
    // https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/connectors/kafka.html#kafka-consumer
    Properties prop = new Properties();
    prop.put("bootstrap.servers", KAFKA_BROKER);
    prop.put("group.id", KAFKA_GROUP_ID);
    prop.put("auto.offset.reset", "latest");
    prop.put("enable.auto.commit", "true");

    FlinkKafkaConsumer<String> source =
        new FlinkKafkaConsumer<>(KAFKA_INPUT_TOPIC, new SimpleStringSchema(), prop);

    // checkpoints
    // internals:
    // https://ci.apache.org/projects/flink/flink-docs-master/internals/stream_checkpointing.html#checkpointing
    // config:
    // https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/stream/checkpointing.html
    if (checkpointEnable)
      env.enableCheckpointing(checkpointInterval, checkpointMode);

    env.addSource(source).keyBy(any -> 1).flatMap(new StatefulMapper()).print();

    env.execute(CLASS_NAME);
  }

  public static class StatefulMapper extends RichFlatMapFunction<String, String> {
    private static final long serialVersionUID = 1L;
    private transient ValueState<Integer> state;

    @Override
    public void flatMap(String record, Collector<String> collector) throws Exception {
      // access the state value
      Integer currentState = state.value();

      currentState = currentState == null ? 0 : currentState;

      // update the counts
      currentState += 1;
      collector.collect(String.format("%s: (%s,%d)",
          LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), record, currentState));
      // update the state
      state.update(currentState);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      ValueStateDescriptor<Integer> descriptor =
          new ValueStateDescriptor<>("CheckpointExample", TypeInformation.of(Integer.class));
      state = getRuntimeContext().getState(descriptor);
    }

  }
}
