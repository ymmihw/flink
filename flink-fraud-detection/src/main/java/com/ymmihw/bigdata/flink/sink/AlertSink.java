package com.ymmihw.bigdata.flink.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import com.ymmihw.bigdata.flink.entity.Alert;
import lombok.extern.slf4j.Slf4j;

/** A sink for outputting alerts. */
@PublicEvolving
@Slf4j
public class AlertSink implements SinkFunction<Alert> {

  private static final long serialVersionUID = 1L;

  @Override
  public void invoke(Alert value, Context context) {
    log.info(value.toString());
  }
}
