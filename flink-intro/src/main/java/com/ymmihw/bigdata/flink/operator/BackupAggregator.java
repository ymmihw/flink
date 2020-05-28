package com.ymmihw.bigdata.flink.operator;

import com.ymmihw.bigdata.flink.model.Backup;
import com.ymmihw.bigdata.flink.model.InputMessage;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class BackupAggregator
    implements AggregateFunction<InputMessage, List<InputMessage>, Backup> {
  private static final long serialVersionUID = 1L;

  @Override
  public List<InputMessage> createAccumulator() {
    return new ArrayList<>();
  }

  @Override
  public List<InputMessage> add(InputMessage inputMessage, List<InputMessage> inputMessages) {
    inputMessages.add(inputMessage);
    return inputMessages;
  }

  @Override
  public Backup getResult(List<InputMessage> inputMessages) {
    Backup backup = new Backup(inputMessages, LocalDateTime.now());
    return backup;
  }

  @Override
  public List<InputMessage> merge(List<InputMessage> inputMessages, List<InputMessage> acc1) {
    inputMessages.addAll(acc1);
    return inputMessages;
  }
}
