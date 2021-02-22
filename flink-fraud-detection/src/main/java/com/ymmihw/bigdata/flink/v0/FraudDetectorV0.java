package com.ymmihw.bigdata.flink.v0;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.ymmihw.bigdata.flink.entity.Alert;
import com.ymmihw.bigdata.flink.entity.Transaction;

public class FraudDetectorV0 extends KeyedProcessFunction<Long, Transaction, Alert> {

  private static final long serialVersionUID = 1L;

  @Override
  public void processElement(Transaction transaction, Context context, Collector<Alert> collector)
      throws Exception {

    Alert alert = new Alert();
    alert.setId(transaction.getAccountId());

    collector.collect(alert);
  }
}
