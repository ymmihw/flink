package com.ymmihw.bigdata.flink.v2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ymmihw.bigdata.flink.entity.Alert;
import com.ymmihw.bigdata.flink.entity.Transaction;
import com.ymmihw.bigdata.flink.sink.AlertSink;
import com.ymmihw.bigdata.flink.source.TransactionSource;

public class FraudDetectionJobV2 {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Transaction> transactions =
        env.addSource(new TransactionSource()).name("transactions");

    DataStream<Alert> alerts = transactions.keyBy(Transaction::getAccountId)
        .process(new FraudDetectorV2()).name("fraud-detector");

    alerts.addSink(new AlertSink()).name("send-alerts");

    env.execute("Fraud Detection");
  }
}
