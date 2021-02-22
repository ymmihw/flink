package com.ymmihw.bigdata.flink.v1;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.ymmihw.bigdata.flink.entity.Alert;
import com.ymmihw.bigdata.flink.entity.Transaction;

public class FraudDetectorV1 extends KeyedProcessFunction<Long, Transaction, Alert> {

  private static final long serialVersionUID = 1L;
  private static final double SMALL_AMOUNT = 1.00;
  private static final double LARGE_AMOUNT = 500.00;
  private transient ValueState<Boolean> flagState;

  @Override
  public void open(Configuration parameters) {
    ValueStateDescriptor<Boolean> flagDescriptor =
        new ValueStateDescriptor<>("flag", Types.BOOLEAN);
    flagState = getRuntimeContext().getState(flagDescriptor);
  }

  @Override
  public void processElement(Transaction transaction, Context context, Collector<Alert> collector)
      throws Exception {

    // Get the current state for the current key
    Boolean lastTransactionWasSmall = flagState.value();

    // Check if the flag is set
    if (lastTransactionWasSmall != null) {
      if (transaction.getAmount() > LARGE_AMOUNT) {
        // Output an alert downstream
        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());

        collector.collect(alert);
      }

      // Clean up our state
      flagState.clear();
    }

    if (transaction.getAmount() < SMALL_AMOUNT) {
      // Set the flag to true
      flagState.update(true);
    }
  }
}
