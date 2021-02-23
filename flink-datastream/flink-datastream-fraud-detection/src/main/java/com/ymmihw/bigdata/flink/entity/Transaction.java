package com.ymmihw.bigdata.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public final class Transaction {
  private long accountId;
  private long timestamp;
  private double amount;
}
