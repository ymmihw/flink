package com.ymmihw.bigdata.flink;

import static org.apache.flink.table.expressions.ExpressionParser.parseExpression;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class SimpleJoinExample {
  public static void main(String[] args) throws Exception {

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

    String peoplesPath =
        SimpleJoinExample.class.getClassLoader().getResource("peoples.csv").getPath();
    String productsPath =
        SimpleJoinExample.class.getClassLoader().getResource("products.csv").getPath();
    String salesPath = SimpleJoinExample.class.getClassLoader().getResource("sales.csv").getPath();

    Table peoples =
        csvTable(env, tableEnv, "peoples", peoplesPath, "pe_id,last_name,country,gender",
            new TypeInformation[] {Types.INT, Types.STRING, Types.STRING, Types.STRING});

    Table products = csvTable(env, tableEnv, "products", productsPath, "prod_id,product_name,price",
        new TypeInformation[] {Types.INT, Types.STRING, Types.FLOAT});

    Table sales = csvTable(env, tableEnv, "sales", salesPath, "people_id,product_id",
        new TypeInformation[] {Types.INT, Types.INT});

    // here is the interesting part:
    Table join = peoples.join(sales).where(parseExpression("pe_id = people_id")).join(products)
        .where(parseExpression("product_id = prod_id")).select(parseExpression("last_name"),
            parseExpression("product_name"), parseExpression("price"))
        .where(parseExpression("price < 40"));

    DataSet<Row> result = tableEnv.toDataSet(join, Row.class);
    result.print();

  }// end main



  public static Table csvTable(ExecutionEnvironment env, BatchTableEnvironment tableEnv,
      String name, String path, String header, TypeInformation<?>[] typeInfo) {
    CsvTableSource tableSource = new CsvTableSource(path, header.split(","), typeInfo);
    DataSet<Row> dataSet = tableSource.getDataSet(env);
    tableEnv.createTemporaryView(name, dataSet);
    return tableEnv.from(name);
  }

}// end class
