package myflink.select;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class SpecifyingQueryDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// ingest a DataStream from an external source
        DataStream<Tuple3<Long, String, Integer>> ds = env.fromElements(
                Tuple3.of(1L, "BMW", 1),
                Tuple3.of(2L, "Tesla", 2),
                Tuple3.of(3L, "Tesla", 3)
        );

// SQL query with an inlined (unregistered) table
        Table table = tableEnv.fromDataStream(ds, $("user"), $("product"), $("amount"));
        Table result = tableEnv.sqlQuery(
                "SELECT SUM(amount) FROM " + table + " WHERE product LIKE '%Tesla%'");


// SQL query with a registered table
// register the DataStream as view "Orders"
        tableEnv.createTemporaryView("Orders", ds, $("user"), $("product"), $("amount"));
// run a SQL query on the Table and retrieve the result as a new Table
        Table result2 = tableEnv.sqlQuery("SELECT product, amount FROM Orders WHERE product LIKE '%Tesla%'");

        tableEnv.toAppendStream(result2, Row.class).print();
        env.execute();

// create and register a TableSink
        final Schema schema = new Schema()
                .field("product", DataTypes.STRING())
                .field("amount", DataTypes.INT());

        tableEnv.connect(new FileSystem().path("data/order.csv"))
                .withFormat(new OldCsv().fieldDelimiter(","))
                .withSchema(schema)
                .createTemporaryTable("RubberOrders");

// run an INSERT SQL on the Table and emit the result to the TableSink
        tableEnv.executeSql(
                "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Tesla%'");

    }
}
