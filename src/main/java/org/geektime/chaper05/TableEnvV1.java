//package org.geektime.chaper05;
//
//import org.apache.flink.api.scala.ExecutionEnvironment;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.*;
//import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//import static org.apache.flink.table.api.Expressions.$;
//import static org.apache.flink.table.api.Expressions.row;
//
///**
// * @Author: john
// * @Date: 2022-11-09-9:48
// * @Description:
// */
//public class TableEnvV1 {
//
//    public static void main(String[] args) throws Exception {
//        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
//        TableEnvironment tenv = TableEnvironment.create(build);
//        Table table = tenv.fromValues(
//                row(1, "ko"),
//                row(2, "kk")
//        );
////        Table table = tenv.fromValues(
////                        DataTypes.ROW(
////                                DataTypes.FIELD("id",DataTypes.INT()),
////                                DataTypes.FIELD("name", DataTypes.STRING())
////                        ),
////                row(1, "ko"),
////                row(2, "kk")
////        );
//        tenv.createTemporaryView("student",table);
////        TableResult tableResult = tenv.executeSql("select * from student");
//        TableResult tableResult = tenv.executeSql("select * from student ");
//        tableResult.print();
//    }
//}
