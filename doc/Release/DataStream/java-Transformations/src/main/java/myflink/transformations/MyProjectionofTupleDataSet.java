package myflink.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class MyProjectionofTupleDataSet {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Double, String>> in = env.fromElements(Tuple3.of(1, 2.0, "SS"),
                Tuple3.of(2, 3.0, "SSS3E")
        );
        // converts Tuple3<Integer, Double, String> into Tuple2<String, Integer>
        DataSet<Tuple2<String, Integer>> out = in.project(2, 0);
        out.print();

        DataSet<Tuple3<String,String,String>> ds = env.fromElements(Tuple3.of("sss", "ddd", "SS"),
                Tuple3.of("ss2s", "dd2d","SSS3E")
        );
        //这会引起错误
       // DataSet<Tuple1<String>> ds2 = ds.project(0).distinct(0);

        DataSet<Tuple1<String>> ds2 = ds.<Tuple1<String>>project(0).distinct(0);
        ds2.print();

    }
}
