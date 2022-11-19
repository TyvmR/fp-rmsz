package cn.flink.opt1;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

public class FlinkTableStandardStructure {

    public static void main(String[] args) {

        //获取tableEnvironment

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                // .inStreamingMode()//默认就是这个
                //      .useBlinkPlanner()//flink1.14版本之后，默认就是使用了Blink
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);
        //从集合当中获取数据，得到一个表
        Table projTable = tEnv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING())

                ),
                row(1, "zhangsan"),
                row(2, "lisi")
        ).select($("id"), $("name"));
        //注册表
        tEnv.createTemporaryView("sourceTable",projTable);
        //对table当中数据的操作
        tEnv.sqlQuery("select * from sourceTable").execute().print();
        //将操作完成的结果给保存起来
    }

}
