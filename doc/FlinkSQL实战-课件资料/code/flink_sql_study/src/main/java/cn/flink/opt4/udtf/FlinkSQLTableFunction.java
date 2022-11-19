package cn.flink.opt4.udtf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class FlinkSQLTableFunction {

    public static void main(String[] args) {

        //获取tableEnvironment

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                // .inStreamingMode()//默认就是这个
                //      .useBlinkPlanner()//flink1.14版本之后，默认就是使用了Blink
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);

        tEnv.createTemporarySystemFunction("JsonFunc",JsonFunction.class);
        tEnv.createTemporarySystemFunction("explodeFunc",ExplodeFunc.class);



        String source_sql = "CREATE TABLE json_table (\n" +
                "  line STRING \n" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='input/product_user.json',\n" +
                "  'format'='raw'\n" +  //定义数据内容为raw，表示一整行数据作为一条
                ")";

        tEnv.executeSql(source_sql);

        tEnv.sqlQuery("select JsonFunc(line,'date_time') ,JsonFunc(line,'price')  ,JsonFunc(line,'productId') , id,name,begin_time,email  from json_table,lateral table(explodeFunc (line,'userBaseList') )")
                .execute().print();
    }
    /**
     * 自定义scalarFunction，实现json格式的数据的解析
     */
    public static class JsonFunction extends ScalarFunction{
        public String eval(String line,String key){
            JSONObject jsonObject = JSONObject.parseObject(line);
            if(jsonObject.containsKey(key)){
                return jsonObject.getString(key);
            }else{
                return "";
            }
        }
    }


    //一条JsonArray数据进入，然后解析成为多条数据
    @FunctionHint(output = @DataTypeHint("ROW<id String,name String,begin_time String, email String>"))
    public static class ExplodeFunc extends TableFunction{

        public void eval(String line,String key){
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONArray jsonArray = jsonObject.getJSONArray(key);

            for(int i = 0;i< jsonArray.size();i ++){
                String begin_time = jsonArray.getJSONObject(i).getString("begin_time");
                String id = jsonArray.getJSONObject(i).getString("id");
                String name = jsonArray.getJSONObject(i).getString("name");
                String email = jsonArray.getJSONObject(i).getString("email");

                //使用collect来收集解析出来之后的数据
                collect(Row.of(id,name,begin_time,email));


            }


        }




    }






}
