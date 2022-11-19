package cn.flink.opt4.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class JsonParseFunction  extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    public String eval(String jsonLine,String key){
        JSONObject jsonObject = JSONObject.parseObject(jsonLine);
        if(jsonObject.containsKey(key)){
            return jsonObject.getString(key);
        }else{
            return "";
        }
    }
}
