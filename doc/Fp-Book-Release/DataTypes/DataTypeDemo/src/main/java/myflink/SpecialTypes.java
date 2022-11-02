
package myflink;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class SpecialTypes {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        HashMap<String,String> map = new HashMap<>();
        //创建Map类型数据集
        env.fromElements(map.put("name","peter"),
                map.put("name","peter"));
        //创建List类型数据集
        List<String> stringList = null;

        env.fromElements(stringList.add("sss"),
                stringList.add("sss"));
    }

}

