package myflink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;

import java.io.IOException;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class StateProcessorReadDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 载入保存点
        ExistingSavepoint savepoint = Savepoint.load(env, "F:/savepoint/savepoint-1", new MemoryStateBackend());

        /**
         * 从保存点读取运算符
         * @param  uid运算符的uid。
         * @param  状态的（唯一）名称。
         */
        DataSet<Integer> listState  = savepoint.readListState("uid1","state1", Types.INT);
        //输出数据
        listState.print();
    }
}
