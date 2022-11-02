package myflink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.StateBootstrapFunction;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class StateProcessorWriteDemo {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> input = env.fromElements(1, 2, 3, 4, 5, 6);
        // 将新的转换状态写入保存点
        BootstrapTransformation transformation = OperatorTransformation
                .bootstrapWith(input)
                .transform(new MySimpleBootstrapFunction());
        int maxParallelism = 128;

        Savepoint
                .create(new MemoryStateBackend(), maxParallelism)
                //向保存点添加新的运算符，uid：运算符的uid，Transformation：要包含的运算符
                .withOperator("uid1", transformation)
                // 存入新的或更新的保存点
                .write("F:/savepoint/savepoint-1");

        env.execute();
    }

    private static class MySimpleBootstrapFunction extends StateBootstrapFunction<Integer>  {

        private ListState<Integer> state;

        @Override
        public void processElement(Integer value, Context ctx) throws Exception {
            state.add(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("state1", Types.INT));
        }
    }

    }
