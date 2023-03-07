package org.example.metric.prmetheus.v1;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

/**
 * @Author: john
 * @Date: 2022-11-29-10:45
 * @Description: 定制化推送指标
 */
public class FlinkPushGateway {
    public static void main(String[] args) {
        Configuration config = new Configuration();
//        config.setInteger(RestOptions.PORT,19999);
        config.setString("metrics.reporter.promgateway.class","org.example.metric.prmetheus.v2.CustomedPrometheusPushGatewayReporter");
//        config.setString("metrics.reporter.promgateway.factory.class","org.example.metric.prmetheus.v2.CustomedPrometheusPushGatewayReporterFactory");
        config.setString("metrics.reporter.promgateway.host","10.2.3.18");
        config.setString("metrics.reporter.promgateway.port","18165");
        config.setString("metrics.reporter.promgateway.jobName","test_johnwqeqw21");
        config.setString("metrics.reporter.promgateway.randomJobNameSuffix","false");
        config.setString("metrics.reporter.promgateway.interval","10 SECONDS");
        config.setString("metrics.reporter.promgateway.needBasicAuth","true");
        config.setString("metrics.reporter.promgateway.groupingKey","job_type=flink;job_id=common;jobName=test_johnwqeqw22");
        config.setString("metrics.reporter.promgateway.username","admin");
        config.setString("metrics.reporter.promgateway.password","Push$777");
        config.setString("metrics.reporter.promgateway.deleteOnShutdown","true");
        config.setString("metrics.reporter.promgateway.metric.only.report.names","flink_jobmanager_job_uptime,flink_jobmanager_job_lastCheckpointSize,flink_jobmanager_job_lastCheckpointDuration,flink_jobmanager_job_numberOfFailedCheckpoints,flink_jobmanager_Status_JVM_CPU_Load,flink_jobmanager_Status_JVM_Memory_Heap_Used,flink_jobmanager_Status_JVM_GarbageCollector_PS_MarkSweep_Count,flink_jobmanager_Status_JVM_GarbageCollector_MarkSweepCompact_Count,flink_jobmanager_Status_JVM_GarbageCollector_PS_MarkSweep_Time,flink_jobmanager_Status_JVM_GarbageCollector_MarkSweepCompact_Time,flink_taskmanager_Status_JVM_CPU_Load,flink_taskmanager_Status_JVM_Memory_Heap_Used,flink_taskmanager_Status_JVM_GarbageCollector_MarkSweepCompact_Count,flink_taskmanager_Status_JVM_GarbageCollector_G1_Old_Generation_Count,flink_taskmanager_Status_JVM_GarbageCollector_PS_MarkSweep_Count,flink_taskmanager_Status_JVM_GarbageCollector_MarkSweepCompact_Time,flink_taskmanager_Status_JVM_GarbageCollector_G1_Old_Generation_Time,flink_taskmanager_Status_JVM_GarbageCollector_PS_MarkSweep_Time,flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outPoolUsage,flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inPoolUsage,flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outputQueueLength,flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inputQueueLength,flink_taskmanager_job_task_currentInputWatermark,flink_jobmanager_job_downtime,flink_taskmanager_job_task_numBytesIn,flink_taskmanager_job_task_numBytesOut,flink_taskmanager_job_task_numRecordsIn,flink_taskmanager_job_task_numRecordsOut");
//        config.setString("metrics.reporter.promgateway.metric.only.report.names","flink_jobmanager_job_uptime");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.disableOperatorChaining();
        env.setParallelism(1);
        DataStreamSource<Integer> source= env.addSource(new RandomSourceFunc(Integer.MAX_VALUE));
        source.print();
        source.map(new MetricsMapFunc())
                .name(MetricsMapFunc.class.getSimpleName())
                .addSink(new DiscardingSink<>())
                .name(DiscardingSink.class.getSimpleName());
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
