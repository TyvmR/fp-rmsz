package org.example.metric.prmetheus.v2;


import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FliniMetric {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger(RestOptions.PORT, 19002);
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.class").stringType().noDefaultValue(), "org.example.metric.prmetheus.v2.CustomedPrometheusPushGatewayReporter");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.host").stringType().noDefaultValue(), "10.2.3.18");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.port").stringType().noDefaultValue(), "18165");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.username").stringType().noDefaultValue(),"admin");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.password").stringType().noDefaultValue(), "Push$777");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.jobName").stringType().noDefaultValue(), "myJob");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.randomJobNameSuffix").stringType().noDefaultValue(), "false");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.deleteOnShutdown").stringType().noDefaultValue(), "false");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.interval").stringType().noDefaultValue(),"5 SECONDS" );
		configuration.setString("metrics.reporter.promgateway.groupingKey","job_type=flink;job_id=common;jobName=test_johnwqeqw22");
		configuration.setString("metrics.reporter.promgateway.metric.only.report.names","flink_jobmanager_job_uptime,flink_jobmanager_job_lastCheckpointSize,flink_jobmanager_job_lastCheckpointDuration,flink_jobmanager_job_numberOfFailedCheckpoints,flink_jobmanager_Status_JVM_CPU_Load,flink_jobmanager_Status_JVM_Memory_Heap_Used,flink_jobmanager_Status_JVM_GarbageCollector_PS_MarkSweep_Count,flink_jobmanager_Status_JVM_GarbageCollector_MarkSweepCompact_Count,flink_jobmanager_Status_JVM_GarbageCollector_PS_MarkSweep_Time,flink_jobmanager_Status_JVM_GarbageCollector_MarkSweepCompact_Time,flink_taskmanager_Status_JVM_CPU_Load,flink_taskmanager_Status_JVM_Memory_Heap_Used,flink_taskmanager_Status_JVM_GarbageCollector_MarkSweepCompact_Count,flink_taskmanager_Status_JVM_GarbageCollector_G1_Old_Generation_Count,flink_taskmanager_Status_JVM_GarbageCollector_PS_MarkSweep_Count,flink_taskmanager_Status_JVM_GarbageCollector_MarkSweepCompact_Time,flink_taskmanager_Status_JVM_GarbageCollector_G1_Old_Generation_Time,flink_taskmanager_Status_JVM_GarbageCollector_PS_MarkSweep_Time,flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outPoolUsage,flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inPoolUsage,flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outputQueueLength,flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inputQueueLength,flink_taskmanager_job_task_currentInputWatermark,flink_jobmanager_job_downtime,flink_taskmanager_job_task_numBytesIn,flink_taskmanager_job_task_numBytesOut,flink_taskmanager_job_task_numRecordsIn,flink_taskmanager_job_task_numRecordsOut");
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);
		EnvironmentSettings es = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamTableEnvironment tenv = StreamTableEnvironment.create(env,es);
		tenv.executeSql("CREATE TABLE john_test_source_tb(\r\n"
				+ "  `user_id` BIGINT,\r\n"
				+ "  `item_id` BIGINT,\r\n"
				+ "  `behavior` STRING,\r\n"
				+ "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\r\n"
				+ ") WITH (\r\n"
				+ "  'connector' = 'kafka',\r\n"
				+ "  'topic' = 'so1_j1_1',\r\n"
				+ "  'properties.bootstrap.servers' = '10.2.3.15:12108',\r\n"
				+ "  'properties.group.id' = 'so1_j1——2',\r\n"
				+ "  'scan.startup.mode' = 'earliest-offset',\r\n"
				+ "  'format' = 'csv'\r\n"
				+ ")");
		
		tenv.executeSql("CREATE TABLE john_test_sink_tb(\r\n"
				+ "  `number` BIGINT \r\n"
				+ ") WITH (\r\n"
				+ "  'connector' = 'blackhole' \r\n"
				+ ")");
		
	    Table midTb = tenv.sqlQuery("select user_id as account_id, count(*) as number from john_test_source_tb group by user_id");
		
		tenv.executeSql("INSERT INTO john_test_sink_tb SELECT number FROM " +  midTb);
	}
}
