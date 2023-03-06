package org.sql;


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
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.class").stringType().noDefaultValue(), "org.sql.CustomedPrometheusPushGatewayReporter");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.host").stringType().noDefaultValue(), "10.2.3.18");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.port").intType().noDefaultValue(), 18165);
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.username").stringType().noDefaultValue(),"admin");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.password").stringType().noDefaultValue(), "Push$777");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.jobName").stringType().noDefaultValue(), "myJob");
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.randomJobNameSuffix").booleanType().noDefaultValue(), true);
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.deleteOnShutdown").booleanType().noDefaultValue(), false);
		configuration.set(ConfigOptions.key("metrics.reporter.promgateway.interval").stringType().noDefaultValue(),"5 SECONDS" );
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
				+ "  'properties.bootstrap.servers' = '10.2.3.18:18108,10.2.3.15:18108,10.2.3.19:18108',\r\n"
				+ "  'properties.group.id' = 'so1_j1',\r\n"
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
