package org.fllik.bwsd.book.shizhanpai.demo;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public interface DataLaketDemoTests {

    /**
     * 常规维表输入数据源创建语句
     */
    String CREATE_INPUT_SOURCE_DIM = " create table `${outputTable}` ( ${fields} " +
            " ) with (" +
            "'connector'='jdbc'," +
            "'url'='${url}'," +
            "'username'= '${userName}'," +
            "'password' = '${password}'," +
            "'table-name' = '${inputTable}'," +
            "'driver' = 'com.mysql.cj.jdbc.Driver'," +
            "'lookup.cache.max-rows' = '${maxRows}'," +
            "'lookup.cache.ttl' = '${ttl}'" +
            ")";

    String SELECT_JOIN = "select ${fields} from ${firstTable} ${joinType} join " +
            "${secondTable} FOR SYSTEM_TIME AS OF ${firstTable}.${timeField} on ${joinFields} ${condition}";

    /**
     * 聚合计算语句
     */
    String SELECT_AGGREGATE = "select ${windowType}_END(${timeField}, ${interval}) as ${procTimeField} " +
            " ${aggregate} ${selectGroup}, TIMEMILL(${windowType}_END(${timeField}, ${interval}), ${isProcTime}) " +
            " as ${outputTimestamp}  from ${inputTable}  ${condition} " +
            " group by ${windowType}(${timeField}, ${interval}) ${groupBy}";

    /**
     * 聚合计算语句
     * 事件时间
     */
    String SELECT_AGGREGATE_WITH_EVENT_TIME = "select ${windowType}_END(${timeField}, ${interval}) as ${procTimeField}, " +
            " ${windowType}_END(${timeField}, ${interval}) as ${eventTimeField} ${aggregate} ${selectGroup}, TIMEMILL(${windowType}_END(${timeField}, ${interval}), ${isProcTime}) " +
            " as ${outputTimestamp}  from ${inputTable}  ${condition} " +
            " group by ${windowType}(${timeField}, ${interval}) ${groupBy}";


    /**
     * kafka输入源,处理时间
     */
    String CREATE_INPUT_SOURCE_KAFKA_WITH_PROC_TIME = " create table `${outputTable}` ( ${fields} " +
            " cloud_wise_proc_time as  proctime()" +
            " ) with (" +
            "'connector'='kafka-0.11'," +
            "'topic'='${topic}'," +
            "'properties.bootstrap.servers'= '${propertiesBootstrapServers}'," +
            "'properties.max.poll.records'= '${propertiesMaxPollRecords}'," +
            "'properties.group.id' = '${propertiesGroupId}'," +
            "'format' = 'json'," +
            "'json.ignore-parse-errors' = 'true'," +
            "'scan.startup.mode' = '${scanStartupMode}'" +
            ")";

    /**
     * upsert-kafka输入源,处理时间
     */
    String CREATE_INPUT_SOURCE_UPSERTKAFKA_WITH_PROC_TIME = " create table ${outputTable} ( ${fields} " +
            " cloud_wise_proc_time as  proctime()," +
            " PRIMARY KEY(${primaryKeyFields}) NOT ENFORCED " +
            " ) with (" +
            "'connector'='${connectorType}'," +
            "'topic'='${topic}'," +
            "'properties.bootstrap.servers'= '${propertiesBootstrapServers}'," +
            "'properties.max.poll.records'= '${propertiesMaxPollRecords}'," +
            "'properties.group.id' = '${propertiesGroupId}'," +
            "'key.format' = '${keyFormat}'," +
            "'value.format' = '${valueFormat}'," +
            "'value.fields-include' = '${valueFieldsInclude}'," +
            "'value.json.ignore-parse-errors' = '${valueIgnoreParseErrors}'," +
            "'value.json.fail-on-missing-field' = '${valueFailMissingField}'" +
            ")";

    /**
     * kafka输入源， 事件时间
     */
    String CREATE_INPUT_SOURCE_KAFKA_WITH_EVENT_TIME = " create table `${outputTable}` ( ${fields} " +
            " cloud_wise_proc_time as  proctime()," +
            " cloud_wise_event_time as  ${cloudWiseEventTime}, " +
            " watermark for cloud_wise_event_time as cloud_wise_event_time - INTERVAL '${delay}' ${delayUnit} " +
            " ) with (" +
            "'connector'='kafka-0.11'," +
            "'topic'='${topic}'," +
            "'properties.bootstrap.servers'= '${propertiesBootstrapServers}'," +
            "'properties.max.poll.records'= '${propertiesMaxPollRecords}'," +
            "'properties.group.id' = '${propertiesGroupId}'," +
            "'format' = 'json'," +
            "'json.ignore-parse-errors' = 'true'," +
            "'scan.startup.mode' = '${scanStartupMode}'" +
            ")";

    /**
     * upsert-kafka输入源， 事件时间
     */
    String CREATE_INPUT_SOURCE_UPSERTKAFKA_WITH_EVENT_TIME = " create table ${outputTable} ( ${fields} " +
            " cloud_wise_proc_time as  proctime()," +
            " cloud_wise_event_time as  ${cloudWiseEventTime}," +
            " PRIMARY KEY(${primaryKeyFields}) NOT ENFORCED ," +
            " watermark for cloud_wise_event_time as cloud_wise_event_time - INTERVAL '${delay}' ${delayUnit} " +
            " ) with (" +
            "'connector'='${connectorType}'," +
            "'topic'='${topic}'," +
            "'properties.bootstrap.servers'= '${propertiesBootstrapServers}'," +
            "'properties.max.poll.records'= '${propertiesMaxPollRecords}'," +
            "'properties.group.id' = '${propertiesGroupId}'," +
            "'key.format' = '${keyFormat}'," +
            "'value.format' = '${valueFormat}'," +
            "'value.fields-include' = '${valueFieldsInclude}'," +
            "'value.json.ignore-parse-errors' = '${valueIgnoreParseErrors}'," +
            "'value.json.fail-on-missing-field' = '${valueFailMissingField}'" +
            ")";

    /**
     * kafka输出源：创建输出表
     */
    String CREATE_OUTPUT_SINK_KAFKA = " create table `${outputTable}` ( ${fields} " +
            " ) with (" +
            "'connector'='kafka-0.11'," +
            "'topic'='${topic}'," +
            "'properties.bootstrap.servers'= '${propertiesBootstrapServers}'," +
            "'properties.acks'= '${propertiesAcks}'," +
            "'properties.retries' = '${propertiesRetries}'," +
            "'properties.batch.size' = '${propertiesBatchSize}'," +
            "'properties.linger.ms' = '${propertiesLingerMs}'," +
            "'properties.buffer.memory' = '${propertiesBufferMemory}'," +
            "'json.ignore-parse-errors' = 'true'," +
            "'format' = '${format}'" +
            ")";
    /**
     * upsert-kafka输出源：创建输出表
     */
    String CREATE_OUTPUT_SINK_UPSERTKAFKA = " create table ${outputTable} ( ${fields}, " +
            " PRIMARY KEY(${primaryKeyFields}) NOT ENFORCED " +
            " ) with (" +
            "'connector'='${connectorType}'," +
            "'topic'='${topic}'," +
            "'properties.bootstrap.servers'= '${propertiesBootstrapServers}'," +
            "'properties.acks'= '${propertiesAcks}'," +
            "'properties.retries' = '${propertiesRetries}'," +
            "'properties.batch.size' = '${propertiesBatchSize}'," +
            "'properties.linger.ms' = '${propertiesLingerMs}'," +
            "'properties.buffer.memory' = '${propertiesBufferMemory}'," +
            "'key.format' = '${keyFormat}'," +
            "'value.format' = '${valueFormat}'," +
            "'value.fields-include' = '${valueFieldsInclude}'," +
            "'value.json.ignore-parse-errors' = '${valueIgnoreParseErrors}'," +
            "'value.json.fail-on-missing-field' = '${valueFailMissingField}'" +
            ")";

    /**
     * JSON keyFormat 参数
     */
    String CONCAT_JSON_KEY_CONFIG_KEYFORMAT =
            StrUtil.COMMA +
                    "'key.json.ignore-parse-errors' = '${keyIgnoreParseErrors}'," +
                    "'key.json.fail-on-missing-field' = '${keyFailMissingField}'" +
                    ")";

    /**
     * kafka输出
     */
    String INSERT_OUTPUT_SINK_KAFKA = " insert into `${outputTable}` select ${fields} " +
            " from ${inputTable} ${condition}";

    String CREATE_OUT_PUT_SINK_ELASTICSEARCH = " create table `${outputTable}` ( ${fields} " +
            " ) with (" +
            "'connector'='${connectorType}-${connectorVersion}'," +
            "'hosts'='${hosts}'," +
            "'index'= '${connectorIndex}'," +
            "'document-type'= '${connectorDocumentType}'," +
            "'sink.bulk-flush.max-actions' = '${connectorBulkFlushMaxActions}'," +
            "'failure-handler' = '${failureHandler}'," +
            "'format' = '${format}'" +
            ")";

    /**
     * 数据运算语句
     */
    String SELECT_CACULATE = "select ${eventTimeField}${procTimeField}${caculate}  from ${inputTable} ${condition}";

    /**
     * select as view 语句
     */
    String SELECT_AS_VIEW = "create view ${outTable} as (${sql})";

    /**
     * select as view 语句的特例（只有一个子节点，并且子节点是sink类型节点）
     */
    String SELECT_INSERT_INTO_SINK = "insert into ${outTable} (${sql})";

    /**
     * 获取kafkaSource的数据进行拉平处理
     * 事件时间下的timestamp
     */
    String SELECT_FLATTENING_KAFKA_SOURCE_WITH_EVENT_TIME_TIMESTAMP = "select ${fields} , cloud_wise_proc_time , cloud_wise_event_time from ${sourceTable}";


    /**
     * 获取kafkaSource的数据进行拉平处理
     * 处理时间下的timestamp
     */
    String SELECT_FLATTENING_KAFKA_SOURCE_WITH_PROC_TIME_TIMESTAMP = "select ${fields},cloud_wise_proc_time from ${sourceTable}";


    /**
     * postgre source表
     */
    String CREATE_POSTGRE_SOURCE_TABLE_SQL =
            " create table `${outputTable}` ( ${fields} \n" +
                    " ) with (\n" +
                    "'connector'='jdbc',\n" +
                    "'url'='${url}',\n" +
                    "'username'= '${userName}',\n" +
                    "'password' = '${password}',\n" +
                    "'table-name' = '${inputTable}',\n" +
                    "'driver' = 'org.postgresql.Driver',\n" +
                    "'lookup.cache.max-rows' = '${maxRows}',\n" +
                    "'lookup.cache.ttl' = '${ttl}'\n" +
                    ")";
    /**
     * postgre source sql with partition
     */
    String CREATE_POSTGRE_SOURCE_TABLE_WITH_PARTITION_SQL =
            " create table `${outputTable}` ( ${fields} \n" +
                    " ) with (\n" +
                    "'connector'='jdbc',\n" +
                    "'url'='${url}',\n" +
                    "'username'= '${userName}',\n" +
                    "'password' = '${password}',\n" +
                    "'table-name' = '${inputTable}',\n" +
                    "'driver' = 'org.postgresql.Driver',\n" +
                    "'scan.where.condition' = '${scanWhereCondition}',\n" +
                    "'scan.fetch-size' = '${scanFetchSize}',\n" +
                    "'scan.partition.column' = '${partitionColumn}',\n" +
                    "'scan.partition.num' = '${partitionNum}',\n" +
                    "'scan.partition.lower-bound' = '${partitionLowerBound}',\n" +
                    "'scan.partition.upper-bound' = '${partitionUpperBound}'\n" +
                    ")";

    /**
     * postgre sink表
     */
    String CREATE_POSTGRE_SINK_TABLE_SQL =
            " create table `${outputTable}` ( ${fields} \n" +
                    " ) with (\n" +
                    "'connector'='jdbc',\n" +
                    "'url'='${url}',\n" +
                    "'username'= '${userName}',\n" +
                    "'password' = '${password}',\n" +
                    "'table-name' = '${inputTable}',\n" +
                    "'driver' = 'org.postgresql.Driver',\n" +
                    "'sink.buffer-flush.max-rows' = '${flushMaxRows}',\n" +
                    "'sink.buffer-flush.interval' = '${flushInterval}',\n" +
                    "'sink.max-retries' = '${sinkMaxRetries}'\n" +
                    ")";

    /**
     * oracle source表
     * 11g 驱动类
     */
    String CREATE_ORACLE_SOURCE_TABLE_SQL =
            " create table `${outputTable}` ( ${fields} \n" +
                    " ) with (\n" +
                    "'connector'='jdbc',\n" +
                    "'url'='${url}',\n" +
                    "'username'= '${userName}',\n" +
                    "'password' = '${password}',\n" +
                    "'table-name' = '${inputTable}',\n" +
                    //todo：测试不写driver 是否能批量写数据
                    //"'driver' = 'oracle.jdbc.driver.OracleDriver'\n" +
                    "'lookup.cache.max-rows' = '${maxRows}'," +
                    "'lookup.cache.ttl' = '${ttl}'" +
                    ")";

    /**
     * oracle sink表
     * 11g 驱动类
     */
    String CREATE_ORACLE_SINK_TABLE_SQL =
            " create table `${outputTable}` ( ${fields} \n" +
                    " ) with (\n" +
                    "'connector'='jdbc',\n" +
                    "'url'='${url}',\n" +
                    "'username'= '${userName}',\n" +
                    "'password' = '${password}',\n" +
                    "'table-name' = '${inputTable}'\n" +
                    //todo：测试不写driver 是否能批量写数据
                    //"'driver' = 'oracle.jdbc.driver.OracleDriver'\n" +
                    ")";

    /**
     * clickhouse source sql
     */
    String CREATE_CLICKHOUSE_SOURCE_TABLE_SQL =
            " create table `${outputTable}` ( ${fields} \n" +
                    " ) with (\n" +
                    "'connector'='jdbc',\n" +
                    "'url'='${url}',\n" +
                    "'username'= '${userName}',\n" +
                    "'password' = '${password}',\n" +
                    "'table-name' = '${inputTable}',\n" +
                    "'lookup.cache.max-rows' = '${maxRows}',\n" +
                    "'lookup.cache.ttl' = '${ttl}',\n" +
                    "'scan.where.condition' = '${scanWhereCondition}'" +
                    ")";


    /**
     * clickhouse source sql with partition
     */
    String CREATE_CLICKHOUSE_SOURCE_TABLE_WITH_PARTITION_SQL =
            " create table `${outputTable}` ( ${fields} \n" +
                    " ) with (\n" +
                    "'connector'='jdbc',\n" +
                    "'url'='${url}',\n" +
                    "'username'= '${userName}',\n" +
                    "'password' = '${password}',\n" +
                    "'table-name' = '${inputTable}',\n" +
                    "'lookup.cache.max-rows' = '${maxRows}',\n" +
                    "'lookup.cache.ttl' = '${ttl}',\n" +
                    "'scan.where.condition' = '${scanWhereCondition}',\n" +
                    "'scan.partition.column' = '${partitionColumn}',\n" +
                    "'scan.partition.num' = '${partitionNum}',\n" +
                    "'scan.partition.lower-bound' = '${partitionLowerBound}',\n" +
                    "'scan.partition.upper-bound' = '${partitionUpperBound}'\n" +
                    ")";


    /**
     * clickhouse sink sql
     */
    String CREATE_CLICKHOUSE_SINK_TABLE_SQL =
            " create table `${outputTable}` ( ${fields} \n" +
                    " ) with (\n" +
                    "'connector'='jdbc',\n" +
                    "'url'='${url}',\n" +
                    "'username'= '${userName}',\n" +
                    "'password' = '${password}',\n" +
                    "'table-name' = '${inputTable}',\n" +
                    "'sink.buffer-flush.max-rows' = '${flushMaxRows}',\n" +
                    "'sink.buffer-flush.interval' = '${flushInterval}',\n" +
                    "'sink.max-retries' = '${sinkMaxRetries}'\n" +
                    ")";

    /**
     * jdbc输出
     * outputTable：sink表名
     * inputTable：父节点表名的逻辑注意点
     */
    String INSERT_OUTPUT_SINK_JDBC = " insert into `${outputTable}` select ${fields} " +
            " from ${inputTable} ${condition}";

    /**
     * kerberos认证 参数
     */
    String CONCAT_CONFIG_KERBEROS =
            ",'properties.security.protocol' = '${propertiesSecurityProtocol}'," +
                    "'properties.sasl.mechanism' = '${propertiesSaslMechanism}'," +
                    "'properties.sasl.kerberos.service.name' = '${propertiesSaslKerberosServiceName}'" +
                    ")";
    /**
     * SpecificOffset 参数
     */
    String CONCAT_CONFIG_SPECIFIC_OFFSET =
            ",'scan.startup.specific-offsets' = '${scanStartupSpecificOffsets}'" +
                    ")";
    /**
     * Timestamp 参数
     */
    String CONCAT_CONFIG_TIMESTAMP =
            ",'scan.startup.timestamp-millis' = '${scanStartupTimestampMillis}'" +
                    ")";
    /**
     * keyFormat 参数 json类型
     */
    String CONCAT_CONFIG_KEYFORMAT =
            ",'key.format' = '${keyFormat}'," +
                    "'key.fields' = '${keyFlields}'," +
                    "'key.json.ignore-parse-errors' = '${keyIgnoreParseErrors}'," +
                    "'key.json.fail-on-missing-field' = '${keyFailMissingField}'" +
                    ")";

    /**
     * sinkSemantic 参数
     */
    String CONCAT_CONFIG_SINKSEMANTIC =
            ",'sink.semantic' = '${sinkSemantic}')";

    /**
     * transactionTimeoutMs 参数
     */
    String CONCAT_CONFIG_TRANSACTIONTIMEOUTMS =
            ",'properties.transaction.timeout.ms' = '${transactionTimeoutMs}')";

    /**
     * keyFormat 参数 raw类型
     */
    String CONCAT_CONFIG_KEYFORMAT_RAW =
            ",'key.format' = '${keyFormat}'," +
                    "'key.fields' = '${keyFlields}'" +
                    ")";
    /**
     * json.fail-on-missing-field 参数 value.fields-include 参数
     */
    String CONCAT_CONFIG_FORMATJSON_VALUEINCLUDE =
            ",'json.fail-on-missing-field' = '${valueFailMissingField}'," +
                    "'value.fields-include' = '${valueFieldsInclude}'" +
                    ")";


    /**
     * 创建文件连接器作为source(json)-系统时间
     */
    String CREATE_SOURCE_FILESYSTEM_JSON = " create table ${outputTable} ( ${fields} " +
//            ",cloud_wise_proc_time as  proctime()" +
            " ) with (" +
            " 'connector' = 'filesystem'," +
            " 'format' = '${format}'," +
            " 'json.fail-on-missing-field' = '${jsonFailOnMissingField}'," +
            " 'json.ignore-parse-errors' = '${jsonIgnoreParseErrors}'," +
            " 'path' = '${fileSystem}://${filePath}'" +
            " )";
    /**
     * 创建文件连接器作为source(json)-系统时间
     */
    String CREATE_SOURCE_FILESYSTEM_JSON_EVENT_TIME = " create table ${outputTable} ( ${fields} " +
            " , cloud_wise_proc_time as  proctime()," +
            " cloud_wise_event_time as  ${cloudWiseEventTime}, " +
            " watermark for cloud_wise_event_time as cloud_wise_event_time - INTERVAL '${interval}' ${unit} " +
            " ) with (" +
            " 'connector' = 'filesystem'," +
            " 'format' = '${format}'," +
            " 'json.fail-on-missing-field' = '${jsonFailOnMissingField}'," +
            " 'json.ignore-parse-errors' = '${jsonIgnoreParseErrors}'," +
            " 'path' = '${fileSystem}://${filePath}'" +
            " )";

    /**
     * 创建文件连接器作为source(json)-系统时间
     */
    String CREATE_SOURCE_FILESYSTEM_JSON_EVENT_TIME_PK = " create table ${outputTable} ( ${fields} " +
            " , cloud_wise_proc_time as  proctime()," +
            " cloud_wise_event_time as  ${cloudWiseEventTime}, " +
            " watermark for cloud_wise_event_time as cloud_wise_event_time - INTERVAL '${interval}' ${unit}, " +
            " PRIMARY KEY(${primaryKeyFields}) NOT ENFORCED " +
            " ) with (" +
            " 'connector' = 'filesystem'," +
            " 'format' = '${format}'," +
            " 'json.fail-on-missing-field' = '${jsonFailOnMissingField}'," +
            " 'json.ignore-parse-errors' = '${jsonIgnoreParseErrors}'," +
            " 'path' = '${fileSystem}://${filePath}'" +
            " )";

    /**
     * 创建文件连接器作为source(csv)-系统时间
     */
    String CREATE_SOURCE_FILESYSTEM_CSV_PROC_TIME = " create table ${outputTable} ( ${fields} " +
//            "   `cloud_wise_event_time` AS `to_timestamp`(`FROM_UNIXTIME`(`${evnt_time}` / 1000))," +
//            "   WATERMARK FOR `cloud_wise_event_time` AS `cloud_wise_event_time` - INTERVAL '${interval}' ${unit}" +
            " )  with (" +
            " 'connector' = 'filesystem'," +
            " 'format' = '${format}'," +
            " 'csv.ignore-parse-errors' = '${csvIgnoreParseErrors}'," +
            " 'csv.fail-on-missing-field' = '${csvFailOnMissingField}'," +
            " 'csv.escape-character' = '\\'," +
            " 'path' = '${fileSystem}://${filePath}'" +
            " )";
    /**
     * 创建文件连接器作为source(csv)-事件时间
     */
    String CREATE_SOURCE_FILESYSTEM_CSV_EVENT_TIME = " create table ${outputTable} ( ${fields} " +
            "  ,cloud_wise_proc_time as  proctime()," +
            "   cloud_wise_event_time as  ${cloudWiseEventTime}," +
            " watermark for cloud_wise_event_time as cloud_wise_event_time - INTERVAL '${interval}' ${unit} " +
            " )  with (" +
            " 'connector' = 'filesystem'," +
            " 'format' = '${format}'," +
            " 'csv.ignore-parse-errors' = '${csvIgnoreParseErrors}'," +
            " 'csv.fail-on-missing-field' = '${csvFailOnMissingField}'," +
            " 'csv.escape-character' = '\\'," +
            " 'path' = '${fileSystem}://${filePath}'" +
            " )";

    /**
     * 创建文件连接器作为source(csv)-事件时间-维表，增加主键
     */
    String CREATE_SOURCE_FILESYSTEM_CSV_EVENT_TIME_PK = " create table  ( " +
            "`id` STRING,`num` STRING," +
            "`ct` STRING,`name` STRING," +
            "`debug_dd84eb13` BIGINT   ," +
            "cloud_wise_proc_time as  proctime(),   " +
            "cloud_wise_event_time as  to_timestamp(FROM_UNIXTIME(`debug_dd84eb13` / 1000)), " +
            "watermark for cloud_wise_event_time as cloud_wise_event_time - INTERVAL '1' HOUR,  " +
            "PRIMARY KEY(`id`) NOT ENFORCED  )  " +
            "with ( 'connector' = 'filesystem', " +
            "'format' = 'csv'," +
            " 'csv.ignore-parse-errors' = 'true', " +
            "'csv.fail-on-missing-field' = 'false', " +
            "'csv.escape-character' = '\\', " +
            "'path' = 'file:///data/app/dodpMangerService/debug/110/dd84eb13/d87955f8-ae29-4d5b-bc77-2fc723820fb8/es_weibiao_join.csv' )";



    public static void main(String[] args) {
        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(localEnvironment);
        try {
            streamTableEnvironment.executeSql(CREATE_SOURCE_FILESYSTEM_CSV_EVENT_TIME_PK);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 创建文件连接器作为source(json)
     */
    String CREATE_SINK_FILESYSTEM_JSON = " create table ${outputTable} ( ${fields} " +
            " ) with (" +
            " 'connector' = 'filesystem'," +
            " 'format' = 'json'," +
            " 'json.fail-on-missing-field' = '${jsonFailOnMissingField}'," +
            " 'json.ignore-parse-errors' = '${jsonIgnoreParseErrors}'," +
            " 'path' = '${fileSystem}://${filePath}'" +
            " )";
    /**
     * 创建文件连接器作为sink(csv)
     */
    String CREATE_SINK_FILESYSTEM_CSV = " create table ${outputTable} ( ${fields} " +
            " ) with (" +
            " 'connector' = 'filesystem'," +
            " 'format' = '${format}'," +
            " 'csv.ignore-parse-errors' = '${csvIgnoreParseErrors}'," +
            " 'path' = '${fileSystem}://${filePath}'" +
            " )";

    /**
     * 文件系统输出
     */
    String INSERT_OUTPUT_SINK_FILESYSTEM = " insert into ${outputTable} select ${fields} " +
            " from ${inputTable} ${condition}";

    /**
     * es 源表模版
     */
    String CREATE_SOURCE_ELASTICSEARCH =
            " create table ${outputTable} ( ${fields} \n" +
                    " ) with ( \n" +
                    "'connector'='elasticsearch-source',\n" +
                    "'hosts'='${hosts}',\n" +
                    "'index'= '${connectorIndex}',\n" +
                    "'document-type'= '${connectorDocumentType}',\n" +
                    "'username'= '${username}',\n" +
                    "'password'= '${password}',\n" +
                    "'fetch-size'='${fetchSize}',\n" +
                    "'format' = '${format}'\n" +
                    ")\n";

}
