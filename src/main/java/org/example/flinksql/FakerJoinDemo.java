package org.example.flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class FakerJoinDemo {

    public static void main(String[] args) {
        EnvironmentSettings build = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(build);
        tableEnvironment.executeSql("CREATE TABLE NOC (\n" +
                "  agent_id STRING,\n" +
                "  codename STRING\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'faker',\n" +
                "  'fields.agent_id.expression' = '#{regexify ''(1|2|3|4|5){1}''}',\n" +
                "  'fields.codename.expression' = '#{superhero.name}',\n" +
                "  'number-of-rows' = '10'\n" +
                ")");

        tableEnvironment.executeSql("CREATE TABLE RealNames (\n" +
                "  agent_id STRING,\n" +
                "  name     STRING\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'faker',\n" +
                "  'fields.agent_id.expression' = '#{regexify ''(1|2|3|4|5){1}''}',\n" +
                "  'fields.name.expression' = '#{Name.full_name}',\n" +
                "  'number-of-rows' = '10'\n" +
                ")\n");

        TableResult tableResult = tableEnvironment.executeSql("SELECT\n" +
                "    name,\n" +
                "    codename\n" +
                "FROM NOC\n" +
                "INNER JOIN RealNames ON NOC.agent_id = RealNames.agent_id");
        tableResult.print();
    }
}
