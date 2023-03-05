package org.example.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class SqlFakerExp {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        String topN = "CREATE TABLE spells_cast (\n" +
                "    wizard STRING,\n" +
                "    spell  STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'faker',\n" +
                "  'fields.wizard.expression' = '#{harry_potter.characters}',\n" +
                "  'fields.spell.expression' = '#{harry_potter.spells}',\n" +
                "  'rows-per-second' = '1'\n" +
                ")";

        String selectTopN = "SELECT wizard, spell, times_cast\n" +
                "FROM (\n" +
                "    SELECT *,\n" +
                "    ROW_NUMBER() OVER (PARTITION BY wizard ORDER BY times_cast DESC) AS row_num\n" +
                "    FROM (SELECT wizard, spell, COUNT(*) AS times_cast FROM spells_cast GROUP BY wizard, spell)\n" +
                ")\n" +
                "WHERE row_num <= 2";
        tableEnvironment.executeSql(topN);

        TableResult tableResult = tableEnvironment.executeSql(selectTopN);
        tableResult.print();
    }
}
