package org.example.fllikpartbook.chapter_6;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @Author: john
 * @Date: 2022-10-10-14:44
 * @Description:
 */
public class ReadCsvDemoV1 {


    public static void main(String[] args) throws Exception {
        readCsvDemo();
    }


    public static void readCsvDemo() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        CsvReader csvReader = environment.readCsvFile("C:\\fp-rmsz\\src\\main\\resources\\data.csv");
        DataSource<User> userDataSource = csvReader.fieldDelimiter(",")
                .ignoreFirstLine()
                .includeFields(true, false, true)
                .pojoType(User.class, "name", "age");
        userDataSource.print();
    }



}
