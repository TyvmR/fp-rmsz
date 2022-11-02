/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package myflink.sql;

import myflink.pojo.MyOrder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;

import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.$;
/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class SQLBatchDemo {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		DataSet<MyOrder> input = env.fromElements(
			new MyOrder(1L,"BMW", 1),
			new MyOrder(2L,"Tesla", 8),
			new MyOrder(2L,"Tesla", 8),
			new MyOrder(3L,"Rolls-Royce", 20));

		//注册DataSet为view
	 tEnv.createTemporaryView("MyOrder", input,$("id"),$("product"), $("amount"));

		//执行SQL
		Table table = tEnv.sqlQuery(
			"SELECT product,SUM(amount) as amount FROM MyOrder GROUP BY product");

		tEnv.toDataSet(table, Row.class).print();
	}

}
