package myflink;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.*;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator.CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY;

public class CatalogDemo {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 		TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

// 创建一个内存Catalog
		Catalog catalog = new GenericInMemoryCatalog(GenericInMemoryCatalog.DEFAULT_DB);

// 注册Catalog
		tableEnv.registerCatalog("myCatalog", catalog);
		HashMap<String, String> hashMap = new HashMap<String, String>();
			hashMap.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY); //
			hashMap.put(CATALOG_PROPERTY_VERSION, "1"); //

// 创建一个Catalog数据库
		catalog.createDatabase("myDb", new CatalogDatabaseImpl(hashMap,"comment"),false);

//创建一个Catalog表
		TableSchema schema = TableSchema.builder()
				.field("name", DataTypes.STRING())
				.field("age", DataTypes.INT())
				.build();

		catalog.createTable(
				new ObjectPath("myDb","mytable"),
				new CatalogTableImpl(schema,hashMap, CATALOG_PROPERTY_VERSION),false);
		catalog.createTable(
				new ObjectPath("myDb","mytable2"),
				new CatalogTableImpl(schema,hashMap, CATALOG_PROPERTY_VERSION),false);

		List<String> tables = catalog.listTables("myDb"); // tables should contain "mytable"
		System.out.println("表信息："+tables.toString());
	}
}
