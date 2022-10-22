package org.example.db;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

/**
 * @Author: john
 * @Date: 2022-10-13-13:10
 * @Description:
 */
public class DbUtil {

    public static void main(String[] args) throws SQLException {
        List<Entity> all = Db.use().findAll(Entity.create("biz_data_store").set("is_deleted", 0));
        for (Entity entity : all) {
            JSONObject eneityDb = JSONUtil.parseObj(entity.getStr("store_conf_json"));

            if (Objects.isNull(eneityDb.getStr("engine"))){
                eneityDb.set("engine","MergeTree");
                Db.use().execute("update biz_data_store set store_conf_json = ? where id = ?",eneityDb.toString(),entity.getLong("id"));
            }
        }
        System.out.println(JSONUtil.toJsonPrettyStr(all));

    }
}
