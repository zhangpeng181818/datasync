package com.zp.domain;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.fastjson.JSONArray;
import lombok.Data;

import java.util.List;

@Data
public class DBinfo {

    private String username;
    private String password;
    private String jdbcUrl;
    private String table;
    private DataBaseType dataBaseType;
    private List<String> columns;
    private String writeMode;
    private int batchSize = 2048;
    private int batchByteSize = 33554432;
    private boolean emptyAsNull = true;
    private int columnNumber;
    private String where;


    public DBinfo(Configuration configuration, String operate) {
        if (OperateType.WRITER.getValue().equalsIgnoreCase(operate)) {
            username = configuration.getString("writer.parameter.username");
            password = configuration.getString("writer.parameter.password");
            jdbcUrl = configuration.getString("writer.parameter.connection[0].jdbcUrl");
            table = configuration.getString("writer.parameter.connection[0].table[0]");
            dataBaseType = parseDataBaseType(configuration.getString("writer.name"));
            columns = configuration.get("writer.parameter.column", JSONArray.class).toJavaList(String.class);
            columnNumber = columns == null ? 0 : columns.size();
            writeMode = configuration.getString("writer.parameter.writeMode");
        } else {
            username = configuration.getString("reader.parameter.username");
            password = configuration.getString("reader.parameter.password");
            jdbcUrl = configuration.getString("reader.parameter.connection[0].jdbcUrl[0]");
            table = configuration.getString("reader.parameter.connection[0].table[0]");
            dataBaseType = parseDataBaseType(configuration.getString("reader.name"));
            columns = configuration.get("reader.parameter.column", JSONArray.class).toJavaList(String.class);
            columnNumber = columns == null ? 0 : columns.size();
            where = configuration.getString("reader.parameter.where");
        }
    }

    private DataBaseType parseDataBaseType(String name) {
        for (DataBaseType val : DataBaseType.values()) {
            if (name.contains(val.getTypeName())) {
                return val;
            }
        }
        return null;
    }
}