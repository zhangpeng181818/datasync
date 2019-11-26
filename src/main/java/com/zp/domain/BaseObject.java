package com.zp.domain;

import com.alibaba.datax.common.util.Configuration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class BaseObject {

    public static String QUERY_SQL_MAX_COLUMN = "SELECT MAX(%s) AS maxVal FROM %s";

    public static String QUERY_SQL_VALID_ID = "SELECT DISTINCT %s  FROM  %s WHERE %s = '%s'";

    public static String QUERY_SQL_VALID_IDS = "SELECT DISTINCT %s FROM %s WHERE ( %s )";

    public static String QUERY_SQL_VALID_ORDERS = "SELECT %s FROM %s WHERE %s = '%s' and %s IN ( %s )";

    public static String QUERY_SQL_VALID_LIS_ITEMS_RESULT = "SELECT %s FROM %s WHERE %s  AND %s >= ( %s ) and %s >= ( %s ) AND %s IN ( %s )";

    public static String QUERY_SQL_TEMPLATE = "SELECT %s FROM %s WHERE ( %s )";

    public static String QUERY_SQL_TODAY = "SELECT cast(convert(varchar(8),getdate(),112) AS int)";


    public DBinfo dBinfo;

    public Configuration configuration;

    public Connection connection;


    public void filterConfiguration(Configuration configure) {
        Configuration jobConfWithSetting = configure.getConfiguration("job").clone();
        String content = jobConfWithSetting.getConfiguration("content[0]").toJSON();
        configuration = Configuration.from(content);
    }

    public Connection getConnection() {
        try {
            return connection.isClosed() || !connection.isValid(1000) ? createConnection() : connection;
        } catch (SQLException e) {
            return createConnection();
        }
    }

    public Connection createConnection() {
        connection = com.alibaba.datax.plugin.rdbms.util.DBUtil.getConnection(
                dBinfo.getDataBaseType(),
                dBinfo.getJdbcUrl(),
                dBinfo.getUsername(),
                dBinfo.getPassword());
        return connection;
    }

    public void closeConnection() {
        com.alibaba.datax.plugin.rdbms.util.DBUtil.closeDBResources((ResultSet) null, (Statement) null, connection);
    }

    public int queryColumnMaxValue(String column) throws SQLException {
        String sql = QUERY_SQL_MAX_COLUMN.replaceFirst("%s", column).replaceFirst("%s", dBinfo.getTable());
        ResultSet rs = com.alibaba.datax.plugin.rdbms.util.DBUtil.query(getConnection(), sql);
        while (rs.next()) {
            return rs.getInt(1);
        }
        return 0;
    }

    public List<Integer> getVaildIds(String sql) throws SQLException {
        ResultSet rs = com.alibaba.datax.plugin.rdbms.util.DBUtil.query(getConnection(), sql);
        List<Integer> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getInt(1));
        }
        return ids;
    }
}
