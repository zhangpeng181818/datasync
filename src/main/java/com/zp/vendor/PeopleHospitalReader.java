package com.zp.vendor;

import com.zp.domain.Reader;
import com.zp.utils.StringUtil;
import com.alibaba.datax.common.element.Record;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

@Slf4j
public class PeopleHospitalReader extends Reader {

    private String key = "\\$max_order_no";

    public PeopleHospitalReader(String filePath) {
        super(filePath);
    }

    public List<Record> incrementQueryOrders(String column) throws SQLException {
        String sql = QUERY_SQL_TEMPLATE.replaceFirst("%s", StringUtil.replace(dBinfo.getColumns().toString()))
                .replaceFirst("%s", dBinfo.getTable())
                .replaceFirst("%s", dBinfo.getWhere())
                .replaceAll(key, column);
        log.debug("【incrementQueryOrders】：【{}】", sql);
        return query(sql);
    }

    public List<Record> queryValidLisItemsResult(String column, String column2, String column3, String column4) throws SQLException {
        String sql = QUERY_SQL_VALID_LIS_ITEMS_RESULT.replaceFirst("%s", StringUtil.replace(dBinfo.getColumns().toString()))
                .replaceFirst("%s", dBinfo.getTable())
                .replaceFirst("%s", dBinfo.getWhere())
                .replaceFirst("%s", column)
                .replaceFirst("%s", StringUtil.getFormatDate("yyyyMMdd", new Date()))
                .replaceFirst("%s", column2)
                .replaceFirst("%s", StringUtil.getFormatDate("HHmm", new Date()))
                .replaceFirst("%s", column3)
                .replaceFirst("%s", column4);
        log.debug("【queryValidLisItemsResult】：【{}】", sql);
        return query(sql);
    }

    public List<Record> queryPatientList() throws SQLException {
        String sql = QUERY_SQL_TEMPLATE.replaceFirst("%s", StringUtil.replace(dBinfo.getColumns().toString()))
                .replaceFirst("%s", dBinfo.getTable())
                .replaceFirst("%s", dBinfo.getWhere());
        log.debug("【queryPatientList】：【{}】", sql);
        return query(sql);
    }

    public List<Record> queryPatientTransList() throws SQLException {
        String sql = QUERY_SQL_TEMPLATE.replaceFirst("%s", StringUtil.replace(dBinfo.getColumns().toString()))
                .replaceFirst("%s", dBinfo.getTable())
                .replaceFirst("%s", dBinfo.getWhere());
        log.debug("【queryPatientTransList】：【{}】", sql);
        return query(sql);
    }
}
