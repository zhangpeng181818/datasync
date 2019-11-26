package com.zp.vendor;

import com.zp.domain.Writer;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class PeopleHospitalWriter extends Writer {

    public PeopleHospitalWriter(String filePath) {

        super(filePath);
    }

    public List<Integer> queryValidIds(String column, String column2, String column3) throws SQLException {
        long startTime = System.currentTimeMillis();
        String sql = QUERY_SQL_VALID_ID.replaceFirst("%s", column)
                .replaceFirst("%s", dBinfo.getTable())
                .replaceFirst("%s", column2)
                .replaceFirst("%s", column3);
        log.debug("【queryValidIds】：【{}】", sql);
        ResultSet rs = com.alibaba.datax.plugin.rdbms.util.DBUtil.query(getConnection(), sql);
        List<Integer> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getInt(1));
        }
        log.debug("查询 {} 条记录，耗时：{} ms", ids.size(), (System.currentTimeMillis() - startTime));
        return ids;
    }
}
