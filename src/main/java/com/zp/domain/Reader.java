package com.zp.domain;

import com.zp.utils.StringUtil;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public abstract class Reader extends BaseObject {


    public final byte[] EMPTY_CHAR_ARRAY = {};

    public String mandatoryEncoding;

    public Class<? extends Record> RECORD_CLASS;

    public Reader(String filePath){
        filterConfiguration(ConfigParser.parse(filePath));
        dBinfo = new DBinfo(configuration, OperateType.READER.getValue());
        createConnection();
    }



    public List<Record> queryValidOrders(String column, String column2) throws SQLException {
        String sql = QUERY_SQL_VALID_ORDERS.replaceFirst("%s", StringUtil.replace(dBinfo.getColumns().toString()))
                .replaceFirst("%s", dBinfo.getTable())
                .replaceFirst("%s","dept_code")
                .replaceFirst("%s","311")
                .replaceFirst("%s", column)
                .replaceFirst("%s", column2);
        log.debug("【queryValidOrders】：【{}】", sql);
        return query(sql);
    }

    public List<Record> incrementQueryOrders(String column) throws SQLException {
        String sql = QUERY_SQL_TEMPLATE.replaceFirst("%s", StringUtil.replace(dBinfo.getColumns().toString()))
                .replaceFirst("%s", dBinfo.getTable())
                .replaceFirst("%s", dBinfo.getWhere());
        log.debug("【incrementQueryOrders】：【{}】", sql);
        return query(sql);
    }

    public List<Record> query(String sql) throws SQLException {
        long startTime = System.currentTimeMillis();
        ResultSet rs = com.alibaba.datax.plugin.rdbms.util.DBUtil.query(getConnection(), sql);
        List<Record> list = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnNumber = metaData.getColumnCount();

        while (rs.next()) {
            list.add(transportOneRecord(rs, metaData, columnNumber, mandatoryEncoding));
        }
        log.debug("查询 {} 条记录，耗时：{} ms", list.size(), (System.currentTimeMillis() - startTime));
        return list;
    }

    public Record createRecord() {
        try {
            RECORD_CLASS = (Class<? extends Record>) Class.forName("com.alibaba.datax.core.transport.record.DefaultRecord");
            return (Record) RECORD_CLASS.newInstance();
        } catch (Exception var2) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, var2);
        }
    }

    public Record transportOneRecord(ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding) {
        return buildRecord(rs, metaData, columnNumber, mandatoryEncoding);
    }

    public Record buildRecord(ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding) {
        Record record = createRecord();

        try {
            for (int i = 1; i <= columnNumber; ++i) {
                switch (metaData.getColumnType(i)) {
                    case -16:
                    case -15:
                    case -9:
                    case -1:
                    case 1:
                    case 12:
                        String rawData;
                        if (StringUtils.isBlank(mandatoryEncoding)) {
                            rawData = rs.getString(i);
                        } else {
                            rawData = new String(rs.getBytes(i) == null ? this.EMPTY_CHAR_ARRAY : rs.getBytes(i), mandatoryEncoding);
                        }

                        record.addColumn(new StringColumn(rawData));
                        break;
                    case -7:
                    case 16:
                        record.addColumn(new BoolColumn(rs.getBoolean(i)));
                        break;
                    case -6:
                    case -5:
                    case 4:
                    case 5:
                        record.addColumn(new LongColumn(rs.getString(i)));
                        break;
                    case -4:
                    case -3:
                    case -2:
                    case 2004:
                        record.addColumn(new BytesColumn(rs.getBytes(i)));
                        break;
                    case 0:
                        String stringData = null;
                        if (rs.getObject(i) != null) {
                            stringData = rs.getObject(i).toString();
                        }

                        record.addColumn(new StringColumn(stringData));
                        break;
                    case 2:
                    case 3:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;
                    case 6:
                    case 7:
                    case 8:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;
                    case 91:
                        if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                            record.addColumn(new LongColumn(rs.getInt(i)));
                        } else {
                            record.addColumn(new DateColumn(rs.getDate(i)));
                        }
                        break;
                    case 92:
                        record.addColumn(new DateColumn(rs.getTime(i)));
                        break;
                    case 93:
                        record.addColumn(new DateColumn(rs.getTimestamp(i)));
                        break;
                    case 2005:
                    case 2011:
                        record.addColumn(new StringColumn(rs.getString(i)));
                        break;
                    default:
                        throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, String.format("您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .", metaData.getColumnName(i), metaData.getColumnType(i), metaData.getColumnClassName(i)));
                }
            }
        } catch (Exception var11) {
            log.error("{}", var11.getMessage());
        }

        return record;
    }

}
