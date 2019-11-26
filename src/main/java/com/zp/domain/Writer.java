package com.zp.domain;

import com.zp.utils.StringUtil;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil.isOB10;

@Slf4j
public abstract class Writer extends BaseObject {

    public String writeRecordSql;

    public String INSERT_OR_REPLACE_TEMPLATE;

    public Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

    public Writer(String filePath) {
        filterConfiguration(ConfigParser.parse(filePath));
        dBinfo = new DBinfo(configuration, OperateType.WRITER.getValue());
        createConnection();
        dealWriteMode();
    }

    public void startWrite(List<Record> recordList) throws SQLException {
        if (recordList == null || recordList.isEmpty()) {
            return;
        }
        long startTime = System.currentTimeMillis();
        this.resultSetMetaData = com.alibaba.datax.plugin.rdbms.util.DBUtil.getColumnMetaData(getConnection(), this.dBinfo.getTable(), StringUtils.join(this.dBinfo.getColumns(), ","));
        this.calcWriteRecordSql();

        Iterator<Record> recordIterator = recordList.iterator();

        List<Record> writeBuffer = new ArrayList<>(StringUtil.batchSize);
        int bufferBytes = 0;

        try {
            Record record;
            while (recordIterator.hasNext()) {
                record = recordIterator.next();
                writeBuffer.add(record);
                bufferBytes += record.getMemorySize();
                if (writeBuffer.size() >= StringUtil.batchSize || bufferBytes >= StringUtil.batchByteSize) {
                    this.doBatchInsert(getConnection(), writeBuffer);
                    writeBuffer.clear();
                    bufferBytes = 0;
                }
            }

            if (!writeBuffer.isEmpty()) {
                this.doBatchInsert(getConnection(), writeBuffer);
                writeBuffer.clear();
            }
        } catch (Exception var10) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, var10);
        } finally {
            writeBuffer.clear();
            com.alibaba.datax.plugin.rdbms.util.DBUtil.closeDBResources((ResultSet) null, (Statement) null, connection);
            log.debug("写入：{} 条记录,耗时：{} ms", recordList.size(), (System.currentTimeMillis() - startTime));
        }

    }

    protected void doBatchInsert(Connection connection, List<Record> buffer) throws SQLException {
        PreparedStatement preparedStatement = null;

        try {
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(this.writeRecordSql);
            Iterator i$ = buffer.iterator();

            while (i$.hasNext()) {
                Record record = (Record) i$.next();
                preparedStatement = fillPreparedStatement(preparedStatement, record);
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException var10) {
            log.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + var10.getMessage());
            connection.rollback();
            this.doOneInsert(connection, buffer);
        } catch (Exception var11) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, var11);
        } finally {
            com.alibaba.datax.plugin.rdbms.util.DBUtil.closeDBResources(preparedStatement, (Connection) null);
        }

    }


    protected void doOneInsert(Connection connection, List<Record> buffer) {
        PreparedStatement preparedStatement = null;

        try {
            connection.setAutoCommit(true);
            preparedStatement = connection.prepareStatement(this.writeRecordSql);
            Iterator i$ = buffer.iterator();

            while (i$.hasNext()) {
                Record record = (Record) i$.next();

                try {
                    preparedStatement = fillPreparedStatement(preparedStatement, record);
                    preparedStatement.execute();
                } catch (SQLException var17) {
                    log.error("{}", var17.getMessage());
                } finally {
                    preparedStatement.clearParameters();
                }
            }
        } catch (Exception var19) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, var19);
        } finally {
            com.alibaba.datax.plugin.rdbms.util.DBUtil.closeDBResources(preparedStatement, (Connection) null);
        }

    }


    protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record) throws SQLException {
        for (int i = 0; i < this.dBinfo.getColumnNumber(); ++i) {
            int columnSqltype = (Integer) ((List) this.resultSetMetaData.getMiddle()).get(i);
            preparedStatement = this.fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, record.getColumn(i));
        }

        return preparedStatement;
    }

    protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqltype, Column column) throws SQLException {
        java.util.Date utilDate;
        switch (columnSqltype) {
            case -16:
            case -15:
            case -9:
            case -1:
            case 1:
            case 12:
            case 2005:
            case 2011:
                preparedStatement.setString(columnIndex + 1, column.asString());
                break;
            case -7:
                if (this.dBinfo.getDataBaseType() == DataBaseType.MySql) {
                    preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                } else {
                    preparedStatement.setString(columnIndex + 1, column.asString());
                }
                break;
            case -6:
                Long longValue = column.asLong();
                if (null == longValue) {
                    preparedStatement.setString(columnIndex + 1, (String) null);
                } else {
                    preparedStatement.setString(columnIndex + 1, longValue.toString());
                }
                break;
            case -5:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
                String strValue = column.asString();
                if (this.dBinfo.isEmptyAsNull() && "".equals(strValue)) {
                    preparedStatement.setString(columnIndex + 1, (String) null);
                } else {
                    preparedStatement.setString(columnIndex + 1, strValue);
                }
                break;
            case -4:
            case -3:
            case -2:
            case 2004:
                preparedStatement.setBytes(columnIndex + 1, column.asBytes());
                break;
            case 16:
                preparedStatement.setString(columnIndex + 1, column.asString());
                break;
            case 91:
                if (((String) ((List) this.resultSetMetaData.getRight()).get(columnIndex)).equalsIgnoreCase("year")) {
                    if (column.asBigInteger() == null) {
                        preparedStatement.setString(columnIndex + 1, (String) null);
                    } else {
                        preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
                    }
                } else {
                    Date sqlDate = null;

                    try {
                        utilDate = column.asDate();
                    } catch (DataXException var13) {
                        throw new SQLException(String.format("Date 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlDate = new Date(utilDate.getTime());
                    }

                    preparedStatement.setDate(columnIndex + 1, sqlDate);
                }
                break;
            case 92:
                Time sqlTime = null;

                try {
                    utilDate = column.asDate();
                } catch (DataXException var12) {
                    throw new SQLException(String.format("TIME 类型转换错误：[%s]", column));
                }

                if (null != utilDate) {
                    sqlTime = new Time(utilDate.getTime());
                }

                preparedStatement.setTime(columnIndex + 1, sqlTime);
                break;
            case 93:
                Timestamp sqlTimestamp = null;

                try {
                    utilDate = column.asDate();
                } catch (DataXException var11) {
                    throw new SQLException(String.format("TIMESTAMP 类型转换错误：[%s]", column));
                }

                if (null != utilDate) {
                    sqlTimestamp = new Timestamp(utilDate.getTime());
                }

                preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, String.format("您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.", ((List) this.resultSetMetaData.getLeft()).get(columnIndex), ((List) this.resultSetMetaData.getMiddle()).get(columnIndex), ((List) this.resultSetMetaData.getRight()).get(columnIndex)));
        }

        return preparedStatement;
    }

    public void calcWriteRecordSql() {
        if (!"?".equals(this.calcValueHolder(""))) {
            List<String> valueHolders = new ArrayList(this.dBinfo.getColumnNumber());

            for (int i = 0; i < this.dBinfo.getColumnNumber(); ++i) {
                String type = (String) ((List) this.resultSetMetaData.getRight()).get(i);
                valueHolders.add(this.calcValueHolder(type));
            }

            boolean forceUseUpdate = false;
            if (this.dBinfo.getDataBaseType() != null && this.dBinfo.getDataBaseType() == DataBaseType.MySql && isOB10(this.dBinfo.getJdbcUrl())) {
                forceUseUpdate = true;
            }

            INSERT_OR_REPLACE_TEMPLATE = WriterUtil.getWriteTemplate(this.dBinfo.getColumns(), valueHolders, this.dBinfo.getWriteMode(), this.dBinfo.getDataBaseType(), forceUseUpdate);
            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.dBinfo.getTable());
        }
    }

    public String calcValueHolder(String columnType) {
        return "?";
    }

    public void dealWriteMode() {
        List<String> columns = this.dBinfo.getColumns();
        String jdbcUrl = this.dBinfo.getJdbcUrl();
        List<String> valueHolders = new ArrayList(columns.size());

        for (int i = 0; i < columns.size(); ++i) {
            valueHolders.add("?");
        }
        boolean forceUseUpdate = false;
        if (this.dBinfo.getDataBaseType() == DataBaseType.MySql && isOB10(jdbcUrl)) {
            forceUseUpdate = true;
        }
        INSERT_OR_REPLACE_TEMPLATE = getWriteTemplate(columns, valueHolders, this.dBinfo.getWriteMode(), this.dBinfo.getDataBaseType(), forceUseUpdate);
        this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.dBinfo.getTable());
    }

    public static String getWriteTemplate(List<String> columnHolders, List<String> valueHolders, String writeMode, DataBaseType dataBaseType, boolean forceUseUpdate) {
        boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert") || writeMode.trim().toLowerCase().startsWith("replace") || writeMode.trim().toLowerCase().startsWith("update");
        if (!isWriteModeLegal) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, String.format("您所配置的 writeMode:%s 错误. 因为DataX 目前仅支持replace,update 或 insert 方式. 请检查您的配置并作出修改.", writeMode));
        } else {
            String writeDataSqlTemplate;
            if (forceUseUpdate || (dataBaseType == DataBaseType.MySql || dataBaseType == DataBaseType.Tddl) && writeMode.trim().toLowerCase().startsWith("update")) {
                writeDataSqlTemplate = "INSERT INTO %s (" + StringUtils.join(columnHolders, ",") + ") VALUES(" + StringUtils.join(valueHolders, ",") + ")" + onDuplicateKeyUpdateString(columnHolders);
            } else {
                if (writeMode.trim().toLowerCase().startsWith("update")) {
                    writeMode = "replace";
                }

                writeDataSqlTemplate = writeMode + " INTO %s (" + StringUtils.join(columnHolders, ",") + ") VALUES(" + StringUtils.join(valueHolders, ",") + ")";
            }

            return writeDataSqlTemplate;
        }
    }

    public static String onDuplicateKeyUpdateString(List<String> columnHolders) {
        if (columnHolders != null && columnHolders.size() >= 1) {
            StringBuilder sb = new StringBuilder();
            sb.append(" ON DUPLICATE KEY UPDATE ");
            boolean first = true;
            Iterator i$ = columnHolders.iterator();

            while(i$.hasNext()) {
                String column = (String)i$.next();
                if (!first) {
                    sb.append(",");
                } else {
                    first = false;
                }

                sb.append(column);
                sb.append("=VALUES(");
                sb.append(column);
                sb.append(")");
            }

            return sb.toString();
        } else {
            return "";
        }
    }

}
