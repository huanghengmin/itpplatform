package com.hzih.itp.platform.dbchange.source.plugin;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.dbchange.DbInit;
import com.hzih.itp.platform.dbchange.datautils.ArrayListInputStream;
import com.hzih.itp.platform.dbchange.datautils.MDataConstructor;
import com.hzih.itp.platform.dbchange.datautils.db.Column;
import com.hzih.itp.platform.dbchange.datautils.db.Row;
import com.hzih.itp.platform.dbchange.datautils.db.Rows;
import com.hzih.itp.platform.dbchange.datautils.db.pk.PkSet;
import com.hzih.itp.platform.dbchange.datautils.dboperator.DbopUtil;
import com.hzih.itp.platform.dbchange.exception.EXSql;
import com.hzih.itp.platform.dbchange.source.SourceOperation;
import com.hzih.itp.platform.dbchange.source.info.DatabaseInfo;
import com.hzih.itp.platform.dbchange.source.info.TableInfo;
import com.hzih.itp.platform.dbchange.source.utils.SourceObjectCache;
import com.hzih.itp.platform.dbchange.target.info.ColumnMap;
import com.hzih.itp.platform.dbchange.target.info.TableMap;
import com.hzih.itp.platform.dbchange.target.utils.StringUtil;
import com.hzih.itp.platform.utils.DataAttributes;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.Jdbc;
import com.inetec.common.config.stp.nodes.Type;
import com.inetec.common.exception.E;
import com.inetec.common.exception.Ex;
import com.inetec.common.i18n.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;

/**
 * Created by 钱晓盼 on 14-1-14.
 */
public class SourceProcessDelete extends DbInit implements ISourceProcess {
    final static Logger logger = LoggerFactory.getLogger(SourceProcessDelete.class);
    private boolean isRun = false;
    private SourceOperation source;
    private DatabaseInfo databaseInfo;
    private Type type;
    private Jdbc jdbc;
    private String appName;
    private TableInfo[] m_tableInfos = new TableInfo[0];
    private SourceObjectCache m_objectCache;

    @Override
    public DataAttributes process(InputStream in) {
        DataAttributes dataAttributes = new DataAttributes();
        return source.process(in,dataAttributes);
    }

    @Override
    public boolean process(byte[] data) {
        return false;
    }

    @Override
    public void init(SourceOperation source, DatabaseInfo databaseInfo) {
        this.source = source;
        this.databaseInfo = databaseInfo;
        this.type = source.getType();
        this.appName = type.getTypeName();
        setTableInfo();
        this.m_objectCache = new SourceObjectCache();
        try {
            m_dbName = databaseInfo.getName();
            jdbc = ChangeConfig.ichange.getJdbc(m_dbName);
            initDbSource();
            m_schemaName = jdbc.getDbOwner();
        } catch (Ex ex) {
            LogLayout.error(logger, appName, "initDbSource error", ex);
        }
    }

    private void setTableInfo() {
        m_tableInfos = databaseInfo.getTableInfo();
    }

    @Override
    public void stop() {
        isRun = false;
    }

    @Override
    public boolean isRun() {
        return isRun;
    }

    @Override
    public void run() {
        isRun = true;
        while (isRun) {
            exportOldData();
            LogLayout.info(logger, appName, "完成一次删除同步");
            try {
                Thread.sleep(databaseInfo.getInterval() * 1000);
            } catch (InterruptedException ie) {
            }
        }
    }

    private void exportOldData() {
        int tableSize = m_tableInfos.length;
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger, appName, "开始导出源表数据, 源表个数为: " + tableSize);
        }
        testStatus();
        for (int i = 0; i < tableSize; i++) {
            TableInfo tableInfo = m_tableInfos[i];
            long nextTime = tableInfo.getNeExtTime();
            if (System.currentTimeMillis() >= nextTime) {
                String tableName = m_tableInfos[i].getName();
                try {
                    exportWholeTable(tableInfo,tableName);
                    tableInfo.nextTime();
                } catch (Ex Ex) {
                    Message Message = new Message("导出源表 {0} 数据出错.", tableName);
                    LogLayout.error(logger, appName, Message.toString(), Ex);
                }
            }
        }
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"完成源表数据的导出.");
        }
        return;
    }


    private void exportWholeTable(TableInfo tableInfo,String tableName) throws Ex {
        if (tableInfo == null) {
            throw new Ex().set(E.E_ObjectNotFound, new Message("The table name {0} is not configed", tableName));
        }
        Column[] clobColumns = tableInfo.getClobColumns();
        Column[] blobColumns = tableInfo.getBlobColumns();
        boolean hasBlob = blobColumns.length > 0;
        boolean hasClob = clobColumns.length > 0;
        boolean hasLob = hasBlob || hasClob;
        boolean hasBothBlobAndClob = hasBlob && hasClob;

        Column[] basicColumns = tableInfo.getBasicColumns();

        String strBasicQuery = null;
        if (hasLob) {
            Column[] columns = null;
            Column[] lobColumn = null;
            if (hasBothBlobAndClob) {
                lobColumn = addTwoTypeColumns(blobColumns, clobColumns);
            } else {
                if (hasBlob) {
                    lobColumn = blobColumns;
                } else if (hasClob) {
                    lobColumn = clobColumns;
                }
            }
            columns = addTwoTypeColumns(basicColumns, lobColumn);
            strBasicQuery = getTableQueryString(tableInfo,tableName, columns);
        } else {
            strBasicQuery = getTableQueryString(tableInfo,tableName, basicColumns);
        }
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"query table sql string: " + strBasicQuery);
        }
        int n = 0;
        do {
            n = processMaxRecords(strBasicQuery, tableInfo, databaseInfo.getMaxRecord());
        } while (n > 0);
    }

    private int processMaxRecords(String sql, TableInfo tableInfo, int max) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        testStatus();
        Connection conn = connection(appName);
        Statement stmt = null;
        try {
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(max);
            stmt.setMaxRows(max);
            ResultSet rs = stmt.executeQuery(sql);
            multiData = dataConstructor(rs, tableInfo, max);
            if (rs != null) {
                rs.close();
            }
            if (multiData.getData().getRowArray().length > 0) {
                disposeData(conn,tableInfo,multiData.getDataInputStream(), multiData.getData().getRowArray());
                return multiData.getData().getRowArray().length;
            } else {
                return multiData.getData().getRowArray().length;
            }
        } catch (SQLException e) {
            LogLayout.error(logger,appName,"An error occured while exporting the table" + tableInfo.getName(),e);
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
        return 0;
    }

    private MDataConstructor dataConstructor(ResultSet rs,
                    TableInfo tableInfo, int max) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        MDataConstructor m_dataConstructor = new MDataConstructor();
        MDataConstructor m_result = new MDataConstructor();
        Rows basicRows = new Rows();
        String tableName = tableInfo.getName();
        Column[] clobColumns = tableInfo.getClobColumns();
        Column[] blobColumns = tableInfo.getBlobColumns();
        boolean hasBlob = blobColumns.length > 0;
        boolean hasClob = clobColumns.length > 0;
        boolean hasBothBlobAndClob = hasBlob && hasClob;
        PkSet pks = null;
        try {
            for (int x = 0; x < max && rs.next(); x++) {
                Row row = getRowData(tableInfo, rs);
                pks = new PkSet(PkSet.getPks(row));

                //TODO 判断是否存在相同 pk 的记录
//                boolean isExistRow = basicRows.isExistRow(row);

                basicRows.addRow(row);

                int j = tableInfo.getBasicColumns().length + 1;
                if (hasBothBlobAndClob) {

                    for (int i = 0; i < blobColumns.length; i++) {
                        Column c = blobColumns[i].copyColumnWithoutValue();
                        c = m_I_dbOp.getColumnData(c, rs, j);
                        if (!c.isNull()) {
                            multiData.addBlobData(databaseInfo.getName(), tableName, c.getName(), pks, c.getValue().getInputStream(), c.getValue().getInputStreamLength());
                        } else {
                            multiData.addBlobData(databaseInfo.getName(), tableName, c.getName(), pks, null, 0);
                        }
                        j++;
                    }
                    if (logger.isDebugEnabled()) {
                        LogLayout.debug(logger,appName,"Lob column index: " + j);
                    }
                    for (int i = 0; i < clobColumns.length; i++) {
                        Column c = clobColumns[i].copyColumnWithoutValue();
                        c = m_I_dbOp.getColumnData(c, rs, j);
                        if (!c.isNull()) {
                            multiData.addClobData(databaseInfo.getName(), tableName, c.getName(), databaseInfo.getEncoding(), pks, c.getValue().getReader(), c.getValue().getReaderLength());
                        } else {
                            multiData.addClobData(databaseInfo.getName(), tableName, c.getName(), databaseInfo.getEncoding(), pks, null, 0);
                        }
                        j++;
                    }
                } else {
                    if (hasBlob) {
                        for (int i = 0; i < blobColumns.length; i++) {
                            Column c = blobColumns[i].copyColumnWithoutValue();
                            c = m_I_dbOp.getColumnData(c, rs, j);
                            if (!c.isNull()) {
                                multiData.addBlobData(databaseInfo.getName(), tableName, c.getName(), pks, c.getValue().getInputStream(), c.getValue().getInputStreamLength());
                            } else {
                                multiData.addBlobData(databaseInfo.getName(), tableName, c.getName(), pks, null, 0);
                            }
                            j++;
                        }
                    } else if (hasClob) {
                        for (int i = 0; i < clobColumns.length; i++) {
                            Column c = clobColumns[i].copyColumnWithoutValue();
                            c = m_I_dbOp.getColumnData(c, rs, j);
                            if (!c.isNull()) {
                                multiData.addClobData(databaseInfo.getName(), tableName, c.getName(), databaseInfo.getEncoding(), pks, c.getValue().getReader(), c.getValue().getReaderLength());
                            } else {
                                multiData.addClobData(databaseInfo.getName(), tableName, c.getName(), databaseInfo.getEncoding(), pks, null, 0);
                            }
                            j++;
                        }
                    }
                }
            }

            ArrayListInputStream data = new ArrayListInputStream();
            m_dataConstructor.setBasicData(databaseInfo.getName(), tableName, basicRows);
            m_dataConstructor.updateHeader(multiData.getHeader());
            data.addInputStream(m_dataConstructor.getDataInputStream());
            data.addInputStream(multiData.getDataWhitoutBaseInputStream());
            m_result = new MDataConstructor(m_dataConstructor.getHeader(), data, basicRows);

        } catch (SQLException e) {
            LogLayout.error(logger,appName,"An error occured while exporting the table "+tableName, e);
        }
        return m_result;
    }


    private Row getRowData(TableInfo tableInfo, ResultSet rs) throws SQLException {
        String tableName = tableInfo.getName();
        ResultSetMetaData rsMetaData = rs.getMetaData();
        int columnCount = rsMetaData.getColumnCount();
        Row row = new Row(databaseInfo.getName(), tableName);
        for (int i = 0; i < columnCount; i++) {
            String columnName = rsMetaData.getColumnName(i + 1);
            Column column = tableInfo.find(columnName).copyColumnWithoutValue();
            if (!column.isLobType()) {
                column = m_I_dbOp.getColumnData(column, rs, i + 1);
            }
            row.addColumn(column);
        }
        return row;
    }

    /**
     * 删除同步发送数据
     *
     * @param conn
     * @param tableInfo
     *@param in
     * @param rows   @throws Ex
     */
    private void disposeData(Connection conn, TableInfo tableInfo, InputStream in, Row[] rows) throws Ex {
        if (rows.length > 0) {
            DataAttributes result = process(in);
            if (result.getStatus().isSuccess()) {
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"process status Value:" + result.getStatus().isSuccess());
                }
               deleteRows(conn,tableInfo,rows);
//                auditProcess(rows[0].getTableName(), rows.length);
            } else {
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"process status Value:" + result.getStatus().isSuccess());
                }
                try {
                    Thread.sleep(Int_Thread_SleepTime);
                } catch (InterruptedException ie) {
                }
                Message Message = new Message("目标端处理出错.");
                LogLayout.debug(logger,appName,Message.toString());
            }
        } else {
            Message Message = new Message("数据为空,不传输.");
            LogLayout.warn(logger,appName,Message.toString());
        }
    }

    private void deleteRows(Connection conn, TableInfo tableInfo, Row[] rows) throws Ex {
        String tableName = tableInfo.getName();
        for (Row row : rows) {
            deleteRecord(row,tableName, conn);
        }
    }

    /**
     * 入库 -- 删除
     *
     * @param tableName
     * @param conn
     * @throws Ex
     */
    private void deleteRecord(Row row, String tableName, Connection conn) throws Ex {
        PreparedStatement prepStmt = null;
        try {
            // String sqlString = "delete from {0}"
            String sqlString = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTable,
                        new String[]{m_I_dbOp.formatTableName(m_schemaName, tableName)});
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"Get delete record query table string:" + sqlString);
            }
            Column[] pkArray = row.getPkColumnArray();
            String strWhere = getWhereStringFromPks(pkArray);
            sqlString = sqlString + strWhere;
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"isDelete : " + sqlString);
                LogLayout.debug(logger,appName,"is pk value: " + pkArray[0].getValue().getValueString());
            }

            prepStmt = m_I_dbOp.getSatement(conn, sqlString, pkArray);
            prepStmt.setQueryTimeout(I_StatementTimeOut);
            prepStmt.executeUpdate();
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured during deleting a row for database/table: " + m_dbName + "/" + tableName + "."));
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
        }
    }


    private String getWhereStringFromPks(Column[] pkColumns) throws SQLException {

        String strWhere = " where ";
        for (int i = 0; i < pkColumns.length; i++) {
            Column pkColumn = pkColumns[i];
            strWhere += m_I_dbOp.formatColumnName(pkColumn.getName());
            if (i != pkColumns.length - 1) {
                strWhere += "=? and ";
            } else {
                strWhere += "=?";
            }
        }
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"Get where string: " + strWhere);
        }
        return strWhere;
    }

}
