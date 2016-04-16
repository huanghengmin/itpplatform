package com.hzih.itp.platform.dbchange.source.plugin;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.dbchange.DBChangeSource;
import com.hzih.itp.platform.dbchange.DbInit;
import com.hzih.itp.platform.dbchange.datautils.ArrayListInputStream;
import com.hzih.itp.platform.dbchange.datautils.MDataConstructor;
import com.hzih.itp.platform.dbchange.datautils.db.*;
import com.hzih.itp.platform.dbchange.datautils.db.pk.PkSet;
import com.hzih.itp.platform.dbchange.datautils.dboperator.DbopUtil;
import com.hzih.itp.platform.dbchange.datautils.dboperator.sqlbundle.DBType;
import com.hzih.itp.platform.dbchange.exception.EXSql;
import com.hzih.itp.platform.dbchange.source.SourceOperation;
import com.hzih.itp.platform.dbchange.source.info.DatabaseInfo;
import com.hzih.itp.platform.dbchange.source.info.TableInfo;
import com.hzih.itp.platform.dbchange.source.info.TempRowBean;
import com.hzih.itp.platform.dbchange.source.info.TimeoutRow;
import com.hzih.itp.platform.dbchange.source.utils.SourceObject;
import com.hzih.itp.platform.dbchange.source.utils.SourceObjectCache;
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
import java.util.*;

/**
 * Created by 钱晓盼 on 14-1-14.
 */
public class SourceProcessTrigger extends DbInit implements ISourceProcess {
    final static Logger logger = LoggerFactory.getLogger(SourceProcessTrigger.class);

    public final static String Str_Sql_SelectAllFromTemp = "Sql_SelectAllFromTemp";
    public final static String Str_Sql_DeleteFromTempForIDSet = "Sql_DeleteFromTempForIDSet";
    public final static String Str_Sql_DeleteFromTable = "Sql_DeleteFromTable";
    public final static String Str_Sql_UpdateSpecifyFlag = "Sql_UpdateSpecifyFlag";

    private boolean isRun = false;
    private SourceOperation source;
    private DatabaseInfo databaseInfo;
    private Type type;
    private Jdbc jdbc;
    private String appName;
    private TableInfo[] m_tableInfos = new TableInfo[0];
    private Connection m_conn = null;
    private SourceObjectCache m_objectCache;
    private Map m_rowList = new HashMap();

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
            LogLayout.error(logger,appName,"initDbSource error", ex);
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
        boolean isInitOldStep = databaseInfo.isOldStep();
        while (isRun) {
            if(isInitOldStep) {
                exportOldData();
                isInitOldStep = false;
                LogLayout.info(logger,appName,"完成触发同步的同步前--全表同步");
                updateOldStep();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
            } else {
                int size = exportNewData();
                if (size == 0) {
                    try {
                        Thread.sleep(databaseInfo.getInterval() * 1000);
                    } catch (InterruptedException ie) {
                        // okay.
                    }
                }
            }
        }
    }

    /**
     * 完成一次已有数据的同步后改变配置文件中允许同步已有数据标记oldStep的值为false
     * 确保重启服务后数据不会覆盖
     */
    private void updateOldStep() {


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
                    exportWholeTable(tableName);
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

    private int exportNewData() {
        int size = 0;
        testStatus();
        int tableSize = m_tableInfos.length;
        for (int i = 0; i < tableSize; i++) {
            TableInfo tableInfo = m_tableInfos[i];
            String tableName = m_tableInfos[i].getName();
            try {
                do {
                    long s = System.currentTimeMillis();
                    size = exportForTempTable(tableInfo, m_tempRowLastMaxRow);
                    long l = System.currentTimeMillis() - s;
                    LogLayout.info(logger,appName,"完成一次采集文件处理, 耗时"+l+"豪秒");
                } while (size > 0);
            } catch (Ex Ex) {
                Message Message = new Message("导出源表 {0} 数据出错.", tableName);
                LogLayout.error(logger, appName, Message.toString(), Ex);
            }
        }
        return size;
    }

    private int exportForTempTable(TableInfo tableInfo, long maxrecod) throws Ex {
        Connection conn = connection(appName);
        Statement stmt = null;
        MDataConstructor multiData = new MDataConstructor();
        int length = 0;
        ArrayList listRow = new ArrayList();
        try {
            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_SelectAllFromTemp,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, databaseInfo.getTodoTable()),
                            "'" + m_schemaName + "'", "'" + tableInfo.getName() + "'", String.valueOf(maxrecod)});
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"Str_Sql_SelectAllFromTemp: " + sqlQuery);
            }
            int max = databaseInfo.getMaxRecord();
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(I_StatementTimeOut);
            stmt.setFetchSize(max);
            if (m_dbType.equals(DBType.C_MSSQL))
                stmt.setMaxRows(max);
            else
                stmt.setMaxRows(I_MaxRows);
            ResultSet rs = stmt.executeQuery(sqlQuery);
            boolean done = false;
            int size = 0;
            while (!done) {
                TempRowBean tmp = dataConstructorForTemp(rs, max);
                if (tmp.getLength() == 0) {
                    done = true;
                } else {
                    if (tmp.getMaxId() > maxrecod) {
                        maxrecod = tmp.getMaxId();
                    }
                    size = size + tmp.getLength();
                    listRow.add(tmp);
                }
            }
            if (rs != null)
                rs.close();

            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
            //删除临时记录

            if (listRow.size() > 0) {
                if (m_dbType.equals(DBType.C_MSSQL)) {
                    deleteTempRow(conn, listRow);
                }
                listRow.clear();
                m_tempRowLastMaxRow = maxrecod;
                length = size;
            } else {
                m_tempRowLastMaxRow = 0;
            }
        } catch (SQLException  sqlEEx) {
            /*EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("An error occured while retrieving a temp row."));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
            testBadConnection(conn, Exsql);
            throw Exsql;*/
            LogLayout.error(logger,appName,"An error occured while retrieving a temp row.",sqlEEx);
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
        return length;
    }

    /**
     * 批量delete  .成批删除TempRow
     *
     * @param list
     * @return
     * @throws Ex
     */
    private int deleteTempRow(Connection conn, ArrayList list) throws Ex {

        Statement stmt = null;
        int nUpdate = 0;
        testStatus();
        if (conn == null) {
            conn = getConnection();
        }
        String sqlQuery = "";
        try {

            stmt = conn.createStatement();
            for (int i = 0; i < list.size(); i++) {
                TempRowBean tmp = (TempRowBean) list.get(i);
                sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTempForIDSet,
                        new String[]{m_I_dbOp.formatTableName(m_schemaName, databaseInfo.getTodoTable()), tmp.getIds() + ""});
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"Delete temprow sql: " + sqlQuery);
                }
                stmt.addBatch(sqlQuery);
            }
            int sizes[] = stmt.executeBatch();
            for (int i = 0; i < sizes.length; i++) {
                nUpdate = nUpdate + sizes[i];
            }
        } catch (SQLException sqlEEx) {
//            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to delete a temp row({0}).", "" + sqlQuery));
//            EStatus status = EStatus.E_DATABASEERROR;
//            status.setDbInfo(m_dbName, m_dbDesc);
//            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
//            testBadConnection(conn, Exsql);
//            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            //returnConnection(conn);
        }

        return nUpdate;
    }

    private void exportWholeTable(String tableName) throws Ex {
        TableInfo tableInfo = findTableInfo(tableName);
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
        String strWhere = null;

        boolean isOldStep = tableInfo.isTriggerEnable() || tableInfo.isDelete();
        isOldStep = isOldStep || tableInfo.isSpecifyFlag();
        isOldStep = databaseInfo.isOldStep() || !isOldStep;
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
//            strWhere = getWhereStringFromSpeicfyFlag(tableInfo.isSpecifyFlag());
//            strBasicQuery += strWhere;
        } else {
            strBasicQuery = getTableQueryString(tableInfo,tableName, basicColumns);
//            strWhere = getWhereStringFromSpeicfyFlag(tableInfo.isSpecifyFlag());
//            strBasicQuery += strWhere;
        }
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"query table sql string: " + strBasicQuery);
        }
        if (isOldStep) {
            processMaxRecordsForOldStep(strBasicQuery, tableInfo, databaseInfo.getMaxRecord());
        } else {
            int n = 0;
            do {
                n = processMaxRecords(strBasicQuery, tableInfo, databaseInfo.getMaxRecord());
            } while (n > 0);
        }

    }

    public TableInfo findTableInfo(String tableName) {
        for (int i = 0; i < m_tableInfos.length; i++) {
            if (m_tableInfos[i].getName().equalsIgnoreCase(tableName)) {
                return m_tableInfos[i];
            }
        }
        return null;
    }

    private void processMaxRecordsForOldStep(String query, TableInfo tableInfo, int max) throws Ex {
        MDataConstructor multiData = null;
        testStatus();
        Connection conn = getConnection();

        if (conn == null) {
            dataBaseRetryProcess(conn);
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        Statement stmt = null;
        try {

            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(max);
            //stmt.setQueryTimeout(I_StatementTimeOut);
            ResultSet rs = stmt.executeQuery(query);
            boolean done = false;
            multiData = new MDataConstructor();
            do {
                try {
                    multiData = dataConstructor(rs, tableInfo, max);
                    if (multiData.getData().getRowArray().length > 0) {
                        if (logger.isDebugEnabled()) {
                            LogLayout.debug(logger,appName,"basic row length:" + (multiData.getData().getRowArray().length));
                        }
                        disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), false);

                    } else {
                        done = true;
                    }
                } catch (Ex ex) {

                }

            }
            while (!done);
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            /*EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table {0}.", tableInfo.getName()));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbName);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + "table:" + tableInfo.getName(), true);
            throw Exsql;*/
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
    }

    private int processMaxRecords(String sql, TableInfo tableInfo, int max) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        testStatus();
        Connection conn = getConnection();

        if (conn == null) {
//            EStatus status = EStatus.E_DATABASECONNECTIONERROR;
//            status.setDbInfo(m_dbName, m_dbDesc);
//            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + tableInfo.getName(), true);
            dataBaseRetryProcess(conn);
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
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
                disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), false);
                return multiData.getData().getRowArray().length;
            } else {
                return multiData.getData().getRowArray().length;
            }

        } catch (SQLException e) {
//            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table {0}.", tableInfo.getName()));
//            EStatus status = EStatus.E_DATABASECONNECTIONERROR;
//            status.setDbInfo(m_dbName, m_dbDesc);
//            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + tableInfo.getName(), true);
//            testBadConnection(conn, Exsql);
//            throw Exsql;
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
            /*EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table {0}.", tableName));
            EStatus status = EStatus.E_DATABASEERROR;
            status.setDbInfo(m_dbName, m_dbDesc);
            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + "table:" + tableName, true);
            throw Exsql;*/
        }
        return m_result;
    }

    private TempRowBean dataConstructorForTemp(ResultSet rs, int max) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        HashMap map = new HashMap();
        ArrayList list = new ArrayList();
        ArrayList list1 = new ArrayList();
        TempRowBean result = new TempRowBean();
        long maxrecode = 0;
        try {
            TempRow row = new TempRow();
            String action = null;
            String action_temp = null;
            String tableName = null;
            String tableName_temp = null;
            for (int i = 0; i < max && rs.next(); i++) {
                row = new TempRow();
                row.setId(rs.getLong(1));
                row.setDatabaseName(rs.getString(2));
                tableName = rs.getString(3);
                row.setTableName(tableName);
                row.setPks(rs.getString(4));
                action = rs.getString(5);
                row.setAct(action);
                row.setActTime(rs.getTimestamp(6));
                if(action.equals(action_temp) && tableName.equals(tableName_temp)) {
                    list.add(row);
                    if (row.getId() > maxrecode) {
                        maxrecode = row.getId();
                    }
                    map.put(row, row);
                } else {
                    if(action_temp!=null && tableName_temp!=null) {
//                        TableInfo tableinfo = findTableInfo(row.getTableName());
                        list1 = preprocessForTemp(map);
                        multiData = processTempRows(list1);
                        if (multiData.getData().getRowArray().length > 0) {
                            disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), true);
                        }
                        multiData = new MDataConstructor();
                        list1 = new ArrayList();
                        map = new HashMap();
                        list = new ArrayList();
                    }
                    list.add(row);
                    if (row.getId() > maxrecode) {
                        maxrecode = row.getId();
                    }
                    map.put(row, row);
                }
                action_temp = action;
                tableName_temp = tableName;

            }

//            TableInfo tableinfo = findTableInfo(row.getTableName());
            list1 = preprocessForTemp(map);
            multiData = processTempRows(list1);
            if (multiData.getData().getRowArray().length > 0) {
                disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray(), true);
            }

            //删除临时记录
            if (list.size() > 0) {
                String ids = getTempRowIdSet(list);
//                if (!m_dbType.equals(DBType.C_MSSQL)) {
                deleteTempRow(ids);
//                }
                result.setLength(list.size());
                result.setMaxId(maxrecode);
                result.setIds(ids);
            }
        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table."));
            throw Exsql;
        }
        return result;
    }

    private String getTempRowIdSet (List tempRows) {
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < tempRows.size(); i++) {
            TempRow temp = (TempRow) tempRows.get(i);
            buff.append(String.valueOf(temp.getId()));
            if (i < tempRows.size() - 1) {
                buff.append(",");
            }
        }
        return buff.toString();
    }

    private int deleteTempRow(String ids) throws Ex {

        Statement stmt = null;
        int nUpdate = 0;
        testStatus();
        Connection conn = getConnection();


        try {

            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTempForIDSet,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, databaseInfo.getTodoTable()), ids + ""});
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"Delete temprow sql: " + sqlQuery);
            }
            stmt = conn.createStatement();
            {
                nUpdate = stmt.executeUpdate(sqlQuery);
            }
        } catch (SQLException sqlEEx) {
//            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to delete a temp row({0}).", "" + ids));
//            EStatus status = EStatus.E_DATABASEERROR;
//            status.setDbInfo(m_dbName, m_dbDesc);
//            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + m_databaseInfo.getTodoTable(), true);
//            testBadConnection(conn, Exsql);
//            throw Exsql;
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }

        return nUpdate;
    }

    protected MDataConstructor processTempRows(List tempRows) throws Ex {
        MDataConstructor multiData = new MDataConstructor();
        MDataConstructor m_dataConstructor = new MDataConstructor();
        MDataConstructor m_result = new MDataConstructor();
        Connection conn = null;
        Rows tempBasicRows = new Rows();
        Properties dataProps = new Properties();
        TempRow tempRow = null;
        testStatus();
        try {
            conn = getConnection();
            for (int x = 0; x < tempRows.size(); x++) {

                tempRow = (TempRow) tempRows.get(x);
                String tableName = tempRow.getTableName();
                TableInfo tableInfo = findTableInfo(tableName);

                PkSet pks = tempRow.getPks();
                Column pkColumns[] = pks.getPkArray();
                if (tempRow.getAction() == Operator.OPERATOR__DELETE) {
                    Row datarow = new Row(databaseInfo.getName(), tableName);
                    datarow.setAction(Operator.OPERATOR__DELETE);

                    for (int j = 0; j < pkColumns.length; j++) {
                        Column c = pkColumns[j];
                        Column metaRow = databaseInfo.find(tableName, c.getName()).copyColumnWithoutValue();
                        if (metaRow != null) {
                            metaRow.setValue(c.getValue());
                            datarow.addColumn(metaRow);
                        }
                    }
                    tempBasicRows.addRow(datarow);
                } else {
                    PreparedStatement prepStmt = null;
                    try {
                        Column[] clobColumns = tableInfo.getClobColumns();
                        Column[] blobColumns = tableInfo.getBlobColumns();
                        boolean hasClob = clobColumns.length > 0;
                        boolean hasBlob = blobColumns.length > 0;
                        boolean hasBothBlobAndClob = hasBlob && hasClob;

                        String strQuery = getTableQueryString(tableInfo,tableName, tableInfo.getBasicColumns());
                        String strWhere = getWhereStringFromPks(pkColumns);
                        if (logger.isDebugEnabled()) {
                            LogLayout.debug(logger,appName,"Get where sql string(lob): " + strWhere);
                        }
                        prepStmt = m_I_dbOp.getSatement(conn, strQuery + strWhere, pkColumns);
                        prepStmt.setFetchSize(1);
                        ResultSet rs = prepStmt.executeQuery();

                        if (rs.next()) {
                            Row row = getRowData(tableInfo, rs);
                            row.setAction(tempRow.getAction());
                            tempBasicRows.addRow(row);
//                            multiData.setBasicData(databaseInfo.getName(), tableName, tempBasicRows);
                            if (hasBothBlobAndClob) {
                                for (int i = 0; i < blobColumns.length; i++) {
                                    Column c = blobColumns[i].copyColumnWithoutValue();
                                    c = getLobData(tableInfo,tableName, c, pkColumns, strWhere);
                                    if (!c.isNull()) {
                                        multiData.addBlobData(databaseInfo.getName(), tableName, c.getName(), pks, c.getValue().getInputStream(), c.getValue().getInputStreamLength());
                                    } //else {}
                                }

                                for (int i = 0; i < clobColumns.length; i++) {
                                    Column c = clobColumns[i].copyColumnWithoutValue();
                                    c = getLobData(tableInfo,tableName, c, pkColumns, strWhere);
                                    if (!c.isNull()) {
                                        multiData.addClobData(databaseInfo.getName(), tableName, c.getName(), databaseInfo.getEncoding(), pks, c.getValue().getReader(), c.getValue().getReaderLength());
                                    }
                                }

                            } else {
                                if (hasBlob) {
                                    for (int i = 0; i < blobColumns.length; i++) {
                                        Column c = blobColumns[i].copyColumnWithoutValue();
                                        c = getLobData(tableInfo,tableName, c, pkColumns, strWhere);
                                        if (!c.isNull()) {
                                            multiData.addBlobData(databaseInfo.getName(), tableName, c.getName(), pks, c.getValue().getInputStream(), c.getValue().getInputStreamLength());
                                        } //else {}
                                    }
                                }
                                if (hasClob) {
                                    for (int i = 0; i < clobColumns.length; i++) {
                                        Column c = clobColumns[i].copyColumnWithoutValue();
                                        c = getLobData(tableInfo,tableName, c, pkColumns, strWhere);
                                        if (!c.isNull()) {
                                            multiData.addClobData(databaseInfo.getName(), tableName, c.getName(), databaseInfo.getEncoding(), pks, c.getValue().getReader(), c.getValue().getReaderLength());
                                        }
                                    }
                                }
                            }
                        } else {
                            //deleteTempRow(tempRow.getId(), conn);
                            deleteTempRow(String.valueOf(tempRow.getId()));
//                            multiData = null;
                            LogLayout.warn(logger, appName, "Can not find DbChangeSource temp-data from temp table.");
//                            m_log.setTableName(tableName);
//                            m_log.setStatusCode(EStatus.E_PackDataFaild.getCode() + "");
//                            m_log.warn("Can not find DbChangeSource temp-data from temp table.");
                        }
                        rs.close();
                    } catch (SQLException sqlEEx) {
//                        EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("An error occured while processing the temp row('{0}') of table '{1}'.", "" + tempRow.getId(), tempRow.getTableName()));
//                        EStatus status = EStatus.E_DATABASEERROR;
//                        status.setDbInfo(m_dbName, m_dbDesc);
//                        m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + tempRow.getTableName(), true);
//                        testBadConnection(conn, Exsql);
//                        throw Exsql;
                        LogLayout.error(logger,appName,sqlEEx.getMessage(),sqlEEx);
                    } finally {
                        DbopUtil.closeStatement(prepStmt);
                    }
                }

            }
            ArrayListInputStream data = new ArrayListInputStream();
            if (tempRow != null)
                m_dataConstructor.setBasicData(databaseInfo.getName(), tempRow.getTableName(), tempBasicRows, dataProps);
            else
                m_dataConstructor.setBasicData(databaseInfo.getName(), "", tempBasicRows, dataProps);
            if(multiData !=null ){
                m_dataConstructor.updateHeader(multiData.getHeader());
                data.addInputStream(m_dataConstructor.getDataInputStream());
                data.addInputStream(multiData.getDataWhitoutBaseInputStream());
                m_result = new MDataConstructor(m_dataConstructor.getHeader(), data, m_dataConstructor.getData());
            }
        } finally {
            returnConnection(conn);
        }
        return m_result;
    }

    private Column getLobData(TableInfo tableInfo,String tableName, Column lobColumn, Column[] pkColumns, String strWhere) throws Ex {

        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger, appName, "Get table query where sql string(lob): " + strWhere);
        }
        String strLobQuery = getTableQueryString(tableInfo,tableName, new Column[]{lobColumn});
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"Get table query sql string(lob): " + strLobQuery);
        }
        strLobQuery += strWhere;
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"Query lob table sql string(lob): " + strLobQuery);
        }
        testStatus();
        if (m_conn == null)
            m_conn = getConnection();
        if (m_conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        PreparedStatement prepStmt = null;
        try {
            prepStmt = m_I_dbOp.getSatement(m_conn, strLobQuery, pkColumns);
            ResultSet rs = prepStmt.executeQuery();
            if (rs.next()) {
                lobColumn = m_I_dbOp.getColumnData(lobColumn, rs, 1);
                return lobColumn;
            }
            rs.close();
        } catch (SQLException sqlEEx) {
//            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("Failed to retrieve LOB column '{0}' from the table '{1}'.", lobColumn.getName(), tableName));
//            EStatus status = EStatus.E_DATABASEERROR;
//            status.setDbInfo(m_dbName, m_dbDesc);
//            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " TempTable:" + tableName, true);
//            testBadConnection(m_conn, Exsql);
//            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
            //returnConnection(conn);
        }

        return lobColumn;
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

     /**
     * 预处理TempRow 合并对相同记录.以最后更新的时间为准.
     *
     * @param list
     * @return List
     */
    private ArrayList preprocessForTemp(Map list) throws Ex {

        ArrayList result = new ArrayList();
        Iterator tempRows = list.keySet().iterator();
        //HashMap map = new HashMap();
        //HashMap tememap = new HashMap();
        boolean isEqual = false;
        for (; tempRows.hasNext(); ) {
            TempRow tempRow = (TempRow) tempRows.next();
            String tableName = tempRow.getTableName();
            PkSet pks = tempRow.getPks();
            Column pkColumns[] = null;
            try {
                pkColumns = pks.getPkArray();
            } catch (Ex ex) {
                continue;
            }
            SourceObject sourceObject = new SourceObject(getDbName(), tableName, tempRow.getAction());
            for (int j = 0; j < pkColumns.length; j++) {
                Column c = pkColumns[j];
                Column metaRow = databaseInfo.find(tableName, c.getName()).copyColumnWithoutValue();
                if (metaRow != null) {
                    c.setJdbcType(metaRow.getJdbcType());
                    sourceObject.addPk(c.getName(), c.getObject());
                } else {
                    Message Message = new Message("临时表中业务表的主键名不同于配置的主键名,需要用户干预.");
//                    Ex Ex = new Ex().set(EDbChange.E_DataIsNullErrorr, Message);
//                    m_LogLayout.warn(logger,"platform",Message.toString(), Ex);
//                    m_log.warn(Message.toString(), Ex);
//                    throw Ex;
                    LogLayout.warn(logger,appName,Message.toString());
                }
            }
            boolean inputCache = m_objectCache.remove(sourceObject);

            if (!inputCache) {
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"One pk object is removed from cache, cache size=" + m_objectCache.size());
                }
                //TempRow tempRow = (TempRow) tememap.get(sourceObject);
                result.add(tempRow);
            }
            //tememap.put(sourceObject, tempRow);
            //map.put(sourceObject, sourceObject);

        }
        /* Iterator itr = map.values().iterator();
        for (; itr.hasNext();) {
            SourceObject sourceObject = (SourceObject) itr.next();
            boolean inputCache = m_objectCache.remove(sourceObject);

            if (!inputCache) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("One pk object is removed from cache, cache size=" + m_objectCache.size());
                }
                TempRow tempRow = (TempRow) tememap.get(sourceObject);
                result.add(tempRow);
            }
        }*/

        return result;
    }

    public String getDbName() {
        return m_dbName;
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
     * 触发同步发送数据
     * @param in
     * @param rows
     * @throws Ex
     */
    private void disposeData(InputStream in, Row[] rows, boolean isTemp) throws Ex {
        if (rows.length > 0) {
            // dispose
//            DataAttributes result;
//            try {
//                if (m_changeMain.isNetWorkOkay())
//                    result = m_changeMain.dispose(m_changeType, data, new DataAttributes());
//                else
//                    throw new Ex().set(EDbChange.E_NetWorkError);

//            } catch (Ex Ex) {
//                EStatus status = EStatus.E_NetWorkError;
//                status.setDbInfo(m_dbName, m_dbDesc);
//                m_changeMain.setStatus(m_changeType, status, null, true);
//                try {
//                    Thread.sleep(Int_Thread_SleepTime);
//                } catch (InterruptedException ie) {
//                }
//                throw Ex;
//            }
            DataAttributes result = process(in);
            if (result.getStatus().isSuccess()) {
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"process status Value:" + result.getStatus().isSuccess());
                }
                if (!isTemp) {
                    if (logger.isDebugEnabled()) {
                        LogLayout.debug(logger,appName,"isTemp Value:" + isTemp);
                    }
                    for (int j = 0; j < rows.length; j++) {
                        Row r = rows[j];
                        postProcessor(r);
                    }
                }
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"process status Value:" + result.getStatus().isSuccess());
                }
//                auditProcess(rows[0].getTableName(), rows.length);
            } else {
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"process status Value:" + result.getStatus().isSuccess());
                }
                try {
                    Thread.sleep(Int_Thread_SleepTime);
                } catch (InterruptedException ie) {
                    //okay
                }
                Message Message = new Message("目标端处理出错.");
//                throw new Ex().set(EDbChange.E_TargetProcessError, Message);
                LogLayout.debug(logger,appName,Message.toString());
            }
        } else {
            Message Message = new Message("数据为空,不传输.");
            LogLayout.warn(logger,appName,Message.toString());
        }
    }

    private void postProcessor(Row row) throws Ex {
        Connection conn = getConnection();

        PreparedStatement stmt = null;
        String tableName = row.getTableName();
        TableInfo tableInfo = findTableInfo(tableName);
        if (tableInfo == null) {
            throw new Ex().set(E.E_ObjectNotFound, new Message("The table name {0} is not configed", tableName));
        }

        String sqlString = null;
        boolean isSpecifyFlag = databaseInfo.isSpecifyFlag();
        String flagName = databaseInfo.getFlagName();
        boolean isDelete = databaseInfo.isDelete();
        TimeoutRow timeoutRow = (TimeoutRow) m_rowList.get(row);
        if (timeoutRow != null) {
            if (timeoutRow.verifier()) {
                m_rowList.remove(row);
            } else {
                return;
            }
        }
        try {
            if (isDelete) {
                // String sqlString = "delete from {0}";
                sqlString = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTable,
                        new String[]{m_I_dbOp.formatTableName(m_schemaName, tableName)});
            } else if (isSpecifyFlag) {

                sqlString = m_I_dbOp.getSqlProperty(conn, Str_Sql_UpdateSpecifyFlag,
                        new String[]{m_I_dbOp.formatTableName(m_schemaName, tableName), flagName});


                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"update SpecifyFlag sql: " + sqlString);
                }
            } // else {}

            if (isDelete || isSpecifyFlag) {
                Column[] pkArray = row.getPkColumnArray();
                String strWhere = getWhereStringFromPks(pkArray);
                sqlString = sqlString + strWhere;
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"isDelete or isSpecifyFlag sql: " + sqlString);
                    LogLayout.debug(logger,appName,"is pk value: " + pkArray[0].getValue().getValueString());
                }
                stmt = m_I_dbOp.getSatement(conn, sqlString, pkArray);
                stmt.setQueryTimeout(I_StatementTimeOut);
                int numn = stmt.executeUpdate();
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"update datebase  record number: " + numn);
                }

            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("An error occured while processing the table {0} of the old data.", tableName));
//            EStatus status = EStatus.E_DATABASECONNECTIONERROR;
//            status.setDbInfo(m_dbName, m_dbDesc);
//            m_changeMain.setStatus(m_changeType, status, "DataBase:" + m_dbName + " Table:" + tableInfo.getName(), true);
//            testBadConnection(conn, Exsql);
//            if (Exsql.getErrcode().equals(ErrorSql.ERROR___STATEMENT_TIME_OUT)) {
//                m_rowList.put(row, new TimeoutRow(row));
//                m_LogLayout.warn(logger,"platform","Post Processor Time Out: " + Exsql.getMessage());
//                m_log.setDbName(m_dbName);
//                m_log.setTableName(tableInfo.getName());
//                m_log.setStatusCode(EStatus.E_RecordProcessFaild.getCode() + "");
//                m_log.warn("Post Processor Time Out: " + Exsql.getMessage());
//            } else {
//                throw Exsql;
//            }
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
    }

}
