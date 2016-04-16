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
import com.hzih.itp.platform.dbchange.exception.EDbChange;
import com.hzih.itp.platform.dbchange.exception.EXSql;
import com.hzih.itp.platform.dbchange.source.SourceOperation;
import com.hzih.itp.platform.dbchange.source.info.DatabaseInfo;
import com.hzih.itp.platform.dbchange.source.info.TableInfo;
import com.hzih.itp.platform.dbchange.source.info.TimeoutRow;
import com.hzih.itp.platform.dbchange.source.plugin.entirely.EntirelyUtils;
import com.hzih.itp.platform.dbchange.source.utils.SourceObjectCache;
import com.hzih.itp.platform.utils.DataAttributes;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.Jdbc;
import com.inetec.common.config.stp.nodes.Type;
import com.inetec.common.exception.E;
import com.inetec.common.exception.Ex;
import com.inetec.common.i18n.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by 钱晓盼 on 14-1-14.
 */
public class SourceProcessTimeSync extends DbInit implements ISourceProcess {

    final static Logger logger = LoggerFactory.getLogger(SourceProcessTimeSync.class);
    private boolean isRun = false;
    private SourceOperation source;
    private DatabaseInfo databaseInfo;
    private Type type;
    private Jdbc jdbc;
    private String appName;
    private TableInfo[] m_tableInfos = new TableInfo[0];
    private SourceObjectCache m_objectCache;
    private String propName;

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
        this.propName = ChangeConfig.getRunShmPath() + File.separator + appName + "timesync.properties";
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
            timeSync();
            LogLayout.info(logger, appName, "完成一次时间标记同步");
            try {
                Thread.sleep(databaseInfo.getInterval() * 1000);
            } catch (InterruptedException ie) {
            }
        }
    }

    /**
     * 时间标记同步 ，同时加上主键。
     *
     * @return
     */
    public void timeSync() {
        //加载同步时间
        Properties timesyncProp = new Properties();
        File propfile = new File(propName);
        boolean isInittime = false;
        try {
            if (!propfile.exists()) {
                isInittime = true;
                if (!propfile.getParentFile().exists()) {
                    propfile.getParentFile().mkdirs();
                }
                propfile.createNewFile();
            } else {
                if (propfile.length() == 0) {
                    isInittime = true;
                }
            }
            timesyncProp.load(new FileInputStream(propfile));
        } catch (IOException e) {
            LogLayout.error(logger,appName, " type dbchange timesync operator init error!", e);
        }

        Timestamp begin = null;
        Timestamp end = null;

        String pkWhere = "";

        int tableSize = m_tableInfos.length;
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger, appName, "开始导出源表数据,Operator is TimeSync 源表个数为: " + tableSize);
        }
        testStatus();
        for (int i = 0; i < tableSize; i++) {
            TableInfo tableInfo = m_tableInfos[i];
            String tableName = m_tableInfos[i].getName();

            try {
                Timestamp initTime = new Timestamp(0);
                if (isInittime) {
                    if (logger.isDebugEnabled()) {
                        LogLayout.debug(logger, appName, "应用为：" + appName + "取到的时间标记为 isInittime:" + isInittime);
                    }
                    if (tableInfo.getTimeSyncTimeField() == null) {
                        LogLayout.warn(logger, appName, "应用为：" + appName + "取到的时间标记主键为：0,请检查配置.");
                    }

                    initTime = getTimeSyncInitDate(tableInfo.getTimeSyncTimeField().getName(), m_tableInfos[i].getName());
                    if (initTime == null) {
                        LogLayout.warn(logger, appName, "应用为：" + appName + "取到的时间标记为：0,请检查数据.");
                        return;
                    }
                    if (logger.isDebugEnabled()) {
                        LogLayout.debug(logger, appName, "应用为：" + appName + "取到的时间标记为 Inittime:" + initTime);
                    }
                } else {
                    if (timesyncProp.getProperty("begintime") == null) {
                        initTime = getTimeSyncInitDate(tableInfo.getTimeSyncTimeField().getName(), m_tableInfos[i].getName());
                        if (initTime == null) {
                            LogLayout.warn(logger, appName, "应用为：" + appName + "取到的时间标记为：0,请检查数据.");
                            return;
                        }
                    } else {
                        initTime = Timestamp.valueOf(timesyncProp.getProperty("begintime"));
                    }

                }

                String begins = timesyncProp.getProperty("begintime", initTime.toString());
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger, appName,"开始时间标记：" + begins);
                }
                end = new Timestamp(initTime.getTime() + (tableInfo.getInterval() + 1) * 60 * 1000);
                String ends = timesyncProp.getProperty("endtime", end.toString());
                begin = Timestamp.valueOf(begins);
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger, appName,"结束时间标记：" + ends);
                }
                end = Timestamp.valueOf(ends);

                String initEndTime = timesyncProp.getProperty("InitEndTime");
                if (initEndTime == null) {
                    initEndTime = begins;
                }
                long initEnd = Timestamp.valueOf(initEndTime).getTime();

                if (begin.getTime() == end.getTime()) {
                    if (initEnd >= end.getTime()) {
                        end = new Timestamp((end.getTime() + tableInfo.getInterval() + 1) * 60 * 1000);
                    } else {
                        updateTimeSyncPksetAndTime(begin, end, null, tableInfo, false, 0);
                    }
                }


                pkWhere = timesyncProp.getProperty("pkwere", "");
                int rownum = Integer.parseInt(timesyncProp.getProperty("rownum", "0"));
                exportWholeTableTimeSync(tableInfo, begin, end, pkWhere, rownum);
                timesyncProp.clear();
            } catch (Ex Ex) {
                Message Message = new Message("导出源表 {0} Operator is TimeSync 数据出错.", tableName);
                LogLayout.error(logger, appName, Message.toString(), Ex);
            } catch (NullPointerException e) {
                LogLayout.error(logger,appName,e.getMessage(),e);

            }
            //}
        }
        LogLayout.debug(logger, appName,"完成源表数据的导出.");

        return;

    }

    private void exportWholeTableTimeSync(TableInfo tableInfo, Timestamp begin, Timestamp end, String pkWhere, int rownum) throws Ex {
        String tableName = tableInfo.getName();

        Column[] clobColumns = tableInfo.getClobColumns();
        Column[] blobColumns = tableInfo.getBlobColumns();
        boolean hasBlob = blobColumns.length > 0;
        boolean hasClob = clobColumns.length > 0;
        boolean hasLob = hasBlob || hasClob;
        boolean hasBothBlobAndClob = hasBlob && hasClob;

        Column[] basicColumns = tableInfo.getBasicColumns();

        String strBasicQuery = null;
        String strWhere = null;

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
            strWhere = getWhereStringFromTimeSync(tableInfo, pkWhere, begin);
            strBasicQuery += strWhere;
        } else {
            strBasicQuery = getTableQueryString(tableInfo,tableName, basicColumns);
            strWhere = getWhereStringFromTimeSync(tableInfo, pkWhere, begin);
            strBasicQuery += strWhere;

        }
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"query table sql string: " + strBasicQuery);
        }
        processMaxRecordsForTimeSync(strBasicQuery, tableInfo, databaseInfo.getMaxRecord(), begin, end, rownum);

    }

    public Timestamp getTimeSyncInitDate(String time, String tableName) throws Ex {
        Timestamp result = new Timestamp(0);
        testStatus();
        Connection conn = getConnection();

        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        Statement stmt = null;
        try {

            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_TimeSyncInitDate,
                    new String[]{time, tableName});
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"Str_Sql_SelectCountFromTempForSys_time sql:" + sqlQuery);
            }
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(I_StatementTimeOut);
            stmt.setMaxRows(1);
            stmt.setFetchSize(1);
            ResultSet rs = stmt.executeQuery(sqlQuery);

            if (rs.next()) {
                result.setTime(rs.getTimestamp(1).getTime());
            }
            rs.close();
            stmt.close();

        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("Failed to retrieve temp data for the table {0}.", databaseInfo.getName()));
            testBadConnection(conn, Exsql);
            returnConnection(conn);
            LogLayout.error(logger,appName,"应用为：" + appName + " 初始化时间标记读取错误.", e);
            throw Exsql;
        } catch (NullPointerException e) {
            LogLayout.error(logger,appName,"应用为：" + appName + " 初始化时间标记读取错误.", e);
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
        return result;
    }

    public void updateTimeSyncPksetAndTime(Timestamp begintime, Timestamp endtime, Row row, TableInfo tableInfo, boolean isUpdate, int rownum) {
        Properties timesyncProp = new Properties();
        File propfile = new File(propName);
        try {
            if (!propfile.exists()) {
                if (!propfile.getParentFile().exists()) {
                    propfile.getParentFile().mkdirs();
                }
                propfile.createNewFile();
            }
            timesyncProp.load(new FileInputStream(propfile));

        } catch (IOException e) {
            LogLayout.error(logger,appName,appName + " type dbchange timesync operator init error!", e);
        }
        Timestamp tempEndtime = new Timestamp(0);

        Timestamp endTime = new Timestamp(0);
        if (isUpdate) {
            long tbegin = Timestamp.valueOf(timesyncProp.getProperty("begintime", begintime.toString())).getTime();
            if (tbegin <= endtime.getTime()) {
                timesyncProp.put("begintime", endtime.toString());
                tempEndtime = new Timestamp(endtime.getTime() + (tableInfo.getInterval() + 1) * 60 * 1000);
            } else {
                timesyncProp.put("begintime", new Timestamp(tbegin).toString());
                tempEndtime = new Timestamp(tbegin + (tableInfo.getInterval() + 1) * 60 * 1000);
            }
        } else {
            timesyncProp.put("begintime", begintime.toString());
        }

        String ends = timesyncProp.getProperty("InitEndTime", new Timestamp(0).toString());

        try {
            endTime = Timestamp.valueOf(ends);
            if (endTime.getTime() == 0) {
                endTime = getTimeSyncEndDate(tableInfo.getTimeSyncTimeField().getName(), tableInfo.getName());
            }
            if (tempEndtime.getTime() > endTime.getTime()) {
                Timestamp endTime2 = getTimeSyncEndDate(tableInfo.getTimeSyncTimeField().getName(), tableInfo.getName());
                if (endTime2.getTime() > 0) {
                    endTime.setTime(endTime2.getTime());
                }
                tempEndtime = new Timestamp(endTime.getTime() + 1000);
            }
            if (isUpdate)
                timesyncProp.put("endtime", tempEndtime.toString());
            else
                timesyncProp.put("endtime", endtime.toString());

            timesyncProp.put("InitEndTime", endTime.toString());
            if (row != null)
                timesyncProp.put("pkwere", new PkSet(PkSet.getPks(row)).getPkString());
            else {
                if (isUpdate)
                    timesyncProp.put("pkwere", "");
                else {
                    timesyncProp.put("pkwere", timesyncProp.getProperty("pkwhere", ""));
                }
            }
            timesyncProp.put("rownum", "" + rownum);
            FileOutputStream outfile = new FileOutputStream(propfile);
            timesyncProp.store(outfile, null);
            outfile.flush();
            outfile.close();
            timesyncProp.clear();
        } catch (Exception e) {
            LogLayout.error(logger,appName,"应用为：" + appName + " 导出源表"+tableInfo.getName()+"数据出错.", e);
        }
    }

    public Timestamp getTimeSyncEndDate (String time, String tablename) throws Ex {
        Timestamp result = new Timestamp(0);
        testStatus();
        Connection conn = getConnection();

        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        Statement stmt = null;
        try {
            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_TimeSyncEndDate,
                    new String[]{time, tablename});
            //if (m_logger.isDebugEnabled()) {
            LogLayout.info(logger,appName,"Str_Sql_TimeSyncEndDate sql:" + sqlQuery);
            //}
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(I_StatementTimeOut);
            stmt.setMaxRows(1);
            stmt.setFetchSize(1);
            ResultSet rs = stmt.executeQuery(sqlQuery);

            if (rs.next()) {
                result.setTime(rs.getTimestamp(1).getTime());
            }
            rs.close();
            stmt.close();

        } catch (SQLException e) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, new Message("Failed to retrieve temp data for the table {0}.", databaseInfo.getName()));
            testBadConnection(conn, Exsql);
            returnConnection(conn);
            LogLayout.error(logger,appName,"应用为：" + appName + " 结束时间标记读取错误.", e);
            throw Exsql;
        } catch (NullPointerException e) {
            LogLayout.error(logger,appName,"应用为：" + appName + " 结束时间标记读取错误.", e);
        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
        return result;
    }

    private String getWhereStringFromTimeSync(TableInfo tableinfo, String pkwhere, Timestamp begin) {
        if (tableinfo.isTimeSync()) {
            String strWhere = "";
            String order = " order by ";
            String ordertemp = "";
            strWhere += " where ";

            strWhere += " ";
            PkSet pkset = null;
            Column[] pkwhers = new Column[0];
            if (pkwhere != null && !pkwhere.equalsIgnoreCase("")) {
                //.try {

                /* pkset = new PkSet(pkwhere);
              pkwhers = pkset.getPkArray();*/
//                } catch (Ex ex) {
//
//                }
            }
            if (pkwhers.length == 0) {
                Column[] pkcolumn = tableinfo.getPkColumns();
                for (int i = 0; i < pkcolumn.length; i++) {
                    if (pkcolumn[i].getJdbcType() == Types.DATE || pkcolumn[i].getJdbcType() == Types.TIMESTAMP || pkcolumn[i].getJdbcType() == Types.TIME) {
                        strWhere += pkcolumn[i].getName() + " >=? and " + pkcolumn[i].getName() + "< ?";
                        order += pkcolumn[i].getName() + " asc ";
                    } else {
                        /*strWhere += pkcolumn[i].getName() + " >  ";
                        if (pkcolumn[i].getJdbcType() == Types.VARCHAR || pkcolumn[i].getJdbcType() == Types.CHAR) {
                            strWhere += "'" + pk + "'";
                        }

                        if (ordertemp.endsWith("asc")) {
                            ordertemp += ",";
                        }
                        ordertemp += pkcolumn[i].getName() + " asc";*/
                    }

                }
            } else {
                for (int i = 0; i < pkwhers.length; i++) {
                    if (pkwhers[i].getDbType().equalsIgnoreCase("DATE") || pkwhers[i].getDbType().equalsIgnoreCase("TIMESTAMP") || pkwhers[i].getDbType().equalsIgnoreCase("TIME")) {
                        strWhere += pkwhers[i].getName() + " >=? and " + pkwhers[i].getName() + "<? ";
                        order += pkwhers[i].getName() + " asc ";
                        //begin = new Timestamp(Long.parseLong(pkwhers[i].getValue().getValueString()));
                    } else {
                        strWhere += pkwhers[i].getName() + " >";
                        if (pkwhers[i].getDbType().equalsIgnoreCase("VARCHAR") || pkwhers[i].getDbType().equalsIgnoreCase("CHAR")) {
                            strWhere += "'" + pkwhers[i].getValue().getValueString() + "'";
                        } else {

                            strWhere += pkwhers[i].getValue().getValueString();
                        }
                        if (ordertemp.endsWith("asc")) {
                            ordertemp += ",";
                        }
                        ordertemp += pkwhers[i].getName() + " asc";
                    }
                    if (i < pkwhers.length - 1) {
                        strWhere += " and ";
                    }
                }
            }

            if (!order.equalsIgnoreCase(" order by ")) {
                strWhere += order;
                if (!ordertemp.equalsIgnoreCase("")) {
                    strWhere += "," + ordertemp;
                }
            }
            if (logger.isDebugEnabled())
                LogLayout.debug(logger,appName,"time sync where is :" + strWhere);
            return strWhere;
        } else {
            return "";
        }
    }


    private void processMaxRecordsForTimeSync(String query, TableInfo tableInfo, int max, Timestamp begin, Timestamp end, int rownum) throws Ex {
        MDataConstructor multiData = null;
        testStatus();
        Connection conn = connection(appName);
        PreparedStatement stmt = null;
        int row = 0;
        try {

            stmt = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"timeSync query sql:" + query);
            }
            stmt.setFetchSize(max);
            //stmt.setMaxRows();
            //stmt.setQueryTimeout(I_StatementTimeOut);
            stmt.setTimestamp(1, begin);
            stmt.setTimestamp(2, end);
            ResultSet rs = stmt.executeQuery();
            boolean done = false;
            if (rownum > 0) {
                for (int i = 0; i < rownum; i++) {
                    rs.next();
                }
                //rs.absolute(rownum);
            }

            do {
                try {
                    multiData = dataConstructor(rs, tableInfo, max);
                    row = rs.getRow();
                    if (multiData.getData().size() > 0) {
                        if (logger.isDebugEnabled()) {
                            LogLayout.debug(logger,appName,"basic row length:" + multiData.getData().getRowArray().length);
                        }

                        disposeData(multiData.getDataInputStream(), multiData.getData().getRowArray());
                        //okay
                        updateTimeSyncPksetAndTime(begin, end, multiData.getData().getRowArray()[multiData.getData().getRowArray().length - 1], tableInfo, false, row);
                    } else {
                        updateTimeSyncPksetAndTime(begin, end, null, tableInfo, true, 0);
                        done = true;
                    }
                } catch (Ex ex) {
                    if (multiData != null && multiData.getData().size() > 0) {

                        if (row >= multiData.getData().size()) {
                            row = row - multiData.getData().size();
                        } else {
                            row = 0;
                        }
                        updateTimeSyncPksetAndTime(begin, end, null, tableInfo, false, row);
                    }
                    switch (ex.getErrcode().toInt()) {
                        case EDbChange.I_TargetProcessError:
                            sleepTime();
                            break;
                        default:
                            throw ex;

                    }
                }
                try {
                    multiData.getData().clear();
                } catch (Ex ex) {
                    //okay
                }
            }
            while (!done);
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            if (multiData != null && multiData.getData().size() > 0) {
                if (row >= multiData.getData().size()) {
                    row = row - multiData.getData().size();
                } else {
                    row = 0;
                }
                updateTimeSyncPksetAndTime(begin, end, null, tableInfo, false, row);
            }
            EXSql exsql = m_I_dbOp.sqlToExSql(e, new Message("An error occured while exporting the table {0}.", tableInfo.getName()));
            LogLayout.error(logger,appName,exsql.toString(),e);

        } finally {
            DbopUtil.closeStatement(stmt);
            returnConnection(conn);
        }
    }

    /**
     * 时间标记同步发送数据
     * @param in
     * @param rows
     * @throws Ex
     */
    private void disposeData(InputStream in, Row[] rows) throws Ex {
        if (rows.length > 0) {
            DataAttributes result = process(in);
            if (result.getStatus().isSuccess()) {
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
                }
                Message Message = new Message("目标端处理出错.");
                LogLayout.debug(logger,appName,Message.toString());
            }
        } else {
            Message Message = new Message("数据为空,不传输.");
            LogLayout.warn(logger,appName,Message.toString());
        }
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

}
