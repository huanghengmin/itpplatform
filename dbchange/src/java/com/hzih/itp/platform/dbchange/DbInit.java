/*=============================================================
 * 文件名称: ErrorSql.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-10-17
 * ============================================================
 * <p>版权所有  (c) 2005 杭州网科信息工程有限公司</p>
 * <p>
 * 本源码文件作为杭州网科信息工程有限公司所开发软件一部分，它包涵
 * 了本公司的机密很所有权信息，它只提供给本公司软件的许可用户使用。
 * </p>
 * <p>
 * 对于本软件使用，必须遵守本软件许可说明和限制条款所规定的期限和
 * 条件。
 * </p>
 * <p>
 * 特别需要指出的是，您可以从本公司软件，或者该软件的部件，或者合
 * 作商取得并有权使用本程序。但是不得进行复制或者散发本文件，也不
 * 得未经本公司许可修改使用本文件，或者进行基于本程序的开发，否则
 * 我们将在最大的法律限度内对您侵犯本公司版权的行为进行起诉。
 * </p>
 * ==========================================================*/
package com.hzih.itp.platform.dbchange;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.dbchange.datautils.db.Column;
import com.hzih.itp.platform.dbchange.datautils.dboperator.DbopFactory;
import com.hzih.itp.platform.dbchange.datautils.dboperator.DbopUtil;
import com.hzih.itp.platform.dbchange.datautils.dboperator.IDbOp;
import com.hzih.itp.platform.dbchange.datautils.dboperator.sqlbundle.DBType;
import com.hzih.itp.platform.dbchange.datautils.dboperator.sqlbundle.DefaultDBTypeMapper;
import com.hzih.itp.platform.dbchange.exception.EXSql;
import com.hzih.itp.platform.dbchange.exception.ErrorSql;
import com.hzih.itp.platform.dbchange.source.info.TableInfo;
import com.hzih.logback.LogLayout;
import com.inetec.common.db.stp.datasource.DatabaseSource;
import com.inetec.common.exception.E;
import com.inetec.common.exception.Ex;
import com.inetec.common.i18n.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public abstract class DbInit {
    // SQLException sleep time
    public final static int Int_Thread_SleepTime = 10 * 1000;
    public final static int Int_DatabaseRetry_SleepTime = 60 * 1000;

    public final static int I_StatementTimeOut = 5 * 60;
    public final static int I_MaxRows = 20000;
    public static final String Str_ChangeTime = "ChangeTime";
    public final static String Str_Sql_DeleteFromTable = "Sql_DeleteFromTable";
    public final static String Str_Sql_InsertIntoTable = "Sql_InsertIntoTable";
    public final static String Str_Sql_SelectFromTable = "Sql_SelectFromTable";
    public final static String Str_Sql_TimeSyncEndDate = "sql_TimeSyncEndDate";
    public final static String Str_Sql_TimeSyncInitDate = "sql_TimeSyncInitDate";
    public final static String Str_Sql_UpdateSpecifyFlag = "Sql_UpdateSpecifyFlag";

    public static Logger logger = LoggerFactory.getLogger(DbInit.class.getName());

//    public LogHelper m_log = null;
    protected String m_schemaName = "";
    protected String m_dbName;
    protected String m_dbDesc;
    protected String m_nativeCharSet = "";
    protected String m_driverClass = "";
    protected DatabaseSource m_dbSource = null;
    protected IDbOp m_I_dbOp = null;
    protected DBType m_dbType = null;

    //
    public static String Str_Format_Date = "yyyy-MM-dd HH:mm:ss";
    public final static int Int_DataBaseRetryMeanTime = 5 * 60 * 1000;
    private long m_fristTime = 0;
    private long m_lastTime = 0;
    protected long m_tempRowLastMaxRow = 0;


    public void initDbSource() throws Ex {
        if (m_dbSource == null)
            m_dbSource = ChangeConfig.findDataSource(m_dbName);
        initDbOp();
    }

    private void initDbOp() {
        try {
            if (m_dbSource != null) {
                m_I_dbOp = DbopFactory.getDbms(m_dbSource.getConnection(), m_driverClass);
                m_dbType = DefaultDBTypeMapper.getDBType(m_dbSource.getConnection());
                initDbType();
            }
        } catch (RuntimeException e) {
            m_I_dbOp = null;
            LogLayout.error(logger, "DataSource create Connection error.", e);

        } catch (SQLException e) {
            m_I_dbOp = null;
            LogLayout.error(logger, "DataSource create Connection error.", e);
        }
    }

    protected void dataBaseRetryProcess(Connection con) {
        try {
            Thread.sleep(Int_DatabaseRetry_SleepTime);
        } catch (InterruptedException e) {
            //okay
        }

    }

    public String dataFormat(long time) {
        Date date = new Date(time);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Str_Format_Date);
        return simpleDateFormat.format(date);
    }

    public void setDatabaseFailingTime() {
        if (m_fristTime != 0) {
            m_lastTime = System.currentTimeMillis();
        } else {
            m_fristTime = System.currentTimeMillis();
            m_lastTime = m_fristTime;
        }

    }

    protected void testBadConnection(Connection conn, EXSql e) {
        if (conn != null && e.getErrcode().equals(ErrorSql.ERROR___DB_CONNECTION)) {
            Statement stmt = null;
            try {
                stmt = conn.createStatement();
            } catch (SQLException e2) {
                setDatabaseFailingTime();
                returnConnection(conn);
                try {
                    Thread.sleep(Int_Thread_SleepTime);
                } catch (InterruptedException ee) {
                    // ignore.
                }
            } finally {
                DbopUtil.closeStatement(stmt);
            }
        }

    }

//    protected void reConnect() throws Ex {
//        Connection conn = null;
//        while (conn == null){
//            try {
//                Thread.sleep(1000*10);
//            } catch (InterruptedException e) {
//            }
//            conn = m_dbSource.getConnection();
//            throw new Ex().set(E.E_NullPointer, new Message("{0} Database create connection is error. Wait 10 seconds.", m_schemaName));
//
//        }
//    }

    protected Connection getConnection() throws Ex {
        Connection conn = null;
        if (m_dbSource == null) {
            initDbSource();
        }
        if (m_dbSource == null) {
            return null;
        }
        try {
            conn = m_dbSource.getConnection();
            if (conn == null){
                LogLayout.info(logger,"数据连接建立失败");
                throw new Ex().set(E.E_NullPointer, new Message("{0} Database create connection is error.", m_schemaName));
            }
        } catch (Ex Ex) {
            LogLayout.error(logger,"数据连接建立失败",Ex);
            setDatabaseFailingTime();
            throw Ex;
        }
        return conn;
    }

    protected Connection connection(String appName) throws Ex {
        Connection conn = getConnection();
        if (!isOkay()) {
            Message Message = new Message("数据库异常 {0}.", m_dbName);
            LogLayout.warn(logger, appName, Message.toString());
            sleepTime();
            testDb();
        }
        if (conn == null) {
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        return conn;
    }

    protected void commit(Connection conn) throws Ex {
//        m_LogLayout.info(logger,"platform","commit is execute!");
        try {
            boolean nowCommit = conn.getAutoCommit();
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,"now auto commit = " + nowCommit);
            }
            if (!nowCommit) {
                conn.commit();
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured during processing commit data for database: {0}.", m_schemaName));
            testBadConnection(conn, Exsql);
            throw Exsql;
        }
    }

    protected void returnConnection(Connection con) {
        if (m_dbSource != null)
            m_dbSource.returnConnection(con);
        else {
            DbopUtil.closeConnection(con);
        }

    }

    public boolean isOkay() {
        boolean result = false;
        if (m_dbSource != null)
            result = m_dbSource.isOkay();
        return result;
    }

    public void testDb() {
        try {
            if (m_dbSource != null) {
                m_dbSource.testDatabase();
            } else
                initDbSource();
            if (m_I_dbOp == null) {
                initDbOp();
            }
        } catch (Ex ex) {
            LogLayout.error(logger, m_dbName + "Test Database connection error.", ex);
        }
    }

    public void sleepTime() {
        try {
            Thread.sleep(Int_Thread_SleepTime);
        } catch (InterruptedException ee) {
            // ignore.
        }
    }

    public void testStatus() {
        /*if (!isOkay()) {
            Message Message = new com.inetec.common.i18n.Message("不能连接:{0}.", m_dbName);
            m_LogLayout.warn(logger,"platform",Message.toString());
            if (m_dbSource != null) {
                m_log.setSource_ip(m_dbSource.getDbHost());
                m_log.setUserName(m_dbSource.getDbUser());
            }
            m_log.setStatusCode(EStatus.E_DATABASEERROR.getCode() + "");
            m_log.warn(Message.toString());
            sleepTime();
            testDb();
            throw new Ex().set(E.E_DatabaseConnectionError);
        }
        if (!m_changeMain.isNetWorkOkay()) {
            Message Message = new com.inetec.common.i18n.Message("平台不通");
            m_LogLayout.warn(logger,"platform",Message.toString());
            if (m_dbSource != null) {
                m_log.setSource_ip(m_dbSource.getDbHost());
                m_log.setUserName(m_dbSource.getDbUser());
            }
            m_log.setStatusCode(EStatus.E_NetWorkError.getCode() + "");
            m_log.warn(Message.toString());
            sleepTime();
            throw new Ex().set(EDbChange.E_NetWorkError, Message);
        }*/
    }

    public void initDbType() {
        if (m_dbType != null) {
            if (m_dbType.equals(DBType.C_Oracle)) {
                m_schemaName = m_schemaName.toUpperCase();
            } else if(DBType.C_MSSQL.equals(m_dbType)) {

            }
        }
    }

    public Column[] addTwoTypeColumns(Column[] columns1, Column[] columns2) {
        ArrayList columnList = new ArrayList();
        for (int i = 0; i < columns1.length; i++) {
            columnList.add(columns1[i]);
        }
        for (int i = 0; i < columns2.length; i++) {
            columnList.add(columns2[i]);
        }

        return (Column[]) columnList.toArray(new Column[0]);
    }

    public String getTableQueryString(TableInfo tableInfo,String tableName, Column[] cs) throws Ex {
        if (tableInfo == null) {
            throw new Ex().set(E.E_ObjectNotFound, new Message("The table name {0} is not configed", tableName));
        }
        try {
            StringBuffer result = new StringBuffer();
            int columnSize = cs.length;
            if (columnSize != 0) {
                result.append("select ");
                for (int i = 0; i < columnSize; i++) {
                    String columnName = cs[i].getName();
                    result.append(m_I_dbOp.formatColumnName(columnName));
                    if (i != columnSize - 1) {
                        result.append(",");
                    }
                }
            }
            result.append(" from ");
            result.append(m_I_dbOp.formatTableName(m_schemaName, tableName));
            return result.toString();
        } catch (SQLException eEx) {
            throw new Ex().set(eEx);
        }
    }


}
