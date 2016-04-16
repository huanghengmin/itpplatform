/*=============================================================
 * 文件名称: DefaultDBTypeMapper.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-11-12
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
package com.hzih.itp.platform.dbchange.datautils.dboperator.sqlbundle;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */
public class DefaultDBTypeMapper {
    /**
     * Uses an algorithm to derive a token for a support database type from
     * connection metadata.
     *
     * @param databaseProductName    DatabaseProductName from database metadata.
     * @param databaseProductVersion DatabaseProductVersion from database metadata.
     */
    protected static DBType getDBType(String databaseProductName, String databaseProductVersion) {
        DBType dbType = null;
        String strWorking;
        String strDbType = null;


        strWorking = databaseProductName.toLowerCase();
        if (strWorking.indexOf("oracle") > -1) {
            return DBType.C_Oracle;
        } else if (strWorking.indexOf("sql server") > -1) {

            return DBType.C_MSSQL;
        } else if (strWorking.indexOf("db2") > -1) {
            return DBType.C_DB2;
        } else if (strWorking.indexOf("adaptive server enterprise") > -1) {
            return DBType.C_SYBASE;
        } else if (strWorking.indexOf("mysql") > -1) {
            return DBType.C_MYSQL;
        } else {
            return DBType.C_Invalid;
        }
    }

    /**
     * Uses an algorithm to derive a token for a support database type from
     * connection metadata.
     *
     * @param connection Open connection, so that database metadata can obtained.
     */
    public static DBType getDBType(Connection connection) throws SQLException {
        DatabaseMetaData connectionMetaData;
        String databaseProductName, databaseProductVersion;

        // check arguments
        if (connection == null || connection.isClosed()) {
            throw new IllegalArgumentException("Need an open connection");
        }

        // get metadata information
        connectionMetaData = connection.getMetaData();
        databaseProductName = connectionMetaData.getDatabaseProductName();
        databaseProductVersion = connectionMetaData.getDatabaseProductVersion();
        String driverName=connectionMetaData.getDriverName().toLowerCase();
        DBType type= getDBType(databaseProductName, databaseProductVersion);
        if(type.equals(DBType.C_MSSQL)&&driverName.indexOf("jtds")>-1){
            if(databaseProductName.equals("Microsoft SQL Server")){
                type= DBType.C_MSSQL;
            }
            if(databaseProductName.equals("sql server")){
                type=DBType.C_SYBASE;
            }
        }
        return type;
    }


}
