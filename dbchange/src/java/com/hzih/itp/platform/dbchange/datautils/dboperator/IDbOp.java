/*=============================================================
 * 文件名称: IDbOp.java
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

package com.hzih.itp.platform.dbchange.datautils.dboperator;


import com.hzih.itp.platform.dbchange.datautils.db.Column;
import com.hzih.itp.platform.dbchange.exception.EXSql;
import com.inetec.common.i18n.Message;

import java.sql.*;
import java.io.Reader;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-5-20
 * Time: 9:42:10
 * To change this template use File | Settings | File Templates.
 */

public interface IDbOp {

    public int getJdbcTypeFromVenderDb(String type);

    public ResultSet executeQuery(Connection conn, String sqlString) throws SQLException;

    public void executeUpdate(Connection conn, String sqlString) throws SQLException;

    public void call(Connection conn, String sqlString, String[] params) throws SQLException;

    public String getSqlProperty(Connection conn, String sqlKey) throws SQLException;

    public String getSqlProperty(Connection conn, String sqlKey, String[] params) throws SQLException;

    public String formatColumnName(String columnName) throws SQLException;

    public String formatTableName(String schemaName, String tableName) throws SQLException;

    public boolean isFieldEExist(Connection conn, String catalog, String schemaName,
                                 String tableName, String columnName) throws SQLException;

    public boolean isTableEExist(Connection conn, String catalog, String schemaName, String tableName) throws SQLException;



    public long getClobLength(Connection conn, String schemaName,
                              String tableName, String columnName) throws SQLException;

    public String getClobInitializer();

    public long getBlobLength(Connection conn, String schemaName,
                              String tableName, String columnName) throws SQLException;

    public String getBlobInitializer();

    public Column getColumnData(Column column, ResultSet rs, int indeEx) throws SQLException;

    public PreparedStatement getSatement(Connection conn, String sqlString, Column[] columns) throws SQLException;
    public PreparedStatement getReadOnlySatement(Connection conn, String sqlString, Column[] columns) throws SQLException;

    public EXSql sqlToExSql(SQLException e, Message Message);

    /**
     * Update a CLOB column value.
     *
     * @param conn   The database Connection to use.
     * @param table  The table name.
     * @param column The column name.
     * @param where  The SQL where clause.
     * @param params The parameters for the where clause (null if none needed)
     * @param value  A stream that provides the new CLOB value.
     * @param length The length of the new CLOB value.
     * @throws java.sql.SQLException        if the driver encounters an error.
     * @throws java.io.IOException if an I/O error occurs.
     */
    public void updateClobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, Reader value, int length)
            throws SQLException, IOException;

    /**
     * Update a CLOB column value.
     *
     * @param conn   The database Connection to use.
     * @param table  The table name.
     * @param column The column name.
     * @param where  The SQL where clause.
     * @param params The parameters for the where clause (null if none needed)
     * @param value  The new CLOB value.
     * @throws java.sql.SQLException if the driver encounters an error.
     */
    public void updateClobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, String value)
            throws SQLException;


    /**
     * Update a BLOB column value.
     *
     * @param conn   The database Connection to use.
     * @param table  The table name.
     * @param column The column name.
     * @param where  The SQL where clause.
     * @param params The parameters for the where clause (null if none needed)
     * @param value  A stream that provides the new BLOB value.
     * @param length The length of the new BLOB value.
     * @throws java.sql.SQLException if the driver encounters an error.
     * @throws java.io.IOException  if an I/O error occurs.
     */
    public void updateBlobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, InputStream value, int length)
            throws SQLException, IOException;

    /**
     * Update a BLOB column value.
     *
     * @param conn   The database Connection to use.
     * @param table  The table name.
     * @param column The column name.
     * @param where  The SQL where clause.
     * @param params The parameters for the where clause (null if none needed)
     * @param value  The new BLOB value.
     * @throws java.sql.SQLException if the driver encounters an error.
     */
    public void updateBlobColumn(Connection conn, String table, String column,
                                 String where, Object[] params, byte[] value)
            throws SQLException;


}

