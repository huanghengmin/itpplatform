package com.hzih.itp.platform.dbchange.target.plugin.flag;

import com.hzih.itp.platform.dbchange.DbInit;
import com.hzih.itp.platform.dbchange.datautils.*;
import com.hzih.itp.platform.dbchange.datautils.db.*;
import com.hzih.itp.platform.dbchange.datautils.db.pk.PkSet;
import com.hzih.itp.platform.dbchange.datautils.dboperator.DbopUtil;
import com.hzih.itp.platform.dbchange.datautils.dboperator.OracleIDbOp;
import com.hzih.itp.platform.dbchange.exception.EXSql;
import com.hzih.itp.platform.dbchange.source.info.JdbcInfo;
import com.hzih.itp.platform.dbchange.target.info.ColumnMap;
import com.hzih.itp.platform.dbchange.target.info.FilterResult;
import com.hzih.itp.platform.dbchange.target.info.TableMap;
import com.hzih.itp.platform.dbchange.target.plugin.TargetProcessFlag;
import com.hzih.itp.platform.dbchange.target.reg.Expression;
import com.hzih.itp.platform.dbchange.target.reg.Parser;
import com.hzih.itp.platform.dbchange.target.utils.RBufferedInputStream;
import com.hzih.itp.platform.dbchange.target.utils.StringUtil;
import com.hzih.itp.platform.dbchange.target.utils.XmlSaxParser;
import com.hzih.logback.LogLayout;
import com.inetec.common.exception.E;
import com.inetec.common.exception.Ex;
import com.inetec.common.i18n.Message;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 钱晓盼 on 14-1-22.
 */
public class TargetDBService extends DbInit {

    final static Logger logger = LoggerFactory.getLogger(TargetDBService.class);

    private TargetProcessFlag targetProcess;

    private TableMap[] tableMaps;
    private JdbcInfo jdbcInfo;
    private String nativeCharSet;
    private String appName;

    public TargetDBService(TargetProcessFlag targetProcess) {
        this.targetProcess = targetProcess;
        this.appName = targetProcess.getAppName();
    }

    public void setTableMaps(TableMap[] tableMaps) {
        this.tableMaps = tableMaps;
    }

    public void setJdbcInfo(JdbcInfo jdbcInfo) {
        this.jdbcInfo = jdbcInfo;
        m_dbName = jdbcInfo.getDbName();
    }

    public void setNativeCharSet(String nativeCharSet) {
        this.nativeCharSet = nativeCharSet;
    }

    public String getDbName() {
        return jdbcInfo.getDbName();
    }

    /**
     * 基本数据入库处理
     * @param defaultData
     * @return
     */
    public FilterResult processBasicData(DefaultData defaultData) throws Ex {
        boolean passed = true;
        String sourceSchema = defaultData.getSchemaName();
        String sourceTable = defaultData.getTableName();
        String syncTime = defaultData.getHeadValue(DbInit.Str_ChangeTime);
        InputStream basicContentIs = defaultData.getContentStream();
        InputStream basicContentIs2 = new RBufferedInputStream(basicContentIs, (int) defaultData.getContentLength());
        XmlSaxParser parser = new XmlSaxParser();
        Rows basic = parser.parse(basicContentIs2);
        Row[] rows = basic.getRowArray();

        FilterResult filterResult = new FilterResult(rows.length);
        TableMap tableMap = findTableMap(sourceSchema, sourceTable);
        tableMap.setTotalnumber(rows.length);

        /*预处理:
          1: 如果是只插入操作则直接新增,如有例外直接报错,需要调整业务或者业务配置
          2: 如果非只插入操作则需要判断改行记录是否存在, 如果存在新增的操作需要变更成修改
         */
        boolean bDeleteEnable = tableMap.isDeleteEnable();
        boolean isOnlyInsert = tableMap.isOnlyInsert();
        Map<Integer,String> pkMaps = new HashMap<Integer,String>();
        Map<String,String> _pkMaps = new HashMap<String,String>();
        Connection conn = connection(appName);
        for (int i = 0; i < rows.length; i++) {
            try {
                String condition = tableMap.getCondition();//
                if (!"".equals(condition)) {
                    Parser conditionParser = new Parser(condition);
                    Expression express = conditionParser.getExpression();
                    express.setRowData(rows[i]);
                    passed = express.test();
                }
            } catch (SQLException sql) {
                LogLayout.error(logger, appName, sql.getMessage(), sql);
            }
            Column[] srcColumns = rows[i].getColumnArray();
            for (int j = 0; j < srcColumns.length; j++) {
                Column srcColumn = srcColumns[j];
                Column targetColumn = tableMap.findColumnBySourceField(srcColumn.getName());
                if (targetColumn == null) {
                    LogLayout.warn(logger,appName,srcColumn.getName() + " Column not found destField.");
                    rows[i] = null;
                    continue;
                }
                if (!DbopUtil.verifierColumn(srcColumn, targetColumn)) {
                    String log = tableMap.getTargetDb() + "数据源中" + tableMap.getTargetTable() + "数据表的" + "源字段(" + srcColumn.getName() + ")类型(" + srcColumn.getJdbcType() + ")与目标字段("
                            + targetColumn.getName() + ")类型(" + targetColumn.getJdbcType() + ")不匹配";
                    LogLayout.warn(logger,appName,log);
                    rows[i] = null;
                    continue;
                }
                String pks = "";
                if(!isOnlyInsert && (rows[i].isInsertAction() || rows[i].isUpdateAction())) {
                    if (targetColumn.isPk()) {
                        if (!pks.equalsIgnoreCase("")) {
                            pks += ":";
                        }
                        Value value = srcColumn.getValue();
                        String valueString = value == null ? "" : value.getValueString();
                        valueString = new String(Base64.encodeBase64(valueString.getBytes()));
                        pks = targetColumn.getName() + "," + srcColumn.getDbType() + "," + valueString;
                        if (logger.isDebugEnabled()) {
                            LogLayout.debug(logger,appName,"the value type is " + srcColumn.getDbType());
                        }
                        pkMaps.put(i,pks);
                    }
                    // todo : 目标字段设置为NULLABLE
                    if (srcColumn.isNull()) {
                        if (logger.isDebugEnabled()) {
                            LogLayout.debug(logger,appName,"DbChangeSource column values is null.");
                        }
                        targetColumn.setValue(null);
                    } else {
                        targetColumn.setValue(srcColumn.getValue());
                    }
                }
            }
            if(!isOnlyInsert && (rows[i].isInsertAction() || rows[i].isUpdateAction())) {
                boolean isRowExist = isRowExists(tableMap, conn);
                if(isRowExist && rows[i].isInsertAction()) {
                    rows[i].setAction(Operator.OPERATOR__UPDATE);
                } else if(!isRowExist && rows[i].isUpdateAction()) {
                    String pk = pkMaps.get(i);
                    if(_pkMaps.get(pk) == null){   // 确保批量提交时insert只有一条
                        rows[i].setAction(Operator.OPERATOR__INSERT);
                        _pkMaps.put(pk, pk);
                    }
                }
            }
        }
//        commit(conn);
        returnConnection(conn);
//        long l = System.currentTimeMillis() - s;
//        LogLayout.info(logger,appName,"处理basic一次耗时"+l+"毫秒, 用于判断是否存在耗时" + cc + "毫秒,列校验耗时"+cc2+"毫秒");
        conn = connection(appName);
        try {
            DbopUtil.setNotAutoCommit(conn);
        } catch (SQLException e) {
            LogLayout.error(logger,appName,e.getMessage(),e);
        }
        if (isOnlyInsert) {
            for(int i = 0; i < rows.length; i++){
                if(rows[i] == null) {
                    continue;
                }
                if (passed) {
                    filterResult.setIth(i,passed);
                    String targetTable = tableMap.getTargetTable();
                    if (targetTable == null || targetTable.equals("")) {
                        targetTable = sourceTable;
                    }
                    // set value into tableMap
                    String pks = "";
                    Column[] srcColumns = rows[i].getColumnArray();
                    for (int j = 0; j < srcColumns.length; j++) {
                        Column srcColumn = srcColumns[j];
                        Column targetColumn = tableMap.findColumnBySourceField(srcColumn.getName());
                        if (targetColumn.isPk()) {
                            if (!pks.equalsIgnoreCase("")) {
                                pks += ":";
                            }
                            Value value = srcColumn.getValue();
                            String valueString = value == null ? "" : value.getValueString();
                            valueString = new String(Base64.encodeBase64(valueString.getBytes()));
                            pks = targetColumn.getName() + "," + srcColumn.getDbType() + "," + valueString;
                            if (logger.isDebugEnabled()) {
                                LogLayout.debug(logger,appName,"the value type is " + srcColumn.getDbType());
                            }
                        }
                        // todo : 目标字段设置为NULLABLE
                        if (srcColumn.isNull()) {
                            if (logger.isDebugEnabled()) {
                                LogLayout.debug(logger,appName,"DbChangeSource column values is null.");
                            }
                            targetColumn.setValue(null);
                        } else {
                            targetColumn.setValue(srcColumn.getValue());
                        }
                    }
                }
                insertData(tableMap, conn);
            }
        } else {
            for(int i = 0; i < rows.length; i++){
                if(rows[i] == null) {
                    continue;
                }
                if (passed) {
                    filterResult.setIth(i,passed);
                    String targetTable = tableMap.getTargetTable();
                    if (targetTable == null || targetTable.equals("")) {
                        targetTable = sourceTable;
                    }
                    // set value into tableMap
                    String pks = "";
                    Column[] srcColumns = rows[i].getColumnArray();
                    for (int j = 0; j < srcColumns.length; j++) {
                        Column srcColumn = srcColumns[j];
                        Column targetColumn = tableMap.findColumnBySourceField(srcColumn.getName());
                        if (targetColumn.isPk()) {
                            if (!pks.equalsIgnoreCase("")) {
                                pks += ":";
                            }
                            Value value = srcColumn.getValue();
                            String valueString = value == null ? "" : value.getValueString();
                            valueString = new String(Base64.encodeBase64(valueString.getBytes()));
                            pks = targetColumn.getName() + "," + srcColumn.getDbType() + "," + valueString;
                            if (logger.isDebugEnabled()) {
                                LogLayout.debug(logger,appName,"the value type is " + srcColumn.getDbType());
                            }
                        }
                        // todo : 目标字段设置为NULLABLE
                        if (srcColumn.isNull()) {
                            if (logger.isDebugEnabled()) {
                                LogLayout.debug(logger,appName,"DbChangeSource column values is null.");
                            }
                            targetColumn.setValue(null);
                        } else {
                            targetColumn.setValue(srcColumn.getValue());
                        }
                    }
                }
                if (rows[i].isDeleteAction()) {
                    /*if (bDeleteEnable) {
                        deleteRecord(tableMap, conn);
                    } else {
                        LogLayout.warn(logger,appName,"配置中未允许目标端删除数据");

                    }*/
                    LogLayout.warn(logger,appName,"改应用不会存在删除操作,请检查配置或者联系管理员");
                } else if (rows[i].isUpdateAction()) {
                    updateData(tableMap, conn);
                } else if (rows[i].isInsertAction() ) {
                    insertData(tableMap, conn);
                }
            }
        }
        commit(conn);
        returnConnection(conn);
        return filterResult;
    }

    /**
     * blob 批量处理
     * @param dataConsumer
     * @param f
     * @param filename
     * @throws com.inetec.common.exception.Ex
     */
    public void insertOrUpdateBlob(MDataParseImp dataConsumer, FilterResult f, String filename) throws Ex {
        DataInformation[] blobDataInfos = dataConsumer.getBlobDataInfo();
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"BLOB field count: " + blobDataInfos.length);
        }

        Connection conn = connection(appName);
        try {
            DbopUtil.setNotAutoCommit(conn);
        } catch (SQLException e) {
            LogLayout.error(logger,appName,e.getMessage(),e);
        }
        for (int j = 0; j < blobDataInfos.length; j++) {
            ByteLargeObjectData byteLargeObjectData = new ByteLargeObjectData(blobDataInfos[j]);
            byteLargeObjectData.setFilename(filename);
            try {
                insertOrUpdateBlobData(conn, byteLargeObjectData, f);
            } finally {
                byteLargeObjectData.close();
            }
        }
        commit(conn);
        returnConnection(conn);
    }

    /**
     * clob 批量处理
     * @param dataConsumer
     * @param f
     * @param filename
     * @throws com.inetec.common.exception.Ex
     */
    public void insertOrUpdateClob(MDataParseImp dataConsumer, FilterResult f, String filename) throws Ex {
        DataInformation[] clobDataInfos = dataConsumer.getClobDataInfo();

        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"CLOB field count: " + clobDataInfos.length);
        }
        Connection conn = connection(appName);
        try {
            DbopUtil.setNotAutoCommit(conn);
        } catch (SQLException e) {
            LogLayout.error(logger,appName,e.getMessage(),e);
        }
        for (int j = 0; j < clobDataInfos.length; j++) {
            CharLargeObjectData charLargeObjectData = new CharLargeObjectData(clobDataInfos[j]);
            charLargeObjectData.setFilename(filename);
            try {
//                long s = System.currentTimeMillis();
                insertOrUpdateClobData(conn, charLargeObjectData, f);
//                long l = System.currentTimeMillis() - s;
//                LogLayout.info(logger,appName,"处理lob一次耗时"+l+"毫秒");
            } finally {
                charLargeObjectData.close();
            }
        }
        commit(conn);
        returnConnection(conn);
    }

    /**
     * blob字段入库处理
     * @param byteLargeObjectData
     * @param f
     */
    public void insertOrUpdateBlobData(Connection conn,ByteLargeObjectData byteLargeObjectData, FilterResult f) throws Ex {
        if (f!=null && !f.getResult(0)) {        // process only the first record, the unique record
            return;
        }
        String sourceSchema = byteLargeObjectData.getSchemaName();
        String sourceTable = byteLargeObjectData.getTableName();
        String fieldName = byteLargeObjectData.getHeadValue(DataInformation.Str_FieldName);
        String pkString = byteLargeObjectData.getLobPkString();
        long length = byteLargeObjectData.getBlobLength();
        TableMap tableMap = findTableMap(sourceSchema, sourceTable);
        // todo: onlyinsert, deleteEnable and condition.
//        Connection conn = connection(appName);
        InputStream in = null;
        try {
            PkSet pks = new PkSet(pkString);
            Column[] pkColumns = pks.getPkArray();
            int pkSize = pkColumns.length;
            String whereSqlString = lobWhereString(pkColumns, tableMap);
            Object[] params = new Object[pkSize];
            for (int j = 0; j < pkSize; j++) {
                params[j] = pkColumns[j].getObject();
            }
            Column lobColumn = tableMap.findColumnBySourceField(fieldName);
            // put the this object into cache
//            toCache(tableMap, Operator.OPERATOR__UPDATE);
            // todo: length: int->long
            if (lobColumn.getJdbcType() == Types.LONGVARBINARY ||
                    lobColumn.getJdbcType() == Types.VARBINARY || lobColumn.getJdbcType() == Types.BLOB) {
                in = byteLargeObjectData.getBlobInputStream();
            } else {
                in = byteLargeObjectData.getImageInputStream();
            }
            if (lobColumn != null) {
                if ((m_I_dbOp instanceof OracleIDbOp) && (lobColumn.getJdbcType() == Types.LONGVARBINARY ||
                        lobColumn.getJdbcType() == Types.VARBINARY)) {
                    if (logger.isDebugEnabled()) {
                        LogLayout.debug(logger,appName,"Start to insert/update longrow field.");
                    }
                    OracleIDbOp oracleDbms = new OracleIDbOp(conn);

                    oracleDbms.updateLongRawColumn(conn, tableMap.getTargetTable(),
                            lobColumn.getName(), whereSqlString, params, in, (int) length);

                } else if ((m_I_dbOp instanceof OracleIDbOp) && (lobColumn.getJdbcType() == Types.BLOB)) {
                    if (logger.isDebugEnabled()) {
                        LogLayout.debug(logger,appName,"Start to insert/update blob field.");
                    }
                    m_I_dbOp.updateBlobColumn(conn, tableMap.getTargetTable(),
                            lobColumn.getName(), whereSqlString, params, in, (int) length);
                } else {
                    if (logger.isDebugEnabled()) {
                        LogLayout.debug(logger,appName,"Start to insert/update image field.");
                    }
                    m_I_dbOp.updateBlobColumn(conn, tableMap.getTargetTable(),
                            lobColumn.getName(), whereSqlString, params, in, (int) length);
                }
            }
        } catch (SQLException sqlEEx) {
            Message mess = new Message("A SQL exception occured during processing blob data for database/table  " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + ".");
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, mess);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } catch (IOException ioEEx) {
            throw new Ex().set(E.E_IOException, ioEEx, new Message("An IO exception occured during processing blob data for database/table  ", tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
//            returnConnection(conn);
        }
    }

    /**
     * clob字段入库处理
     * @param
     * @param charLargeObjectData
     * @param f
     */
    public void insertOrUpdateClobData(Connection conn,CharLargeObjectData charLargeObjectData, FilterResult f) throws Ex {
        if (!f.getResult(0)) {      // Process the only first row
            return;
        }
        String sourceSchema = charLargeObjectData.getSchemaName();
        String sourceTable = charLargeObjectData.getTableName();
        String fieldName = charLargeObjectData.getHeadValue(DataInformation.Str_FieldName);
        String pkString = charLargeObjectData.getLobPkString();
        long length = charLargeObjectData.getClobLength();

        TableMap tableMap = findTableMap(sourceSchema, sourceTable);

//        Connection conn = connection(appName);
        try {
            PkSet pks = new PkSet(pkString);
            Column[] pkColumns = pks.getPkArray();
            int pkSize = pkColumns.length;
            String whereSqlString = lobWhereString(pkColumns, tableMap);
            Object[] params = new Object[pkSize];
            for (int j = 0; j < pkSize; j++) {
                params[j] = pkColumns[j].getObject();
            }

            Column lobColumn = tableMap.findColumnBySourceField(fieldName);
//            toCache(tableMap, Operator.OPERATOR__UPDATE);
            InputStreamReader reader = new InputStreamReader(charLargeObjectData.getClobInputStream(), DataInformation.Str_CharacterSet);
            if (lobColumn != null) {
                if ((m_I_dbOp instanceof OracleIDbOp) && lobColumn.getJdbcType() == Types.LONGVARCHAR) {
                    if (logger.isDebugEnabled()) {
                        LogLayout.debug(logger,appName,"Start to insert/update longrow field.");
                    }
                    OracleIDbOp oracleDbms = new OracleIDbOp(conn);
                    oracleDbms.updateLongColumn(conn, tableMap.getTargetTable(),
                            lobColumn.getName(), whereSqlString, params, reader, (int) length);
                } else {
                    if (logger.isDebugEnabled()) {
                        LogLayout.debug(logger,appName,"Start to insert/update clob field.");
                    }

                    m_I_dbOp.updateClobColumn(conn, tableMap.getTargetTable(),
                            lobColumn.getName(), whereSqlString, params, reader, (int) length);
                }
                if (reader != null) {
                    reader.close();
                }
            }
        } catch (SQLException e) {
            Message mess = new Message("An error occured in processing CLOB data for  database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + ".");
            EXSql Exsql = m_I_dbOp.sqlToExSql(e, mess);
            testBadConnection(conn, Exsql);
            throw Exsql;
        } catch (IOException ioEEx) {
            throw new Ex().set(E.E_IOException, ioEEx);
        } finally {
//            returnConnection(conn);
        }
    }

    private String lobWhereString(Column[] pkColumns, TableMap tableMap) throws SQLException {
        StringBuffer pkSb = new StringBuffer();
        for (int j = 0; j < pkColumns.length; j++) {
            Column pkColumn = pkColumns[j];
            Column pkTargetColumn = tableMap.findColumnBySourceField(pkColumn.getName());
            pkColumn.setJdbcType(pkTargetColumn.getJdbcType());
            pkTargetColumn.setValue(pkColumn.getValue());
            pkSb.append(m_I_dbOp.formatColumnName(pkTargetColumn.getName()));
            pkSb.append("=? and ");
        }
        String whereSqlString = "where " + StringUtil.getFirstSubString(pkSb.toString(), " and ");
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"Lob where string:" + whereSqlString);
        }
        return whereSqlString;
    }

    /**
     * 判断是否存在同主键的记录
     * @param tableMap
     * @param conn
     * @return
     * @throws com.inetec.common.exception.Ex
     */
    private boolean isRowExists(TableMap tableMap, Connection conn) throws Ex {
//        long k1 = System.currentTimeMillis();
        ArrayList pkColumns = new ArrayList();
        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        for (int i = 0; i < fieldMaps.length; i++) {
            ColumnMap columnMap = fieldMaps[i];
            Column c = columnMap.getTargetColumn();
            if (c.isPk()) {
                pkColumns.add(c);
            }
        }
        PreparedStatement prepStmt = null;
        try {
            String sqlQuery = m_I_dbOp.getSqlProperty(conn, Str_Sql_SelectFromTable,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, tableMap.getTargetTable())});
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"Select sql query:" + sqlQuery);
            }
            String strWhere = getWherePkStringFromFieldMap(fieldMaps);
            sqlQuery += strWhere;
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"All sql string:" + sqlQuery);
            }
//            prepStmt = m_I_dbOp.getSatement(conn, sqlQuery, (Column[]) pkColumns.toArray(new Column[0]));
            prepStmt = m_I_dbOp.getReadOnlySatement(conn, sqlQuery, (Column[]) pkColumns.toArray(new Column[0]));
            if (prepStmt == null) {
                throw new Ex().set(E.E_OperationError, new Message("GetSatement is null by testing a row's existence for database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            }
            prepStmt.setQueryTimeout(I_StatementTimeOut);
            ResultSet rs = prepStmt.executeQuery();
            return rs.next();
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured while testing a row's existence for database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
        }
    }

    private String getWherePkStringFromFieldMap(ColumnMap[] fieldMaps) throws SQLException {

        StringBuffer pkSb = new StringBuffer();
        pkSb.append(" where ");
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"All column are " + c.getName());
            }
            if (c.isPk()) {
                if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"Pk column is " + c.getName());
                }
                pkSb.append(m_I_dbOp.formatColumnName(c.getName()));
                pkSb.append("=?");
                pkSb.append(" and ");
            }
        }
        String strWhere = StringUtil.getFirstSubString(pkSb.toString(), " and ");
        return strWhere;
    }

    /**
     * 入库 -- 新增
     * @param tableMap
     * @param conn
     * @throws com.inetec.common.exception.Ex
     */
    private void insertData(TableMap tableMap, Connection conn) throws Ex {
        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        PreparedStatement prepStmt = null;
        ArrayList valueColumns = new ArrayList();
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            if (!DbopUtil.isLobType(c.getJdbcType())) {
                valueColumns.add(c);
            }
        }

        try {
            // String sqlString = "insert into {0}"
            String sqlString = m_I_dbOp.getSqlProperty(conn, Str_Sql_InsertIntoTable, new String[]{
                    m_I_dbOp.formatTableName(m_schemaName, tableMap.getTargetTable())});

            StringBuffer result = new StringBuffer();
            result.append(sqlString);
            result.append(getInsertSelectColumns(fieldMaps));
            result.append(getColumnValues(fieldMaps));
            String sql = result.toString();
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"insert data sql string:" + sql);
                LogLayout.debug(logger,appName,"Value column size :" + valueColumns.size());
            }
            prepStmt = m_I_dbOp.getSatement(conn, sql, (Column[]) valueColumns.toArray(new Column[0]));
            prepStmt.setQueryTimeout(I_StatementTimeOut);
            int rowInserted = prepStmt.executeUpdate();
            if (rowInserted != 1) {
                throw new Ex().set(E.E_DatabaseError, new Message("insert data error:" + result.toString()));
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured during inserting data for database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
        }
    }

    private String getInsertSelectColumns(ColumnMap[] fieldMaps) throws SQLException {
        String strSelectColumns = " (";
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            strSelectColumns += m_I_dbOp.formatColumnName(c.getName());
            if (i != fieldMaps.length - 1) {
                strSelectColumns += ",";
            }
        }
        strSelectColumns += ")";
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"select columns sql: " + strSelectColumns);
        }
        return strSelectColumns;
    }

    private String getColumnValues(ColumnMap[] fieldMaps) {
        StringBuffer valueSb = new StringBuffer();
        valueSb.append(" values (");
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            if (DbopUtil.isBlobType(c.getJdbcType())) {
                // LONGVARBINARY -->"null,"
                if (m_I_dbOp instanceof OracleIDbOp && (c.getJdbcType() == Types.LONGVARBINARY ||
                        c.getJdbcType() == Types.VARBINARY)) {
                    if (i != fieldMaps.length - 1) {
                        valueSb.append("null,");
                    } else {
                        valueSb.append("null");
                    }
                } else {
                    if (i != fieldMaps.length - 1) {
                        valueSb.append(m_I_dbOp.getBlobInitializer() + ",");
                    } else {
                        valueSb.append(m_I_dbOp.getBlobInitializer());
                    }
                }

            } else if (DbopUtil.isClobType(c.getJdbcType())) {
                if (m_I_dbOp instanceof OracleIDbOp && c.getJdbcType() == Types.LONGVARCHAR) {
                    if (i != fieldMaps.length - 1) {
                        valueSb.append("null,");
                    } else {
                        valueSb.append("null");
                    }
                } else {
                    if (i != fieldMaps.length - 1) {
                        valueSb.append(m_I_dbOp.getClobInitializer() + ",");
                    } else {
                        valueSb.append(m_I_dbOp.getClobInitializer());
                    }
                }
            } else {
                if (i != fieldMaps.length - 1) {
                    valueSb.append("?,");
                } else {
                    valueSb.append("?");
                }
            }
        }
        valueSb.append(")");
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger, appName, "Column value " + valueSb.toString());
        }
        return valueSb.toString();
    }

    /**
     * 入库 -- 删除
     * @param tableMap
     * @param conn
     * @throws com.inetec.common.exception.Ex
     */
    private void deleteRecord(TableMap tableMap, Connection conn) throws Ex {
        // Connection conn = m_dbPool.getConnection();
        PreparedStatement prepStmt = null;
        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        ArrayList columns = new ArrayList();
        for (int i = 0; i < fieldMaps.length; i++) {
            Column c = fieldMaps[i].getTargetColumn();
            if (c.isPk()) {
                columns.add(fieldMaps[i].getTargetColumn());
            }
        }

        try {
            // String sqlString = "delete from {0}"
            String sqlString = m_I_dbOp.getSqlProperty(conn, Str_Sql_DeleteFromTable,
                    new String[]{m_I_dbOp.formatTableName(m_schemaName, tableMap.getTargetTable())});
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"Get delete record query table string:" + sqlString);
            }

            String strWhere = getWherePkStringFromFieldMap(fieldMaps);
            sqlString += strWhere;
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"Delete record sql string:" + sqlString);
            }

            prepStmt = m_I_dbOp.getSatement(conn, sqlString, (Column[]) columns.toArray(new Column[0]));
            prepStmt.setQueryTimeout(I_StatementTimeOut);
            prepStmt.executeUpdate();
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured during deleting a row for database/table: " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
        }
    }

    /**
     * 入库 -- 修改
     * @param tableMap
     * @param conn
     * @throws com.inetec.common.exception.Ex
     */
    private void updateData(TableMap tableMap, Connection conn) throws Ex {

        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        ArrayList valueRows = new ArrayList();
        ArrayList pkRows = new ArrayList();
        // Connection conn  = m_dbPool.getConnection();
        PreparedStatement prepStmt = null;

        for (int i = 0; i < fieldMaps.length; i++) {
            ColumnMap columnMap = fieldMaps[i];
            Column c = columnMap.getTargetColumn();
            if (c.isPk()) {
                pkRows.add(c);
            } else {
                if (!DbopUtil.isLobType(c.getJdbcType())) {
                    valueRows.add(c);
                } //else {}
            }
        }
        valueRows.addAll(pkRows);

        try {
            String updateSql = updateBasicDataSqlString(tableMap);
            if (updateSql.equalsIgnoreCase("")) {
                return;
            } else {
                prepStmt = m_I_dbOp.getSatement(conn, updateSql, (Column[]) valueRows.toArray(new Column[0]));
                prepStmt.setQueryTimeout(I_StatementTimeOut);
                prepStmt.executeUpdate();
            }
        } catch (SQLException sqlEEx) {
            EXSql Exsql = m_I_dbOp.sqlToExSql(sqlEEx, new Message("A SQL exception occured during updating data for datababse/table:  " + tableMap.getTargetDb() + "/" + tableMap.getTargetTable() + "."));
            testBadConnection(conn, Exsql);
            throw Exsql;
        } finally {
            DbopUtil.closeStatement(prepStmt);
        }
    }

    private String updateBasicDataSqlString(TableMap tableMap) throws SQLException {
        boolean bSetable = false;
        StringBuffer result = new StringBuffer();
        StringBuffer valueSb = new StringBuffer();
        StringBuffer pkSb = new StringBuffer();

        result.append("update ");
        result.append(m_I_dbOp.formatTableName(m_schemaName, tableMap.getTargetTable()));
        result.append(" set ");

        ColumnMap[] fieldMaps = tableMap.getFieldMap();
        for (int i = 0; i < fieldMaps.length; i++) {
            ColumnMap columnMap = fieldMaps[i];
            Column c = columnMap.getTargetColumn();
            if (c.isPk()) {
                pkSb.append(m_I_dbOp.formatColumnName(c.getName()));
                pkSb.append("=? and ");
            } else {
                if (DbopUtil.isBlobType(c.getJdbcType())) {

                } else if (DbopUtil.isClobType(c.getJdbcType())) {

                } else {
                    valueSb.append(m_I_dbOp.formatColumnName(c.getName()));
                    valueSb.append("=?,");
                    bSetable = true;
                }
            }
        }

        if (!bSetable) {
            return "";
        }

        result.append(StringUtil.getFirstSubString(valueSb.toString(), ','));
        result.append(" where ");
        result.append(StringUtil.getFirstSubString(pkSb.toString(), " and "));
        String sql = result.toString();
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"update sql string:" + sql);
        }
        return sql;
    }

    private TableMap findTableMap(String sourceSchema, String sourceTable) {
        TableMap tableMap = null;
        for (int j = 0; j < tableMaps.length; j++) {
            tableMap = tableMaps[j];
            if (sourceSchema.equalsIgnoreCase(tableMap.getSourceDb()) &&
                    sourceTable.equalsIgnoreCase(tableMap.getSourceTable())) {
                return tableMap;
            }
        }

        if (tableMap == null) {
            LogLayout.warn(logger, appName, "Can not find DbChangeSource database " + sourceSchema + " and " +
                    "DbChangeSource table " + sourceTable + " in the config mapper");
        }

        return tableMap;
    }


}

