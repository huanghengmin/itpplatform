/*=============================================================
 * 文件名称: DatabaseInfo.java
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
package com.hzih.itp.platform.dbchange.source.info;

import com.hzih.itp.platform.dbchange.datautils.db.Column;
import com.hzih.itp.platform.dbchange.datautils.dboperator.DbopUtil;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.Field;
import com.inetec.common.exception.Ex;
import com.inetec.common.config.stp.nodes.Table;
import com.inetec.common.config.stp.nodes.DataBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class TableInfo {


    private String name;
    private boolean delete;
    private boolean specifyFlag;
    private boolean triggerEnable;
    private boolean allTableEnable;
    private boolean isTimeSync;

    private int sequence;
    private int interval;
    private boolean monitorInsert;
    private boolean monitorUpdate;
    private boolean monitorDelete;
    private boolean mergeTable = false;

    private Column[] basicColumns = new Column[0];
    private Column[] clobColumns = new Column[0];
    private Column[] blobColumns = new Column[0];
    private Map mergeTables = new HashMap();
    private Column[] pkColumns = new Column[0];

    private long nextTime = System.currentTimeMillis();

    private boolean m_twoWay;
    private Logger logger = LoggerFactory.getLogger(TableInfo.class);

    public TableInfo(Table tableConfigNode, String operation) throws Ex {
        name = tableConfigNode.getTableName();
        String strValue;
        delete = tableConfigNode.isDeleteEnable();
        sequence = tableConfigNode.getSeqNumber();
        interval = tableConfigNode.getInterval();
        monitorInsert = tableConfigNode.isMonitorInsert();
        monitorUpdate = tableConfigNode.isMonitorUpdate();
        monitorDelete = tableConfigNode.isMonitorDelete();
        m_twoWay = tableConfigNode.isTwoway();
        triggerEnable = monitorInsert || monitorDelete || monitorUpdate;
        delete = operation.equalsIgnoreCase(DataBase.Str_DeleteOperation) ? true : false;
        specifyFlag = operation.equalsIgnoreCase(DataBase.Str_FlagOperation) ? true : false;
        triggerEnable = operation.equalsIgnoreCase(DataBase.Str_TriggerOperation) ? true : false;
        allTableEnable = operation.equalsIgnoreCase(DataBase.Str_AllTableOperation) ? true : false;
        isTimeSync = operation.equalsIgnoreCase(DataBase.Str_TimeSyncOperation) ? true : false;

        ArrayList basicFieldList = new ArrayList();
        ArrayList clobFieldList = new ArrayList();
        ArrayList blobFieldList = new ArrayList();
        ArrayList pkFieldList = new ArrayList();
        if (tableConfigNode.getAllMergeTables().length > 0) {
            mergeTable = true;
            for (int i = 0; i < tableConfigNode.getAllMergeTables().length; i++) {
                setMergeTableInfo(MergeTableInfo.MergeTableInfo(tableConfigNode.getAllMergeTables()[i], name));
            }
        }

        Field[] fields = (Field[]) tableConfigNode.getAllFields();
        for (int i = 0; i < fields.length; i++) {
            int jdbcType = DbopUtil.getJdbcType(fields[i].getJdbcType());
            Column c = new Column(fields[i].getFieldName(), DbopUtil.getJdbcType(fields[i].getJdbcType()),
                    fields[i].getDbType(), fields[i].isPk());
            if (c.isPk()) {
                pkFieldList.add(c);
            }
            if (c.isBlobType()) {
                blobFieldList.add(c);
            } else if (c.isClobType()) {
                clobFieldList.add(c);
            } else {

                basicFieldList.add(c);
            }

        }
        basicColumns = (Column[]) basicFieldList.toArray(new Column[0]);
        clobColumns = (Column[]) clobFieldList.toArray(new Column[0]);
        blobColumns = (Column[]) blobFieldList.toArray(new Column[0]);
        pkColumns = (Column[]) pkFieldList.toArray(new Column[0]);

        //checkConfigInfo();

    }


    public Column find(String fieldName) {

        for (int i = 0; i < basicColumns.length; i++) {
            if (basicColumns[i].getName().trim().equalsIgnoreCase(fieldName.trim())) {
                return basicColumns[i];
            }
        }
        for (int i = 0; i < clobColumns.length; i++) {
            if (clobColumns[i].getName().trim().equalsIgnoreCase(fieldName.trim())) {
                return clobColumns[i];
            }
        }
        for (int i = 0; i < blobColumns.length; i++) {
            if (blobColumns[i].getName().trim().equalsIgnoreCase(fieldName.trim())) {
                return blobColumns[i];
            }
        }

        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,"Can not find field name.");
        }
        return null;
    }

    public Column[] getBasicColumns() {
        return basicColumns;
    }

    public Column[] getBlobColumns() {
        return blobColumns;
    }

    public Column[] getClobColumns() {
        return clobColumns;
    }

    public Column[] getPkColumns() {
        return pkColumns;
    }

    public Column getTimeSyncTimeField() {
        Column result = null;
        if (isTimeSync)
            for (int i = 0; i < pkColumns.length; i++) {
                if (pkColumns[i].getJdbcType() == Types.DATE || pkColumns[i].getJdbcType() == Types.TIME || pkColumns[i].getJdbcType() == Types.TIMESTAMP) {
                    result = pkColumns[i];
                }
            }
        return result;
    }
    /*
    //todo: check the code
    public void setTableMetadata(TableMetadata tableMetadata) {
        this.tableMetadata = tableMetadata;
        if (specifyFields) {
            Set keySet = fieldProp.keySet();
            Iterator it = keySet.iterator();
            while (it.hasNeExt()) {
                String srcField = (String)it.next();
                Column c = tableMetadata.find(srcField);
                if (c.isLobType()) {
                    fieldsInfo.addLast(srcField);
                } else {
                    fieldsInfo.addFirst(srcField);
                }
            }
        } else {
            Column[] cs = tableMetadata.getColumns();
            int size = cs.length;
            for (int i=0; i<size; i++) {
                String srcField = cs[i].getName();
                Column c = tableMetadata.find(srcField);
                if (c.isLobType()) {
                    fieldsInfo.addLast(srcField);
                } else {
                    fieldsInfo.addFirst(srcField);
                }
            }

        }

    }
    */


    public void checkConfigInfo() {

        //boolean invalid = monitorDelete
        if (monitorDelete && delete) {
            //if (logger.isDebugEnabled()) {
            LogLayout.warn(logger,"platform","monitorDelete and delete value are not allowed to both set true value, table: " + name);
            //}
        }
        if ((monitorInsert || monitorUpdate) && specifyFlag) {
            //if (logger.) {
            LogLayout.warn(logger,"platform","monitorInsert/monitorUpdate and specifyFlag value are not allowed to both set true value, table: " + name);
            //}
        }
        if (!(monitorDelete || delete || monitorUpdate || monitorInsert || specifyFlag)) {
            if (logger.isDebugEnabled()) {
                logger.debug("monitorInsert, monitorUpdate, monitorDelete, specifyFlag and delete value are not allowed to all set false value, table: " + name);
            }
        }

        delete = delete && (!monitorDelete) && !(specifyFlag && (monitorInsert || monitorUpdate));
        specifyFlag = specifyFlag && (!delete) && (!(monitorInsert || monitorUpdate));
        if (logger.isDebugEnabled()) {
            logger.debug("delete are set to " + delete + " in table " + name);
            logger.debug("specifyFlag are set to " + specifyFlag + " in table " + name);
        }
        /*if(!( m_twoWay&&triggerEnable)) {
            LogLayout.warn(logger,"platform","Twoway and monitor allowed to both set true value,table:"+name);
        }*/
    }


    public long getNeExtTime() {
        return nextTime;
    }

    public void nextTime() {
        nextTime += interval * 1000;
    }


    public String getName() {
        return name;
    }

    public boolean isDelete() {
        return delete;
    }

    public boolean isSpecifyFlag() {
        return specifyFlag;
    }

    public boolean isTriggerEnable() {
        return triggerEnable;
    }


    public int getSequence() {
        return sequence;
    }

    public int getInterval() {
        return interval;
    }

    public boolean isMonitorInsert() {
        return monitorInsert;
    }

    public boolean isMonitorUpdate() {
        return monitorUpdate;
    }

    public boolean isMonitorDelete() {
        return monitorDelete;
    }

    public boolean isTwoway() {
        return m_twoWay;
    }

    public boolean isMergeTable() {
        return mergeTable;
    }

    public boolean isTimeSync() {
        return isTimeSync;
    }

    public void setTimeSync(boolean timeSync) {
        isTimeSync = timeSync;
    }

    public void setMergeTableInfo(MergeTableInfo info) {
        mergeTables.put(info.getMergeTableName(), info);
    }

    public MergeTableInfo[] getAllMergeTable() {

        return (MergeTableInfo[]) mergeTables.values().toArray(new MergeTableInfo[0]);
    }

    public MergeTableInfo getMergeTableByName(String name) {

        return (MergeTableInfo) mergeTables.get(name);
    }

}
