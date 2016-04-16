package com.hzih.itp.platform.dbchange.target.plugin;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.dbchange.datautils.DataInformation;
import com.hzih.itp.platform.dbchange.datautils.DefaultData;
import com.hzih.itp.platform.dbchange.datautils.MDataParseImp;
import com.hzih.itp.platform.dbchange.datautils.SuccessData;
import com.hzih.itp.platform.dbchange.datautils.db.Column;
import com.hzih.itp.platform.dbchange.datautils.dboperator.DbopUtil;
import com.hzih.itp.platform.dbchange.source.info.DatabaseInfo;
import com.hzih.itp.platform.dbchange.source.info.JdbcInfo;
import com.hzih.itp.platform.dbchange.target.TargetOperation;
import com.hzih.itp.platform.dbchange.target.info.ColumnMap;
import com.hzih.itp.platform.dbchange.target.info.FilterResult;
import com.hzih.itp.platform.dbchange.target.info.TableMap;
import com.hzih.itp.platform.dbchange.target.info.TableMapSet;
import com.hzih.itp.platform.dbchange.target.plugin.timesync.TargetDBService;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.*;
import com.inetec.common.exception.E;
import com.inetec.common.exception.Ex;
import com.inetec.common.i18n.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by 钱晓盼 on 14-1-14.
 */
public class TargetProcessTimeSync implements ITargetProcess {
    final static Logger logger = LoggerFactory.getLogger(TargetProcessTimeSync.class);

    private boolean isRun = false;
    private TargetOperation target;
    private Type type;
    private String appName;
    public BlockingQueue<String> queue;

    /**
     * 源表信息
     */
    private DatabaseInfo databaseInfo;

    /*
    目标表信息
     */
    private TableMapSet m_tableMaps = new TableMapSet();
    private IChange ichange;
    private TargetDBService[] targetDBServices;

    public TargetDBService[] getTargetDBServices() {
        return targetDBServices;
    }

    public TargetOperation getTarget() {
        return target;
    }

    @Override
    public boolean process(String tempFileName) {
        queue.offer(tempFileName);
        return true;
    }

    private String pollQueue() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
        }
        return null;
    }

    @Override
    public boolean process(byte[] data) {
        return false;
    }

    @Override
    public void init(TargetOperation target, DatabaseInfo databaseInfo) {
        this.target = target;
        this.type = target.getType();
        this.appName = type.getTypeName();
        this.databaseInfo = databaseInfo;
        queue = new LinkedBlockingQueue<String>(20);
        config();
    }

    public String getAppName() {
        return appName;
    }

    private void config() {
        this.ichange = ChangeConfig.loadIChange(StaticField.InternalConfig);
        Map<String,Jdbc> jdbcs = new HashMap<String, Jdbc>();   //目标端需要入库的数据库个数
        SourceDb srcdb = type.getPlugin().getSourceDb(); //源端jdbc有且只有一个
        if (srcdb == null) {
//            m_bConfigured = false;
            return;
        }
        SourceTable[] srctables = (SourceTable[]) srcdb.getAllSourceTables(); //源端一个jdbc下有一个或多个表格信息
        for (int i = 0; i < srctables.length; i++) {
            TargetDb[] targetdb = srctables[i].getAllTargetDbstoArray();    //目标端一个源端表格对应一个或者多个目标端jdbc
            for (int j = 0; j < targetdb.length; j++) {
                jdbcs.put(targetdb[j].getDbName(), ichange.getJdbc(targetdb[j].getDbName()));
                Table targetTables = targetdb[j].getTable();
                //for (int z=0;z<targetTables.length;z++) {
                TableMap tableMap = new TableMap(srcdb.getDbName(),
                        srctables[i].getTableName(),
                        targetdb[j].getDbName(),
                        targetTables.getTableName());
                tableMap.setDeleteEnable(targetTables.isDeleteEnable());
                tableMap.setOnlyInsert(targetTables.isOnlyinsert());
                tableMap.setCondition(targetTables.getCondition());
                Field[] fields = targetTables.getAllFields();
                for (int n = 0; n < fields.length; n++) {
                    Column targetColumn = new Column(fields[n].getDestField(),
                            DbopUtil.getJdbcType(fields[n].getJdbcType()),
                            fields[n].getDbType(),
                            fields[n].isPk());
                    String srcColumnName = fields[n].getFieldName();
                    ColumnMap columnMap = new ColumnMap(srcColumnName, targetColumn);
                    tableMap.AddFieldMap(columnMap);
                }
                m_tableMaps.AddTableMap(tableMap);
            }
        }

        Jdbc[] jdbcarray = null;
        if (jdbcs.size() > 0) {
            jdbcarray = (Jdbc[]) jdbcs.values().toArray(new Jdbc[0]);
        }
        targetDBServices = new TargetDBService[jdbcs.size()];
        for (int i = 0; i < jdbcs.size(); i++) {
            TargetDBService targetDBService = new TargetDBService(this);
            // todo: cache and encoding
            // set table mapping
            TableMap[] tableMaps = m_tableMaps.findByTarget(jdbcarray[i].getJdbcName());
            targetDBService.setTableMaps(tableMaps);

            // set jdbc info
            JdbcInfo jdbcInfo = new JdbcInfo(jdbcarray[i]);
            targetDBService.setJdbcInfo(jdbcInfo);
            targetDBService.setNativeCharSet(jdbcarray[i].getEncoding());
            targetDBServices[i] = targetDBService;
        }
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
        String filePath;
        while (isRun) {
            filePath = pollQueue();
            long s = System.currentTimeMillis();
            try {
                process(m_tableMaps,filePath);
            } catch (Exception e) {
                LogLayout.error(logger, appName, "时间标记同步入库处理失败", e);
            }
            long l = System.currentTimeMillis() - s;
            LogLayout.info(logger,appName,"完成一次入库处理, 耗时"+l+"豪秒, 缓存列表已有"+queue.size()+"个");
        }
    }

    public boolean process(TableMapSet m_tableMaps, String filePath) throws Ex {
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"Start to process " + filePath);
        }
        ArrayList exceptions = new ArrayList();
        MDataParseImp dataConsumer = new MDataParseImp(filePath);
        try {
            if (dataConsumer.isRecover()) {
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"Start to recover.");
                }
                DataInformation[] successDataInfos = dataConsumer.getSuccessDataInfo();
                SuccessData[] successDatas = new SuccessData[successDataInfos.length];
                ArrayList recoverTargetList = new ArrayList();
                for (int i = 0; i < successDatas.length; i++) {
                    successDatas[i] = new SuccessData(successDataInfos[i]);
                    successDatas[i].setFilename(filePath);
                }
                boolean isRecover = false;
                for (int i = 0; i < targetDBServices.length; i++) {
                    isRecover = false;
                    for (int j = 0; j < successDatas.length; j++) {

                        if (targetDBServices[i].getDbName().equals(successDatas[j].getTargetSchema())) {
                            isRecover = true;
                        }
                    }
                    if (isRecover) {
                        recoverTargetList.add(targetDBServices[i].getDbName());
                    }
                }
                String targetName = "";
                for (int i = 0; i < recoverTargetList.size(); i++) {
                    targetName = (String) recoverTargetList.get(i);
                    try {
                        processOneTargetSchema(targetName, dataConsumer,filePath);
                        dataConsumer.succeedToConsumer(targetName);
                    } catch (Ex Ex) {
                        //dataConsumer.failedToConsumer(successData.getTargetSchema(), 1, Ex.getMessage(), Ex);
                        exceptions.add(Ex);
                    } catch (IllegalArgumentException e) {
                        //dataConsumer.failedToConsumer(successData.getTargetSchema(), 1, e.getMessage(), e);
                        exceptions.add(new Ex().set(E.E_Unknown, e));
                    }
                }
                /*int number = 0;
                if (exceptions.size() > 0) {      // Exceptions occured
                    Message Message = new Message(exceptions.size() + " Errors occured in data recovery; see below for details.");
                    m_LogLayout.error(logger,"platform",Message.toString());

                    int n = exceptions.size();
                    for (int i = 0; i < n; i++) {
                        Ex Ex = (Ex) exceptions.get(i);
                        Message = new Message("Detailed information of error " + "" + (i + 1) + ": ");
                        m_LogLayout.error(logger,"platform",Message.toString() + exceptionProcess(Ex), Ex);
                        m_log.setStatusCode(EStatus.E_DbChangeTargetProcessFaild.getKey().toString());
                        m_log.error(Message.toString() + " " + exceptionProcess(Ex), Ex);
                        if (Ex.getErrcode().equals(E.E_DatabaseConnectionError)) {
                            number++;
                        }
                    }
                    if (number == exceptions.size()) {
                        dataProps.setStatus(Status.S_Faild_TargetProcess);
                        return dataProps;
                    } else {
                        throw new Ex().set(E.E_Unknown, new Message(n + " Errors occured during recovery; error messages as detailed."));
                    }
                }*/
            } else {
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"Start to process normal data" + filePath + " ...");
                }
                DataInformation basicDataInformation = dataConsumer.getBasicDataInfo();
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"Get basic data...");
                }
                DefaultData defaultData = new DefaultData(basicDataInformation);
                String schemaName = defaultData.getSchemaName();
                String tableName = defaultData.getTableName();
                defaultData.close();
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"Find table maps by DbChangeSource(" + schemaName + ", " + tableName + ")...");
                }

                TableMap[] tableMaps = m_tableMaps.findBySource(schemaName, tableName);
                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"TableMapSet length is: " + tableMaps.length);
                }
                if (tableMaps.length == 0) {
                    throw new Ex().set(E.E_OperationError, new Message("Find table maps is empty by DbChangeSource {0}, tableName {1}", schemaName, tableName));
                }

                for (int i = 0; i < tableMaps.length; i++) {
                    TableMap tableMap = tableMaps[i];
                    String targetSchema = tableMap.getTargetDb();
                    try {
                        processOneTargetSchema(targetSchema, dataConsumer,filePath);
                        dataConsumer.succeedToConsumer(targetSchema);
                    } catch (Ex Ex) {
                        LogLayout.error(logger,appName,"Failed to process one target schema.", Ex);
                        exceptions.add(Ex);
                    } catch (IllegalArgumentException e) {
                        exceptions.add(new Ex().set(E.E_Unknown, e));
                    } finally {

                    }
                }

                if (logger.isDebugEnabled()) {
                    LogLayout.debug(logger,appName,"Finish to process " + filePath + " data.");
                }
                /*if (exceptions.size() > 0) {      // Exceptions occured
                    Message Message = new Message(exceptions.size() + " Errors occured in data processing; see below for details.");
                    m_LogLayout.error(logger,"platform",Message.toString());

                    int number = 0;
                    int n = exceptions.size();
                    for (int i = 0; i < n; i++) {
                        Ex Ex = (Ex) exceptions.get(i);
                        Message = new Message("Detailed information of error " + "" + (i + 1) + ": ");
                        m_LogLayout.error(logger,"platform",Message.toString() + " " + exceptionProcess(Ex), Ex);
                        m_log.setStatusCode(EStatus.E_DbChangeTargetProcessFaild.getKey().toString());
                        m_log.error(Message.toString() + " " + exceptionProcess(Ex), Ex);
                        if (Ex.getErrcode().equals(E.E_DatabaseConnectionError)) {
                            number++;
                        }
                    }
                    if (number == exceptions.size()) {
                        dataProps.setStatus(Status.S_Faild_TargetProcess);
                        return dataProps;
                    } else {
                        throw new Ex().set(E.E_Unknown, new Message(n + " Errors occured during data processing; error messages as detailed."));
                    }
                }*/
            }
        } finally {
            dataConsumer.close();
        }
        return true;
    }

    private void processOneTargetSchema( String schema, MDataParseImp dataConsumer, String filename) throws Ex {
        TargetDBService operation = getTargetDBServices(schema);
        DataInformation basicDataInformation = dataConsumer.getBasicDataInfo();
        DefaultData defaultData = new DefaultData(basicDataInformation);
        defaultData.setFilename(filename);
        FilterResult f = null;
        try {
//            long s = System.currentTimeMillis();
            f = operation.processBasicData(defaultData);
//            long l = System.currentTimeMillis() - s;
//            LogLayout.info(logger,appName,"处理基本数据一次耗时"+l+"毫秒");
        } catch (Exception e) {
            LogLayout.error(logger,appName,"时间标记同步入库异常",e);
        } finally {
            defaultData.close();
        }
//        s = System.currentTimeMillis();
        // blob
        operation.insertOrUpdateBlob(dataConsumer, f, filename);
        // clob
        operation.insertOrUpdateClob(dataConsumer, f, filename);
//        l = System.currentTimeMillis() - s;
//        LogLayout.info(logger,appName,"处理lob数据一次耗时"+l+"毫秒");
    }

    protected TargetDBService getTargetDBServices(String schemaName) throws Ex {
        TargetDBService result = null;
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,appName,"schemaName " + schemaName);
        }
        for (int i = 0; i < targetDBServices.length; i++) {
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"targetDBServices["+i+"] is " + targetDBServices[i].getDbName());
            }
            if (schemaName.equalsIgnoreCase(targetDBServices[i].getDbName())) {
                result = targetDBServices[i];
                break;
            }
        }
        return result;
    }
}
