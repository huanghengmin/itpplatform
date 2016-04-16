package com.hzih.itp.platform.dbchange;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.dbchange.source.info.DatabaseInfo;
import com.hzih.itp.platform.dbchange.source.info.DbChangeSourceInfo;
import com.hzih.itp.platform.dbchange.target.TargetOperation;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.IChange;
import com.inetec.common.config.stp.nodes.Jdbc;
import com.inetec.common.config.stp.nodes.Type;
import com.inetec.common.db.stp.DataSourceFactory;
import com.inetec.common.exception.Ex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by 钱晓盼 on 14-1-14.
 */
public class DBChangeTarget extends Thread{
    final static Logger logger = LoggerFactory.getLogger(DBChangeTarget.class);

    private boolean isRun = false;
    private List<Type> typeList = new ArrayList<Type>();
    private Map<String,Type> typeMap = new HashMap<String,Type>();

    public void init() {
        typeList = ChangeConfig.loadTypeList(Type.s_app_db, StaticField.InternalConfig);
        typeMap = ChangeConfig.loadTypeMap(typeList);
        Jdbc[] jdbcs = ChangeConfig.loadIChange(StaticField.InternalConfig).getAllJdbcs();
        try {
            DataSourceFactory.init(jdbcs, DataSourceFactory.I_dbTestSleepTime);
        } catch (Ex ex) {
            LogLayout.error(logger,"加载-启动 数据源 错误",ex);
        }
    }

    public void run() {
        isRun = true;
        String appName;
        InetSocketAddress appAddress;
        for(Type type : typeList) {
            appName = type.getTypeName();
//            appAddress = new InetSocketAddress(ChangeConfig.channel.gettIp(), Integer.parseInt(type.getChannelPort()));
//            ChangeConfig.targetAddresses.put(type.getTypeName(),appAddress);
            DbChangeSourceInfo dbSyncInfo = null;
            IChange exIChange = ChangeConfig.loadIChange(StaticField.ExternalConfig);
            try {
                dbSyncInfo = new DbChangeSourceInfo(exIChange.getType(appName).getPlugin(), exIChange);
            } catch (Ex ex) {
                LogLayout.error(logger,appName,"加载DbChangeSourceInfo错误",ex);
            }
            TargetOperation targetOperation = new TargetOperation();
            targetOperation.init(type,dbSyncInfo.getDatabases()[0],exIChange.getType(appName));
            new Thread(targetOperation).start();
            LogLayout.info(logger,"platform","应用"+appName+"启动...");
            LogLayout.info(logger, appName, "应用启动...");
        }
        while (isRun) {
            try {

                Thread.sleep(100000);
            } catch (InterruptedException e) {
            }
        }
    }

    public boolean isRunning() {
        return isRun;
    }

    public void close() {
        isRun = false;
    }
}
