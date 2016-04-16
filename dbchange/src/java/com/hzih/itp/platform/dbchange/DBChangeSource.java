package com.hzih.itp.platform.dbchange;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.dbchange.source.SourceOperation;
import com.hzih.itp.platform.dbchange.source.info.DatabaseInfo;
import com.hzih.itp.platform.dbchange.source.info.DbChangeSourceInfo;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
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
public class DBChangeSource extends Thread{
    final static Logger logger = LoggerFactory.getLogger(DBChangeSource.class);

    private boolean isRun = false;
    private List<Type> typeList = new ArrayList<Type>();           //数据库同步应用列表
    private Map<String,Type> typeMap = new HashMap<String,Type>();         //数据库同步应用MAP,key:typeName(value),value:type

    public void init() {
        typeList = ChangeConfig.loadTypeList(Type.s_app_db, StaticField.ExternalConfig);
        typeMap = ChangeConfig.loadTypeMap(typeList);
        Jdbc[] jdbcs = ChangeConfig.ichange.getAllJdbcs();
        try {
            DataSourceFactory.init(jdbcs,DataSourceFactory.I_dbTestSleepTime);
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
            try{
                String ip = ChangeConfig.channel.gettIp();
                appAddress = new InetSocketAddress(ip, Integer.parseInt(type.getChannelPort()));       //tip,channelport
                ChangeConfig.targetAddresses.put(type.getTypeName(),appAddress);
                ChangeConfig.addHttpClientPool(appName);
                DbChangeSourceInfo dbSyncInfo = null;
                try {
                    dbSyncInfo = new DbChangeSourceInfo(type.getPlugin(), ChangeConfig.ichange);
                } catch (Ex ex) {
                    LogLayout.error(logger,appName,"加载DbChangeSourceInfo错误",ex);
                }
                DatabaseInfo[] databaseInfos = dbSyncInfo.getDatabases();
                SourceOperation sourceOperation = new SourceOperation();
                sourceOperation.init(this,type,databaseInfos[0]);
                new Thread(sourceOperation).start();
                LogLayout.info(logger,"platform","应用"+appName+"启动...");
                LogLayout.info(logger, appName, "应用启动...");
            } catch (Exception e) {
                LogLayout.error(logger,appName,"启动错误",e);
            }
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
