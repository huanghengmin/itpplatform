package com.hzih.itp.platform.filechange;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.filechange.source.SourceOperation;
import com.hzih.itp.platform.filechange.utils.jdbc.SqliteUtil;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by 钱晓盼 on 14-1-9.
 * 1. 加载配置文件-应用
 * 2. 启动应用
 */
public class FileChangeSource extends Thread {
    final static Logger logger = LoggerFactory.getLogger(FileChangeSource.class);

    private boolean isRun = false;
    private List<Type> typeList = new ArrayList<Type>();         //文件同步应用列表
    private Map<String,Type> typeMap = new HashMap<String,Type>();      //文件同步应用map,key:typeName(value),value:type

    public void init() {
        typeList = ChangeConfig.loadTypeList(Type.s_app_file, StaticField.ExternalConfig);
        typeMap = ChangeConfig.loadTypeMap(typeList);
        SqliteUtil.init();
    }

    public void run() {
        isRun = true;
        String appName;
        InetSocketAddress appAddress;
        for(Type type : typeList) {
            appAddress = new InetSocketAddress(ChangeConfig.channel.gettIp(), Integer.parseInt(type.getChannelPort()));
            ChangeConfig.targetAddresses.put(type.getTypeName(),appAddress);
            SourceOperation sourceOperation = new SourceOperation();
            sourceOperation.init(type);
            new Thread(sourceOperation).start();
            appName = type.getTypeName();
            LogLayout.info(logger,"platform","应用"+appName+"启动...");
            LogLayout.info(logger,appName,"应用启动...");
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
