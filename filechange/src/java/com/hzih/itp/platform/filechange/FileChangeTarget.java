package com.hzih.itp.platform.filechange;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.filechange.target.TargetOperation;
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
 */
public class FileChangeTarget extends Thread{

    final static Logger logger = LoggerFactory.getLogger(FileChangeTarget.class);

    private boolean isRun = false;
    private List<Type> typeList = new ArrayList<Type>();
    private Map<String,Type> typeMap = new HashMap<String,Type>();

    public void init() {
        typeList = ChangeConfig.loadTypeList(Type.s_app_file, StaticField.InternalConfig);
        typeMap = ChangeConfig.loadTypeMap(typeList);
    }

    public void run() {
        isRun = true;
        String appName;
//        InetSocketAddress appAddress;
        for(Type type : typeList) {
//            appAddress = new InetSocketAddress(ChangeConfig.channel.gettIp(), Integer.parseInt(type.getChannelPort()));
//            ChangeConfig.targetAddresses.put(type.getTypeName(),appAddress);
            appName = type.getTypeName();
            Type exType = ChangeConfig.loadIChange(StaticField.ExternalConfig).getType(appName);
            TargetOperation targetOperation = new TargetOperation();
            targetOperation.init(type,exType);
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
