package com.hzih.itp.platform.stpchange;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.stpchange.target.TargetOperation;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.Type;
import com.inetec.common.exception.Ex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by 钱晓盼 on 14-1-10.
 */
public class StpChangeTarget extends Thread{
    final static Logger logger = LoggerFactory.getLogger(StpChangeTarget.class);

    private boolean isRun = false;
    private List<Type> typeList = new ArrayList<Type>();
    private Map<String,Type> typeMap = new HashMap<String,Type>();

    public void init() {
        typeList = ChangeConfig.loadTypeList(Type.s_app_httpproxy, StaticField.InternalConfig);
        typeMap = ChangeConfig.loadTypeMap(typeList);
    }

    public void run() {
        isRun = true;
        String appName;
        InetSocketAddress appAddress;
        for(Type type : typeList) {
            appName = type.getTypeName();
            String channelName = ChangeConfig.loadIChange(StaticField.ExternalConfig).getType(appName).getChannel();
            String port = ChangeConfig.loadIChange(StaticField.ExternalConfig).getType(appName).getChannelPort();
            appAddress = new InetSocketAddress(ChangeConfig.getChannel(channelName).gettIp(), Integer.parseInt(port));
            ChangeConfig.targetAddresses.put(appName,appAddress);
            ChangeConfig.addHttpClientPool(appName);
            TargetOperation targetOperation = new TargetOperation();
            targetOperation.init(type);
            new Thread(targetOperation).start();
            LogLayout.info(logger,"platform","应用"+appName+"启动...");
            LogLayout.info(logger, appName, "应用"+appName+"启动...");
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
