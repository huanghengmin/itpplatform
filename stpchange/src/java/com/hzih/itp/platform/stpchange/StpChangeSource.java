package com.hzih.itp.platform.stpchange;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.stpchange.source.SourceOperation;
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
 * Created by 钱晓盼 on 14-1-10.
 */
public class StpChangeSource extends Thread {
    final static Logger logger = LoggerFactory.getLogger(StpChangeSource.class);

    private boolean isRun = false;
    private List<Type> typeList = new ArrayList<Type>();
    private Map<String,Type> typeMap = new HashMap<String,Type>();

    public void init() {
        typeList = ChangeConfig.loadTypeList(Type.s_app_httpproxy, StaticField.ExternalConfig);
        typeMap = ChangeConfig.loadTypeMap(typeList);
    }

    public void run() {
        isRun = true;
        String appName;
        InetSocketAddress appAddress;
        try{
            for(Type type : typeList) {
                appName = type.getTypeName();
                appAddress = new InetSocketAddress(ChangeConfig.channel.gettIp(), Integer.parseInt(type.getChannelPort()));
                ChangeConfig.targetAddresses.put(appName,appAddress);
                ChangeConfig.addUdpClientPool(appName);
                SourceOperation sourceOperation = new SourceOperation();
                sourceOperation.init(type);
                new Thread(sourceOperation).start();
                LogLayout.info(logger,"platform","应用"+appName+"启动...");
                LogLayout.info(logger, appName, "应用启动...");
            }
        } catch (Exception e) {
            LogLayout.error(logger,"启动应用失败",e);
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
