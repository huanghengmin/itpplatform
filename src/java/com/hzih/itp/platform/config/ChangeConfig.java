package com.hzih.itp.platform.config;

import com.hzih.itp.platform.config.http.HttpClientFactory;
import com.hzih.itp.platform.config.http.HttpClientImpl;
import com.hzih.itp.platform.config.http.HttpClientPool;
import com.hzih.itp.platform.config.mina.UdpClientFactory;
import com.hzih.itp.platform.config.mina.UdpClientImpl;
import com.hzih.itp.platform.config.mina.UdpClientPool;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.ConfigParser;
import com.inetec.common.config.stp.nodes.Channel;
import com.inetec.common.config.stp.nodes.IChange;
import com.inetec.common.config.stp.nodes.Type;
import com.inetec.common.db.stp.DataSourceFactory;
import com.inetec.common.db.stp.datasource.DatabaseSource;
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
 * 配置文件加载类
 */
public class ChangeConfig {

    final static Logger logger = LoggerFactory.getLogger(ChangeConfig.class);
    public static Map<String,InetSocketAddress> targetAddresses = new HashMap<String, InetSocketAddress>(); //所有单向到达目标端的通道集合
    public static IChange ichange;//
    public static Channel channel;//通道1的信息
    public static UdpClientPool udpClientPool = null;
    public static HttpClientPool httpClientPool = null;
    public static String backPath;//备份目录

    public static void loadChannelAddress() {
        try {
            ConfigParser configParser = new ConfigParser(StaticField.ExternalConfig);
            ichange = configParser.getRoot();
            channel = ichange.getChannel(StaticField.ChannelName);
            InetSocketAddress audit = new InetSocketAddress(channel.gettIp(), Integer.parseInt(channel.getAuditPort()));
            InetSocketAddress config = new InetSocketAddress(channel.gettIp(), Integer.parseInt(channel.gettPort()));
            targetAddresses.put(StaticField.AuditName,audit);
            targetAddresses.put(StaticField.ConfigName,config);
        } catch (Ex ex) {
            LogLayout.error(logger,"platform","加载通道出错",ex);
        }
    }

    public static Channel getChannel(String channelName) {
        return ichange.getChannel(channelName);
    }

    public static String getBackPath() {
        if(StaticField.SystemPath.indexOf(":")>-1) {
            return StaticField.SystemPath + "/data";
        } else {
            return "/data";
        }
    }

    public static String getRunShmPath() {
        if(StaticField.SystemPath.indexOf(":")>-1) {
            return StaticField.SystemPath + "/shm";
        } else {
            return "/run/shm";
        }
    }



    public static IChange loadIChange(String configPath) {
        try{
            ConfigParser configParser = new ConfigParser(configPath);
            return configParser.getRoot();
        }  catch (Ex ex) {
            LogLayout.error(logger,"platform","加载通道出错",ex);
        }
        return null;
    }


    /**
     * 按照应用类型加载应用配置
     * @param appType        应用类型
     * @param configPath    配置文件路径
     * @return               该应用类型的应用集合
     */
    public static List<Type> loadTypeList(String appType,String configPath) {
        List<Type> typeList = new ArrayList<Type>();
        try {
            ConfigParser configParser = new ConfigParser(configPath);
            IChange ichange = configParser.getRoot();
            Type[] types = ichange.getAllTypes();
            for (Type type : types) {
                if(appType.equals(type.getAppType())) {
                    if(type.isActive()){
                        typeList.add(type);
                    } else {
                        LogLayout.info(logger, "platform", "应用"+type.getTypeName()+"是关闭的..");
                    }
                }
            }
        } catch (Ex ex) {
            LogLayout.error(logger,"platform","加载"+appType+"类型应用出错",ex);
        }
        return typeList;
    }

    /**
     * 组织应用名查找集合
     * @param typeList
     * @return
     */
    public static Map<String,Type> loadTypeMap(List<Type> typeList) {
        Map<String,Type> typeMap = new HashMap<String, Type>();
        InetSocketAddress appAddress;
        for(Type type : typeList) {
            typeMap.put(type.getTypeName(),type);
        }
        return typeMap;
    }

    public static DatabaseSource findDataSource(String dbName) {
        return DataSourceFactory.findDataSource(dbName);
    }

    /**
     * 建立带key的对象池
     * @param appName
     */
    public static void addUdpClientPool(String appName) {
        UdpClientFactory factory = new UdpClientFactory();
        try {
            factory.makeObject(appName);
        } catch (Exception e) {
        }
        udpClientPool = new UdpClientPool(factory,10);
        udpClientPool.setFactory(factory);
        LogLayout.info(logger,"platform","初始化"+appName+"客户端的udp协议池");
    }

    public static UdpClientImpl getUdpControlConnection(String portName) {
        try {
            UdpClientImpl client =(UdpClientImpl) udpClientPool.borrowObject(portName);
            if(client == null){
                LogLayout.warn(logger,"platform","client is null...");
            }
            return client;
        } catch (Exception e) {
            LogLayout.error(logger,"platform","getControlConnection",e);
        }
        return null;
    }

    public static UdpClientImpl getUdpDataConnection(String appName) {
        try {
            return (UdpClientImpl) udpClientPool.borrowObject(appName);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","getDataConnection",e);
        }
        return null;
    }

    public static void returnUdpConnection(String appName,UdpClientImpl conn) {
        try {
            if(appName!=null&&conn!=null)
                udpClientPool.returnObject(appName,conn);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","returnConnection",e);
        }
    }

    /**
     * 建立带key的对象池
     * @param appName
     */
    public static void addHttpClientPool(String appName) {
        HttpClientFactory factory = new HttpClientFactory();
        try {
            factory.makeObject(appName);
        } catch (Exception e) {
        }
        httpClientPool = new HttpClientPool(factory,10);
        httpClientPool.setFactory(factory);
        LogLayout.info(logger,"platform","初始化"+appName+"客户端的http协议池");
    }

    public static HttpClientImpl getHttpControlConnection(String appName) {
        try {
            HttpClientImpl client =(HttpClientImpl) httpClientPool.borrowObject(appName);
            if(client == null){
                LogLayout.warn(logger,"platform","httpClient is null...");
            }
            return client;
        } catch (Exception e) {
            LogLayout.error(logger,"platform","getHttpControlConnection",e);
        }
        return null;
    }

    public static HttpClientImpl getHttpDataConnection(String appName) {
        HttpClientImpl httpClient = null;
        int count = 0;
        do{
            try {
                httpClient = (HttpClientImpl) httpClientPool.borrowObject(appName);
            } catch (Exception e) {
//            LogLayout.error(logger,"platform","getHttpDataConnection",e);
                addHttpClientPool(appName);
            }
            count ++ ;
            if(count>100) {
                LogLayout.warn(logger,"platform","建立httpClient对象错误");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        } while (httpClient == null);
        return httpClient;
    }

    public static void returnHttpConnection(String appName,HttpClientImpl conn) {
        try {
            if(appName!=null&&conn!=null)
                httpClientPool.returnObject(appName,conn);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","returnHttpConnection",e);
        }
    }


}
