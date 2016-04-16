package com.hzih.itp.platform.config.mina;

import com.hzih.itp.platform.config.ChangeConfig;
import org.apache.commons.pool.KeyedPoolableObjectFactory;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: 钱晓盼
 * Date: 12-8-29
 * Time: 下午3:34
 * To change this template use File | Settings | File Templates.
 */
public class UdpClientFactory implements KeyedPoolableObjectFactory {

    public UdpClientFactory() {
        super();
    }

    @Override
    public Object makeObject(Object key) throws Exception {
        String appName = null;
        if(key instanceof String) {
            appName = (String) key;
            key = appName;
        }
        Map<String,InetSocketAddress> targets = ChangeConfig.targetAddresses;

        InetSocketAddress target = targets.get(appName);
        return new UdpClientImpl(appName,target);
    }

    @Override
    public void destroyObject(Object key, Object obj) throws Exception {
        if(key instanceof String) {
            String appName = (String) key;
            key = appName;
        }
        if (obj instanceof UdpClientImpl) {
            UdpClientImpl v = (UdpClientImpl) obj;
            obj = v;
            v.close();
        }
    }

    @Override
    public boolean validateObject(Object key, Object obj) {
        String appName = null;
        if(key instanceof String){
            appName = (String) key;
        }
        UdpClientImpl client = null;
        if (obj instanceof UdpClientImpl){
            client = (UdpClientImpl) obj;
        }

        if(appName.equals(client.getAppName())){
            return true;
        }
        return false;
        /*if (obj instanceof UdpClientImpl){
            return true;
        } else {
            return false;
        }*/
    }

    @Override
    public void activateObject(Object key, Object obj) throws Exception {
        if(key instanceof String) {
            String appName = (String) key;
            key = appName;
        }
        if (obj instanceof UdpClientImpl) {
            UdpClientImpl v = (UdpClientImpl) obj;
            obj = v;
        }

    }

    @Override
    public void passivateObject(Object key, Object obj) throws Exception {
        if(key instanceof String) {
            String appName = (String) key;
            key = appName;
        }
        if (obj instanceof UdpClientImpl) {
            UdpClientImpl v = (UdpClientImpl) obj;
            obj = v;
            v.close();
        }
    }

}
