package com.hzih.itp.platform.config.http;

import com.hzih.itp.platform.config.ChangeConfig;
import org.apache.commons.pool.KeyedPoolableObjectFactory;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Created by 钱晓盼 on 14-1-16.
 */
public class HttpClientFactory implements KeyedPoolableObjectFactory {

    public HttpClientFactory() {
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
        return new HttpClientImpl(appName,target.getPort());
    }

    @Override
    public void destroyObject(Object key, Object obj) throws Exception {
        if(key instanceof String) {
            String appName = (String) key;
            key = appName;
        }
        if (obj instanceof HttpClientImpl) {
            HttpClientImpl v = (HttpClientImpl) obj;
            obj = v;
        }
    }

    @Override
    public boolean validateObject(Object key, Object obj) {
        String appName = null;
        if(key instanceof String){
            appName = (String) key;
        }
        HttpClientImpl client = null;
        if (obj instanceof HttpClientImpl){
            client = (HttpClientImpl) obj;
        }

        if(appName.equals(client.getAppName())){
            return true;
        }
        return false;
    }

    @Override
    public void activateObject(Object key, Object obj) throws Exception {
        if(key instanceof String) {
            String appName = (String) key;
            key = appName;
        }
        if (obj instanceof HttpClientImpl) {
            HttpClientImpl v = (HttpClientImpl) obj;
            obj = v;
        }
    }

    @Override
    public void passivateObject(Object key, Object obj) throws Exception {
        if(key instanceof String) {
            String appName = (String) key;
            key = appName;
        }
        if (obj instanceof HttpClientImpl) {
            HttpClientImpl v = (HttpClientImpl) obj;
            obj = v;
        }
    }
}
