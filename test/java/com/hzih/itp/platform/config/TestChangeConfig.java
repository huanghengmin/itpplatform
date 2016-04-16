package com.hzih.itp.platform.config;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.utils.StaticField;
import com.inetec.common.config.stp.nodes.Type;
import junit.framework.TestCase;

import java.util.List;

/**
 * Created by 钱晓盼 on 14-1-14.
 */
public class TestChangeConfig extends TestCase{

   public void testLoad() {
       ChangeConfig.loadChannelAddress();
       String ip = ChangeConfig.channel.gettIp();
       ChangeConfig.loadIChange(StaticField.ExternalConfig);
       ChangeConfig.getChannel(StaticField.ChannelName);
       ChangeConfig.getBackPath();
       List<Type> dbList = ChangeConfig.loadTypeList(Type.s_app_db, StaticField.ExternalConfig);
       List<Type> proxyList = ChangeConfig.loadTypeList(Type.s_app_proxy,StaticField.ExternalConfig);
       List<Type> httpProxyList = ChangeConfig.loadTypeList(Type.s_app_httpproxy,StaticField.ExternalConfig);
       System.out.println();
   }
}
