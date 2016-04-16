package com.hzih.itp.platform.dbchange.target;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.dbchange.source.info.DatabaseInfo;
import com.hzih.itp.platform.dbchange.target.plugin.*;
import com.hzih.itp.platform.dbchange.target.servlet.FileReceiveServlet;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.*;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 钱晓盼 on 14-1-14.
 */
public class TargetOperation implements Runnable {
    final static Logger logger = LoggerFactory.getLogger(TargetOperation.class);

    private boolean isRun = false;
    private Type type;
    private Type exType;
    private String appName;
    private Server server;
    private String charset;
    private String host;
    private ITargetProcess targetProcess = null;

    private DatabaseInfo databaseInfo;

    public Type getType() {
        return type;
    }

    public Type getExType() {
        return exType;
    }

    public void init(Type type, DatabaseInfo databaseInfo, Type exIChangeType) {
        this.type = type;
        this.appName = type.getTypeName();
        this.exType = exIChangeType;
        this.databaseInfo = databaseInfo;
        this.charset = "utf-8";
        this.server = new Server();
        this.server.setStopAtShutdown(true);
        Connector connector = new SelectChannelConnector();
        this.host = ChangeConfig.getChannel(this.exType.getChannel()).getLocalIp();
        connector.setHost(this.host);
        connector.setPort(Integer.parseInt(this.exType.getChannelPort()));
        this.server.setConnectors(new Connector[] { connector });
        Context context = new Context(Context.SESSIONS);
        context.setContextPath("/");
        this.server.setHandler(context);
        context.addServlet(new ServletHolder(new FileReceiveServlet(this,appName,charset)), StaticField.FileReceiveServlet);
    }

    @Override
    public void run() {
        if(this.databaseInfo.isTriggerEnable()) {
            this.targetProcess = new TargetProcessTrigger();
        } else if(this.databaseInfo.isDelete()) {
            this.targetProcess = new TargetProcessDelete();
        } else if(this.databaseInfo.isAllTableEnable()) {
            this.targetProcess = new TargetProcessEntirely();
        } else if(this.databaseInfo.isSpecifyFlag()) {
            this.targetProcess = new TargetProcessFlag();
        } else if(this.databaseInfo.isTimeSync()) {
            this.targetProcess = new TargetProcessTimeSync();
        }
        this.targetProcess.init(this,databaseInfo);
        new Thread(this.targetProcess).start();
        isRun = true;
        try {
            this.server.start();
            this.server.join();
            LogLayout.info(logger, appName, "应用启动[http://" + host +
                    ":" + type.getChannelPort() + StaticField.FileReceiveServlet + "]成功..");
        } catch (Exception e) {
            LogLayout.error(logger, appName, "启动应用[" + appName + "]servlet容器失败", e);
        }
    }

    public boolean isRunning() {
        return isRun;
    }

    public void close() {
        try {
            this.server.stop();
        } catch (Exception e) {
            LogLayout.error(logger, appName, "关闭应用[" + appName + "]servlet容器失败", e);
        }
        isRun = false;
    }

    public boolean process(String tempFileName) {
        this.targetProcess.process(tempFileName);
        return true;
    }
    


}
