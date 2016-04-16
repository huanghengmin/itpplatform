package com.hzih.itp.platform.stpchange.source.plugin;

import com.hzih.itp.platform.stpchange.source.SourceOperation;
import com.hzih.itp.platform.stpchange.source.plugin.http.FileReceiveServlet;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.SocketChange;
import com.inetec.common.config.stp.nodes.Type;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Created by 钱晓盼 on 14-1-10.
 */
public class SourceProcessHttp implements ISourceProcess{
    final static Logger logger = LoggerFactory.getLogger(SourceProcessHttp.class);

    private SourceOperation source;
    private SocketChange config;
    private boolean isRun = false;
    private Type type;
    private String appName;
    private Server server;

    @Override
    public boolean process(InputStream in) {
        return false;
    }

    @Override
    public boolean process(byte[] data) {
        return false;
    }

    @Override
    public void init(SourceOperation source, SocketChange config) {
        this.source = source;
        this.type = source.getType();
        this.config = config;
        this.appName = type.getTypeName();
        server = new Server();
        server.setStopAtShutdown(true);
        Connector connector = new SelectChannelConnector();
        connector.setHost(config.getServerAddress());
        connector.setPort(Integer.parseInt(config.getPort()));
        server.setConnectors(new Connector[] { connector });
        Context context = new Context(Context.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new FileReceiveServlet(appName,config.getCharset())), StaticField.FileReceiveServlet);
    }

    @Override
    public void stop() {
        try {
            server.stop();
        } catch (Exception e) {
            LogLayout.error(logger,appName,"关闭应用["+appName+"]servlet容器失败",e);
        }
        isRun = false;
    }

    @Override
    public boolean isRun() {
        return isRun;
    }

    @Override
    public void run() {
        isRun = true;
        try {
            server.start();
            server.join();
            LogLayout.info(logger,appName,"应用启动[http://"+config.getServerAddress()+
                    ":"+config.getPort()+StaticField.FileReceiveServlet+"]成功..");
        } catch (Exception e) {
            LogLayout.error(logger,appName,"启动应用["+appName+"]servlet容器失败",e);
        } finally {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
            }
        }
    }
}
