package com.hzih.itp.platform.config.http;

import com.hzih.itp.platform.utils.StaticField;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/**
 * Created by 钱晓盼 on 14-1-16.
 */
public class ServerThread implements Runnable{
    private int port;

    public ServerThread( int port) {
        this.port = port;
    }

    @Override
    public void run() {
        Server server = new Server();
            server.setStopAtShutdown(true);
            Connector connector = new SelectChannelConnector();
            connector.setHost("127.0.0.1");
            connector.setPort(port);
            server.setConnectors(new Connector[] { connector });
            Context context = new Context(Context.SESSIONS);
            context.setContextPath("/");
            server.setHandler(context);
            context.addServlet(new ServletHolder(new FileReceiveServlet("test"+port,"utf-8")), StaticField.FileReceiveServlet);
            try {
                server.start();
                server.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
            }
        }
    }
}
