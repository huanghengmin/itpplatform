package com.hzih.itp.platform.servlet;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.dbchange.DBChangeSource;
import com.hzih.itp.platform.dbchange.DBChangeTarget;
import com.hzih.itp.platform.filechange.FileChangeSource;
import com.hzih.itp.platform.filechange.FileChangeTarget;
import com.hzih.itp.platform.stpchange.StpChangeSource;
import com.hzih.itp.platform.stpchange.StpChangeTarget;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.inetec.common.util.OSInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by 钱晓盼 on 14-1-10.
 */
public class Service extends HttpServlet {
    final static Logger logger = LoggerFactory.getLogger(Service.class);
    public static String platformType;

    public Service() {
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        response.setContentType("text/html");
        PrintWriter writer = response.getWriter();

        writer.println("<html>");
        writer.println("<head>");
        writer.println("<title>ITP Service Page</title>");
        writer.println("</head>");
        writer.println("<body bgcolor=white>");
        writer.println("<table border=\"0\">");
        writer.println("<tr>");
        writer.println("<td>");
        writer.println("<h1>ITP Service  Status Page</h1>");
        writer.println("<P>Service is running.<P><BR>");
        writer.println("</td>");
        writer.println("</tr>");
        writer.println("</table>");
        writer.println("</body>");
        writer.println("</html>");
    }


    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        String command = request.getParameter(StaticField.Command);

        /*if(StaticField.SendConfig.equals(command)) {
            Service.configService.query.offer(command);
        } else if(StaticField.AlertConfig.equals(command)) {

        } else if(StaticField.SendChannelTest.equals(command)) {

        }*/

        byte[] data = (command + " is ok!").getBytes();
        response.setContentLength(data.length);
        response.getOutputStream().write(data);

        response.flushBuffer();
        response.setStatus(HttpServletResponse.SC_OK);
    }

    public void init() {
//        OSInfo os = OSInfo.getOSInfo();
//        if(os.isLinux()) {
//            System.setProperty("logback.configurationFile",StaticField.Str_LogBackConfig);
//        } else {
//            System.setProperty("logback.configurationFile",StaticField.Str_LogBackConfig);
//        }
        String platformType = getPlatformType();         //主机名字
        ChangeConfig.loadChannelAddress();
        String platformName = null;
        if(StaticField.Platform_Type_EX_ITP.equals(platformType)) {
            platformName = StaticField.Platform_Name_EX_ITP;
            LogLayout.info(logger, "platform", "启动 ["+platformName+"] 开始...");
            LogLayout.info(logger,"platform","启动 [文件同步模块] 开始...");
            runFileChangeSource();
            LogLayout.info(logger,"platform","启动 [文件同步模块] 成功...");
            LogLayout.info(logger,"platform","启动 [数据库同步模块] 开始...");
            runDBChangeSource();
            LogLayout.info(logger,"platform","启动 [数据库同步模块] 成功...");
        } else if(StaticField.Platform_Type_EX_STP.equals(platformType)) {
            platformName = StaticField.Platform_Name_EX_STP;
            LogLayout.info(logger,"platform","启动 ["+platformName+"] 开始...");
            LogLayout.info(logger,"platform","启动 [UDP发送模块] 开始...");
            runStpChangeSource();
            LogLayout.info(logger,"platform","启动 [UDP发送模块] 成功...");
        } else if(StaticField.Platform_Type_IN_STP.equals(platformType)) {
            platformName = StaticField.Platform_Name_IN_STP;
            LogLayout.info(logger,"platform","启动 ["+platformName+"] 开始...");
            LogLayout.info(logger,"platform","启动 [UDP接收模块] 开始...");
            runStpChangeTarget();
            LogLayout.info(logger,"platform","启动 [UDP接收模块] 成功...");
        } else if(StaticField.Platform_Type_IN_ITP.equals(platformType)) {
            platformName = StaticField.Platform_Name_IN_ITP;
            LogLayout.info(logger,"platform","启动 ["+platformName+"] 开始...");
            LogLayout.info(logger,"platform","启动 [文件同步模块] 开始...");
            runFileChangeTarget();
            LogLayout.info(logger,"platform","启动 [文件同步模块] 成功...");
            LogLayout.info(logger,"platform","启动 [数据库同步模块] 开始...");
            runDBChangeTarget();
            LogLayout.info(logger,"platform","启动 [数据库同步模块] 成功...");
        }
    }

    private String getPlatformType() {
        OSInfo os = OSInfo.getOSInfo();
        if(os.isLinux()) {
            return os.getHostName();
        }
        return StaticField.Platform_Type_IN_STP;
    }

    private void runStpChangeSource() {
        if(Service.isStpChangeSource) {
            return;
        }
        Service.stpChangeSource.init();
        Service.stpChangeSource.start();
        Service.isStpChangeSource = true;
    }

    private void runStpChangeTarget() {
        if(Service.isStpChangeTarget){
            return;
        }
        Service.stpChangeTarget.init();
        Service.stpChangeTarget.start();
        Service.isStpChangeTarget = true;
    }

    private void runFileChangeSource() {
        if(Service.isFileChangeSource){
            return;
        }
        Service.fileChangeSource.init();
        Service.fileChangeSource.start();
        Service.isFileChangeSource = true;
    }

    private void runFileChangeTarget() {
        if(Service.isFileChangeTarget) {
            return;
        }
        Service.fileChangeTarget.init();
        Service.fileChangeTarget.start();
        Service.isFileChangeTarget = true;
    }

    private void runDBChangeSource() {
        if(Service.isDBChangeSource) {
            return;
        }
        Service.dbChangeSource.init();
        Service.dbChangeSource.start();
        Service.isDBChangeSource = true;
    }

    private void runDBChangeTarget() {
        if(Service.isDBChangeTarget) {
            return;
        }
        Service.dbChangeTarget.init();
        Service.dbChangeTarget.start();
        Service.isDBChangeTarget = true;
    }

    public static boolean isFileChangeSource = false;
    public static boolean isDBChangeSource = false;
    public static boolean isStpChangeSource = false;
    public static boolean isFileChangeTarget = false;
    public static boolean isDBChangeTarget = false;
    public static boolean isStpChangeTarget = false;
    public static FileChangeSource fileChangeSource = new FileChangeSource();
    public static FileChangeTarget fileChangeTarget = new FileChangeTarget();
    public static DBChangeSource dbChangeSource = new DBChangeSource();
    public static DBChangeTarget dbChangeTarget = new DBChangeTarget();
    public static StpChangeSource stpChangeSource = new StpChangeSource();
    public static StpChangeTarget stpChangeTarget = new StpChangeTarget();
}
