package com.hzih.itp.platform.filechange.target;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.filechange.target.plugin.ITargetProcess;
import com.hzih.itp.platform.filechange.target.plugin.TargetProcessFtp;
import com.hzih.itp.platform.filechange.target.plugin.TargetProcessSmb;
import com.hzih.itp.platform.filechange.target.servlet.FileReceiveServlet;
import com.hzih.itp.platform.filechange.utils.FileBean;
import com.hzih.itp.platform.filechange.utils.FileContext;
import com.hzih.itp.platform.utils.DataAttributes;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.itp.platform.utils.Status;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.TargetFile;
import com.inetec.common.config.stp.nodes.Type;
import com.inetec.common.exception.Ex;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by 钱晓盼 on 14-1-9.
 */
public class TargetOperation implements Runnable{
    final static Logger logger = LoggerFactory.getLogger(TargetOperation.class);

    private boolean isRun = false;
    private Type type;
    private String appName;
    private TargetFile config;
    private String charset;

    private Server server;
    private String host;
    private ITargetProcess targetProcess;

    public Type getType() {
        return type;
    }

    public void init(Type type,Type exType) {
        this.type  = type;
        this.appName = type.getTypeName();
        this.config = type.getPlugin().getTargetFile();
        this.charset = config.getCharset();

        server = new Server();
        server.setStopAtShutdown(true);
        Connector connector = new SelectChannelConnector();
        this.host = ChangeConfig.getChannel(exType.getChannel()).getLocalIp();
        connector.setHost(host);
        connector.setPort(Integer.parseInt(exType.getChannelPort()));
        server.setConnectors(new Connector[] { connector });
        Context context = new Context(Context.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new FileReceiveServlet(this,appName,config.getCharset())), StaticField.FileReceiveServlet);
    }

    @Override
    public void run() {

        if(TargetFile.Str_Protocol_Ftp.equals(config.getProtocol())) {
            targetProcess = new TargetProcessFtp();
        } else if(TargetFile.Str_Protocol_SMB.equals(config.getProtocol())) {
            targetProcess = new TargetProcessSmb();
        }
        targetProcess.init(this,config);
        new Thread(targetProcess).start();
        isRun = true;
        try {
            server.start();
            server.join();
            LogLayout.info(logger, appName, "应用启动[http://" + host +
                    ":" + type.getChannelPort() + StaticField.FileReceiveServlet + "]成功..");
        } catch (Exception e) {
            LogLayout.error(logger, appName, "启动应用[" + appName + "]servlet容器失败", e);
        }
    }

    public void stop() {
        try {
            server.stop();
        } catch (Exception e) {
            LogLayout.error(logger, appName, "关闭应用[" + appName + "]servlet容器失败", e);
        }
        isRun = false;
    }

    public boolean isRun() {
        return isRun;
    }

    public void process(String outFile) {
        try {
            send(new FileInputStream(outFile));
            new File(outFile).delete();
        } catch (IOException e) {
            LogLayout.error(logger, appName, "读取文件内容错误", e);
        }
    }

    private void send(InputStream is) throws IOException {
        DataAttributes data = new DataAttributes();
        data.load(is);
        DataAttributes result = new DataAttributes();
        String syncfilecommand = data.getValue(FileContext.Str_SyncFileCommand);
        if (syncfilecommand.equalsIgnoreCase(FileContext.Str_SyncFile)) {
            try {
                FileBean bean = getFileBean(data);
                String fullName = bean.getFullname();
                if (data.getResultData() != null) {
                    //byte[] temp = IOUtils.toByteArray(data.getResultData());
                    //m_LogLayout.info(logger,"platform","recv data length:" + temp.length);
                    boolean res = this.targetProcess.process(data.getResultData(), bean);
                    if (res)
                        result.setStatus(Status.S_Success);
                    else
                        result.setStatus(Status.S_Faild_TargetProcess);
    //                auditProcess(syncfilecommand, fullName, Long.parseLong(data.getValue(FileContext.Str_SyncFileDataSize)), res);
                } else {
                    LogLayout.warn(logger, appName, "targetProcessFtp process syncfile data is null:");
                    result.setStatus(Status.S_Faild_TargetProcess);
                }
            } catch (Ex ex) {
                LogLayout.error(logger,appName,"targetProcessFtp error",ex);
            }
        }
    }

    public FileBean getFileBean(DataAttributes da) throws Ex {
        FileBean result = new FileBean();
        result.setSyncflag(da.getValue(FileContext.Str_SyncFileFlag));
        result.setFullname(result.decode(da.getValue(FileContext.Str_SyncFileFullName)));
        result.setFilepostlocation(Long.parseLong(da.getValue(FileContext.Str_SyncFilePost)));
        result.setMd5(da.getValue(FileContext.Str_SyncFileMD5));
        result.setFilesize(Long.parseLong(da.getValue(FileContext.Str_SyncFileSize)));
        result.setSyncflag(da.getValue(FileContext.Str_SyncFileFlag));
        result.setFile_flag(da.getValue(FileContext.Str_SyncFile_Flag));
        result.setName(result.decode(da.getValue(FileContext.Str_SyncFileName)));
        return result;
    }
}
