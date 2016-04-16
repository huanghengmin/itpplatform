package com.hzih.itp.platform.stpchange.target;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.config.http.Client;
import com.hzih.itp.platform.config.http.HttpClientImpl;
import com.hzih.itp.platform.stpchange.target.plugin.ITargetProcess;
import com.hzih.itp.platform.stpchange.target.plugin.TargetProcessUdp;
import com.hzih.itp.platform.utils.StaticField;
import com.inetec.common.config.stp.nodes.SocketChange;
import com.inetec.common.config.stp.nodes.Type;
import com.sun.java_cup.internal.runtime.virtual_parse_stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.File;

/**
 * Created by 钱晓盼 on 14-1-13.
 */
public class TargetOperation implements Runnable{

    final static Logger logger = LoggerFactory.getLogger(TargetOperation.class);

    private boolean isRun = false;
    private Type type;
    private String appName;
    private String url;
    private SocketChange config;
    private String charset;
    private HttpClientImpl httpClient = null;
    private String fileFullName = null;

    public Type getType() {
        return type;
    }

    public String getUrl() {
        return url;
    }

    public void init(Type type) {
        this.type  = type;
        this.appName = type.getTypeName();
        this.config = type.getPlugin().getTargetSocket();
        this.charset = config.getCharset();
        this.url = "http://" + config.getServerAddress() + ":" + config.getPort() + StaticField.FileReceiveServlet;
    }

    @Override
    public void run() {
        isRun = true;
        ITargetProcess targetProcess = new TargetProcessUdp();
        targetProcess.init(this,config);
        new Thread(targetProcess).start();
        while (isRun) {
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
            }
        }
    }

    public boolean process(String filePath) {
        boolean isSuccess = false;
        fileFullName = filePath.substring((ChangeConfig.getRunShmPath()+"/"+appName).getBytes().length);
        httpClient = ChangeConfig.getHttpDataConnection(appName);
        String fileName = new String(new BASE64Encoder().encode(fileFullName.getBytes()));
        do{
            isSuccess = httpClient.changeFileByHttpClient(appName,url,charset,fileName,new File(filePath));
        } while (!isSuccess);

        ChangeConfig.returnHttpConnection(appName,httpClient);
        return isSuccess;
    }
}
