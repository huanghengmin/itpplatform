package com.hzih.itp.platform.dbchange.source;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.config.http.Client;
import com.hzih.itp.platform.config.http.HttpClientImpl;
import com.hzih.itp.platform.dbchange.DBChangeSource;
import com.hzih.itp.platform.dbchange.DbInit;
import com.hzih.itp.platform.dbchange.source.info.DatabaseInfo;
import com.hzih.itp.platform.dbchange.source.plugin.*;
import com.hzih.itp.platform.utils.DataAttributes;
import com.hzih.itp.platform.utils.SecurityUtils;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.itp.platform.utils.Status;
import com.hzih.logback.LogLayout;
import com.hzih.logback.utils.IOUtils;
import com.inetec.common.config.stp.nodes.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


/**
 * Created by 钱晓盼 on 14-1-14.
 */
public class SourceOperation implements Runnable {
    final static Logger logger = LoggerFactory.getLogger(SourceOperation.class);

    private boolean isRun = false;
    private Type type;
    private DatabaseInfo databaseInfo;
    private DBChangeSource dbChangeSource;
    private String charset;
    private String url;
    private String appName;
    private HttpClientImpl httpClient;

    public void init(DBChangeSource dbChangeSource, Type type, DatabaseInfo databaseInfo) {
        this.type = type;
        this.appName = type.getTypeName();
        this.databaseInfo = databaseInfo;
        this.dbChangeSource = dbChangeSource;
        this.charset = databaseInfo.getEncoding();
        this.url = "http://" +ChangeConfig.getChannel(type.getChannel()).getRemoteIp()
                + ":" + type.getChannelPort() +StaticField.FileReceiveServlet;
    }

    public DBChangeSource getDbChangeSource() {
        return dbChangeSource;
    }

    public Type getType() {
        return type;
    }

    @Override
    public void run() {
        isRun = true;
        ISourceProcess sourceProcess = null;
        if(databaseInfo.isTriggerEnable()) {
            sourceProcess = new SourceProcessTrigger();
            LogLayout.info(logger,appName,"加载触发同步应用...");
        } else if(databaseInfo.isDelete()) {
            sourceProcess = new SourceProcessDelete();
            LogLayout.info(logger,appName,"加载删除同步应用...");
        } else if(databaseInfo.isAllTableEnable()) {
            sourceProcess = new SourceProcessEntirely();
            LogLayout.info(logger,appName,"加载全表同步应用...");
        } else if(databaseInfo.isSpecifyFlag()) {
            sourceProcess = new SourceProcessFlag();
            LogLayout.info(logger,appName,"加载标记同步应用...");
        } else if(databaseInfo.isTimeSync()) {
            sourceProcess = new SourceProcessTimeSync();
            LogLayout.info(logger,appName,"加载时间标记同步应用...");
        }
        sourceProcess.init(this,databaseInfo);
        new Thread(sourceProcess).start();
        while (isRun) {
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
            }
        }
    }


    public DataAttributes process(InputStream is, DataAttributes props) {
        if (props == null) {
            props = new DataAttributes();
        }
        String appName = type.getTypeName();
        props.putValue(DataAttributes.Str_ChangeType, appName);
        DataAttributes fp = new DataAttributes();

        File file = null;
        String fn = props.getValue(DataAttributes.Str_FileName);
        if (fn == null || fn.length() == 0) {
            String day = formatDate(new Date(), "yyyy-MM-dd");
            int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
            String dir = ChangeConfig.getBackPath() + "/" + appName + "/" + day + "/" + hour;
            File dirFile = new File(dir);
            if(!dirFile.isDirectory()){
                dirFile.mkdirs();
            }
            file = makeTempFile(dir);
//            long nTime = System.currentTimeMillis();
//            fn = "temp_" + nTime;    // session id
            fn = file.getPath().split(appName)[1];
            props.putValue(DataAttributes.Str_FileName, fn);
        }
//        long s1 = System.currentTimeMillis();
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            LogLayout.error(logger,appName,"找不到写入的数据库备份文件", e);
        }
        int len = 0;
        byte[] buff = new byte[StaticField.MB * 2];
        int securityLevel = type.getInfoLevel();
        if(securityLevel>0) {
            String security = type.getSecurityFlag();
            InputStream in = SecurityUtils.encrypt(security, securityLevel, is);
            try{
                while((len = in.read(buff))!=-1){
//                    if(len < 1024 * 1024 * 2){
//                        buff = IOUtils.copyArray(buff, len);
//                    }
                    out.write(buff,0,len);
                }
                out.flush();
            } catch (IOException e) {
                LogLayout.error(logger,appName,"数据库文件备份出错",e);
            } finally {
                try {
                    out.close();
                    in.close();
                } catch (IOException e) {
                }
            }
        } else {
            try{
                while((len = is.read(buff))!=-1){
//                    if(len < 1024*1024 *2){
//                        buff = IOUtils.copyArray(buff, len);
//                    }
                    out.write(buff,0,len);
                }
                out.flush();
            } catch (IOException e) {
                LogLayout.error(logger, appName, "数据库文件备份出错", e);
            } finally {
                try {
                    out.close();
                } catch (IOException e) {
                }
            }
//            long l1 = System.currentTimeMillis() - s1;
//            LogLayout.info(logger,appName,"完成一次文件采集耗时"+l1+"豪秒");
            long s = System.currentTimeMillis();
            httpClient = ChangeConfig.getHttpDataConnection(appName);
            boolean isSuccess = false;
            do{
                String fileName = new String(new BASE64Encoder().encode(fn.getBytes()));
                isSuccess = httpClient.changeFileByHttpClient(appName,url,charset,fileName,file);
            } while (!isSuccess);
            ChangeConfig.returnHttpConnection(appName,httpClient);
            long l = System.currentTimeMillis() - s;
            LogLayout.info(logger,appName,"完成一次http传输文件耗时"+l+"豪秒,速度是"+ (file.length()/1024)/l  + "KB/ms");
        }
        try {
            is.close();
        } catch (IOException e) {
        }
        fp.setStatus(Status.S_Success_SendData);
        return fp;
    }

    /**
     * 返回当天的时间,例:2013-03-23
     * @return
     */
    private String formatDate(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    private File makeTempFile(String dir) {
        long l = System.currentTimeMillis();
        String fileName = dir +"/"+l+".tmp";
        return new File(fileName);
    }
}
