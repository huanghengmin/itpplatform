package com.hzih.itp.platform.filechange.source;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.config.http.HttpClientImpl;
import com.hzih.itp.platform.filechange.source.plugin.ISourceProcess;
import com.hzih.itp.platform.filechange.source.plugin.SourceProcessFtp;
import com.hzih.itp.platform.filechange.source.plugin.SourceProcessSmb;
import com.hzih.itp.platform.filechange.utils.FileBean;
import com.hzih.itp.platform.filechange.utils.FileContext;
import com.hzih.itp.platform.utils.DataAttributes;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.itp.platform.utils.Status;
import com.hzih.logback.LogLayout;
import com.hzih.logback.utils.IOUtils;
import com.inetec.common.config.stp.nodes.SourceFile;
import com.inetec.common.config.stp.nodes.Type;
import com.inetec.common.exception.Ex;
import com.inetec.common.io.ByteArrayBufferedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

/**
 * Created by 钱晓盼 on 14-1-9.
 */
public class SourceOperation implements Runnable{
    final static Logger logger = LoggerFactory.getLogger(SourceOperation.class);

    private boolean isRun = false;
    private Type type;
    private String appName;
    private SourceFile config;
    private String charset;
    private String url;
    private HttpClientImpl httpClient;

    public void init(Type type) {
        this.type  = type;
        this.appName = type.getTypeName();
        this.config = type.getPlugin().getSourceFile();
        this.charset = config.getCharset();
        this.url = "http://" + ChangeConfig.getChannel(type.getChannel()).getRemoteIp()
                + ":" + type.getChannelPort() + StaticField.FileReceiveServlet;

//        ChangeConfig.addClientPool(appName);

    }

    public Type getType() {
        return type;
    }

    @Override
    public void run() {

        ISourceProcess sourceProcess = null;
        if(SourceFile.Str_Protocol_Ftp.equals(config.getProtocol())) {       //ftp
            sourceProcess = new SourceProcessFtp();
        } else if(SourceFile.Str_Protocol_SMB.equals(config.getProtocol())) {           //smb
            sourceProcess = new SourceProcessSmb();
        }
        sourceProcess.init(this,config);
        new Thread(sourceProcess).start();
        isRun = true;
        while (isRun) {
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
            }
        }
    }

    public boolean process(byte[] data, FileBean bean) {
        if (data == null && data.length == 0) {
            LogLayout.warn(logger, appName,
                    "Process data is null or data length equel 0 .filebean filename:"
                            +bean.getName()+" for Type name:"+appName);
            return false;
        }
        boolean result = false;
        String fullName = bean.getFullname();
        DataAttributes da = new DataAttributes();
        da.setProperty(FileContext.Str_SyncFileCommand, FileContext.Str_SyncFile);
        da.setProperty(FileContext.Str_SyncFileFlag, bean.getSyncflag());
        da.setProperty(FileContext.Str_SyncFileName, bean.encode(bean.getName()));
        da.setProperty(FileContext.Str_SyncFilePost, "" + bean.getFilepostlocation());
        da.setProperty(FileContext.Str_SyncFileMD5, "" + bean.getMd5());
        da.setProperty(FileContext.Str_SyncFileSize, "" + bean.getFilesize());
        da.setProperty(FileContext.Str_SyncFileFullName, bean.encode(fullName));
        da.setProperty(FileContext.Str_SyncFile_Flag, bean.getFile_flag());
        da.setProperty(FileContext.Str_SyncFileDataSize, "" + data.length);
        da.setResultData(data);
        try {
            DataAttributes res = commandProcessIsReturn(da,bean);
            result = res.getStatus().isSuccess();
            auditProcess(bean,res.getStatus().isSuccess());
        } catch (Exception e) {
            LogLayout.error(logger, appName, "File change type:" + appName + " process file sync data(byte[]) error.", e);
        }
        return result;
    }

    /**
     * 审计处理 入库 发送到下一级 发送到第三方
     * @param bean
     * @param isSuccess
     */
    private void auditProcess(FileBean bean, boolean isSuccess) {

    }

    public DataAttributes commandProcessIsReturn(DataAttributes buff, FileBean bean) throws Ex {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayBufferedInputStream in = new ByteArrayBufferedInputStream();
        try {
            buff.store(out, "");
            in.write(out.toByteArray());
            in.flush();
            buff = disposeData(in,bean);
        } catch (Exception e) {
            LogLayout.error(logger,appName,"发送文件错误",e);
        }
        return buff;
    }

    private DataAttributes disposeData(ByteArrayBufferedInputStream in, FileBean bean) throws Ex {
        DataAttributes result = null;
        DataAttributes props = new DataAttributes();
        props.putValue(DataAttributes.Str_FileSize, String.valueOf(in.getSize()));
        result = dispose(in, props,bean);
        if (!result.getStatus().isSuccess()) {
            LogLayout.warn(logger,appName,"发送到目标端处理出错");
            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,appName,"process status Value:" + result.getStatus().isSuccess());
            }
        }
        return result;
    }

    private DataAttributes dispose(InputStream in, DataAttributes props, FileBean bean) {
        byte[] buff = IOUtils.toByteArray(in);
        long s = System.currentTimeMillis();
        httpClient = ChangeConfig.getHttpDataConnection(appName);
        boolean isSuccess = false;
        do{
            String fileName = new String(new BASE64Encoder().encode(bean.getName().getBytes()));
            isSuccess = httpClient.changeFileByHttpClient(appName,url,charset,fileName,buff);
        } while (!isSuccess);
        ChangeConfig.returnHttpConnection(appName,httpClient);
        long l = System.currentTimeMillis() - s;
//        LogLayout.info(logger,appName,"完成一次http传输文件耗时"+l+"豪秒,速度是"+ (buff.length/1024)/l  + "KB/ms");
        props.setStatus(Status.S_Success);
        return props;
    }
}
