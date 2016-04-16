package com.hzih.itp.platform.filechange.source.plugin;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.filechange.source.SourceOperation;
import com.hzih.itp.platform.filechange.source.plugin.smb.SmbSendFileFilter;
import com.hzih.itp.platform.filechange.source.plugin.smb.SourceSmbUtils;
import com.hzih.itp.platform.filechange.utils.FileBean;
import com.hzih.itp.platform.filechange.utils.FileContext;
import com.hzih.itp.platform.filechange.utils.SmbUtils;
import com.hzih.itp.platform.filechange.utils.jdbc.SqliteUtil;
import com.hzih.itp.platform.filechange.utils.jdbc.TempFile;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.hzit.itp.platform.FileCheck;
import com.inetec.common.config.stp.nodes.SourceFile;
import com.inetec.common.config.stp.nodes.Type;
import com.inetec.common.io.IOUtils;
import jcifs.smb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.util.List;

/**
 * Created by 钱晓盼 on 14-1-9.
 */
public class SourceProcessSmb implements ISourceProcess {
    final static Logger logger = LoggerFactory.getLogger(SourceProcessSmb.class);

    private SourceOperation source;
    private SourceFile config;
    private Type type;
    private boolean isRun = false;
    private String appName;
    private long interval = 1;
    private String filePath;
    private SmbFile smbFile;
    public static FileCheck fileCheck;
    public static File checkFile ;


    @Override
    public boolean process(InputStream in, FileBean bean) {
        return false;
    }

    @Override
    public boolean process(byte[] data, FileBean bean) {
        boolean isOk = false;
        do{
            isOk = source.process(data,bean);
        } while (!isOk);
        return isOk;
    }

    @Override
    public void init(SourceOperation source, SourceFile config) {
        this.source = source;
        this.config = config;
        this.type = source.getType();
        this.appName = type.getTypeName();
        this.filePath = SourceSmbUtils.makeSmbFilePath(config);
        connect();
        fileCheck = new FileCheck();
        fileCheck.init();
        checkFile = getCheckFile();
    }

    @Override
    public void stop() {
        isRun = false;
    }

    @Override
    public boolean isRun() {
        return isRun;
    }


    private File getCheckFile(){
        File checkFile = null;
        File parent = new File(FileContext.FILECHECK + "/" + appName);
        if(parent.isDirectory()){
            File[] checkFiles = parent.listFiles();
            for(File file:checkFiles){
                if(file.getName().substring(file.getName().lastIndexOf(".") +1).equals("xsd")){
                    checkFile = file;
                    break;
                }
            }
        }
        return checkFile;
    }


    private void connect() {
        NtlmPasswordAuthentication nt = new NtlmPasswordAuthentication(
                "network", config.getUserName(), config.getPassword());
        try {
            smbFile = new SmbFile(filePath, nt);
        } catch (MalformedURLException e) {
            LogLayout.error(logger,appName,e.getMessage(),e);
        } catch (IOException e) {
            LogLayout.error(logger,appName,e.getMessage(),e);
        }

    }

    @Override
    public void run() {
        isRun = true;
        boolean isFirst = true;
        while (isRun) {
            try{
                smbFile.connect();
                boolean isRoot = true;
                LogLayout.info(logger, appName, "第" + (interval) + "个 文件共享 [源端]扫描文件列表周期开始...");
                if(isFirst){
                    reSend(TempFile.status_error); //断点续传
                    isFirst = false;
                }
                //没有断点续传的文件
                if(config.isIsincludesubdir()){
                    listDirectors(smbFile, isRoot);
                } else {
                    listFiles(smbFile);
                }
            } catch (Exception e) {
                LogLayout.error(logger, appName, e.getMessage(), e);
            } finally {
                LogLayout.info(logger,appName,"第"+ (interval) +"个 文件共享 [源端]扫描文件列表周期结束!等待"+config.getInterval()/1000+"秒...");
                try {
                    Thread.sleep(config.getInterval());
                } catch (InterruptedException e) {
                    logger.debug(e.getMessage());
                }
                interval ++;
            }
        }

    }

    /**
     *
     */
    private void listDirectors(SmbFile smbFile, boolean isRoot) {
        listFiles(smbFile);
        SmbFileFilter smbFileFilter = new SmbFileFilter() {
            public boolean accept(SmbFile smbFile) throws SmbException {
                if(smbFile.isDirectory()){
                    return true;
                }
                return false;
            }
        };
        try {
            SmbFile[] smbFiles = smbFile.listFiles(smbFileFilter);
            if(!isRoot && smbFiles.length==0){
                deleteDir(smbFile);
            }
            for (SmbFile sf : smbFiles) {
                isRoot = false;
                LogLayout.info(logger, appName, sf.getPath());
                listDirectors(sf, isRoot);
            }
        } catch (SmbException e) {
            LogLayout.error(logger,appName,e.getMessage(),e);
        }
    }

    /**
     *
     * @param smbFile
     */
    private void listFiles(SmbFile smbFile) {
        SmbFileFilter smbFileFilter = new SmbSendFileFilter(config);
        try {
            SmbFile[] smbFiles = smbFile.listFiles(smbFileFilter);
            for (SmbFile sf : smbFiles) {
                sendProcess(sf, TempFile.status_ok);
                reSend(TempFile.status_resend);//重传
            }
        } catch (SmbException e) {
            LogLayout.error(logger,appName,e.getMessage(),e);
        }

    }

    private void sendProcess(SmbFile smbFile, String status) {
        String url = smbFile.getURL().toString();
        String fileName = getFileNameFromUrl(url);
        long smbFileSize = 0;
        try {
            smbFileSize = smbFile.length();
        } catch (SmbException e) {
            LogLayout.error(logger,appName,e.getMessage(),e);
        }
        long lastModified = smbFile.getLastModified();
        boolean isSaved = checkFileIsSaved(appName,fileName,lastModified,smbFileSize);
        if(isSaved) {
            if(config.isDeletefile()){
                deleteSmbFile(smbFile);
                SqliteUtil.delete(appName, fileName);
            } else {
                TempFile tempFile = SqliteUtil.query(appName,fileName);
                if(tempFile!=null && !TempFile.status_ok.equals(tempFile.getStatus())) {
                    tempFile.setStatus(TempFile.status_ok);
                    SqliteUtil.update(tempFile);
                } else if(tempFile == null) {
                    tempFile = new TempFile();
                    tempFile.setAppName(appName);
                    tempFile.setFileFullName(fileName);
                    tempFile.setFileSize(smbFileSize);
                    tempFile.setLastModified(lastModified);
                    tempFile.setStatus(TempFile.status_ok);
                    SqliteUtil.insert(tempFile);
                }
            }
            return;
        }
        //记录到索引数据库

        long s = System.currentTimeMillis();
        FileBean fileBean = new FileBean();
        fileBean.setName(fileName.substring(fileName.lastIndexOf("/")));
        fileBean.setFullname(fileName);
        fileBean.setFilesize(smbFileSize);
        fileBean.setTime(lastModified);

        TempFile tempFile = SqliteUtil.query(appName,fileName);
        if(tempFile != null) {
            tempFile.setFileSize(smbFileSize);
            tempFile.setLastModified(lastModified);
            tempFile.setStatus(status);
            SqliteUtil.update(tempFile);
        } else {
            tempFile = new TempFile();
            tempFile.setAppName(appName);
            tempFile.setFileFullName(fileName);
            tempFile.setFileSize(smbFileSize);
            tempFile.setLastModified(lastModified);
            tempFile.setStatus(status);
            SqliteUtil.insert(tempFile);
        }
        LogLayout.info(logger, appName, "开始文件[" + fileName + "]的同步");
        long l = System.currentTimeMillis() - s ;
//            LogLayout.info(logger,appName,"完成一次 数据库查询和修改或新增 耗时"+l+"毫秒+++++++++++++++++++");
        s = System.currentTimeMillis();
        InputStream in = SmbUtils.getConnectSmbFileInputStream(smbFile, appName);
        downAndSend(in,fileName,fileBean);
        l = System.currentTimeMillis() - s ;
//            LogLayout.info(logger,appName,"完成一次 下载备份发送 耗时"+l+"毫秒*******************");
        if(config.isDeletefile()){
            s = System.currentTimeMillis();
            deleteSmbFile(smbFile);
            l = System.currentTimeMillis() - s ;
//                LogLayout.info(logger,appName,"完成一次 删除ftp文件 耗时"+l+"毫秒===============");
            s = System.currentTimeMillis();
            SqliteUtil.delete(appName,fileName);
            l = System.currentTimeMillis() - s ;
//                LogLayout.info(logger,appName,"完成一次 删除数据库 耗时"+l+"毫秒------------------");
        } else {
            tempFile = SqliteUtil.query(appName,fileName);
            tempFile.setStatus(TempFile.status_ok);
            SqliteUtil.update(tempFile);
        }
        LogLayout.info(logger,appName,"完成文件["+fileName+"]的同步");
    }

    private String getFileNameFromUrl(String url) {
        if(url.indexOf("?") > -1) {
            url = url.substring(0,url.indexOf("?"));
        }
        return url.substring(this.filePath.lastIndexOf("/"));
    }

    private void downAndSend(InputStream in,String localFileName, FileBean fileBean) {
        File tempFile = new File(ChangeConfig.getBackPath() + "/" + appName + localFileName);
        if(!tempFile.getParentFile().exists()) {
            tempFile.getParentFile().mkdirs();
        }
        OutputStream out = null;
        long skipLen = 0;
        int packetSize = StaticField.MB * config.getPacketsize();
        byte[] buf = new byte[packetSize];
        int len = 0;
        int allSize = 0;
        long fileSize = fileBean.getFilesize();
        fileBean.setFile_flag(FileContext.Str_SourceFile);
        try{
            if(tempFile.exists()) {
                skipLen = tempFile.length();
                fileBean.setFilepostlocation(skipLen);
                out = new FileOutputStream(tempFile,true);
                in.skip(skipLen);
            } else {
                fileBean.setFilepostlocation(skipLen);
                out = new FileOutputStream(tempFile);
            }
        } catch (IOException e) {
            LogLayout.error(logger,appName,"I/O错误",e);
        }
        try {
            boolean isFirst = true;
            while((len = in.read(buf))!= -1){
                buf = IOUtils.copyArray(buf, len);
                allSize += len;
                if(isFirst) {
                    if(fileSize==(long)allSize){
                        fileBean.setSyncflag(FileContext.Str_SyncFileFlag);
                    } else {
                        fileBean.setSyncflag(FileContext.Str_SyncFileStart);
                    }
                    isFirst = false;
                } else {
                    if(fileSize==(long)allSize) {
                        fileBean.setSyncflag(FileContext.Str_SyncFileEnd);
                    } else {
                        fileBean.setSyncflag(FileContext.Str_SyncFileIng);
                    }
                }
                process(buf, fileBean);
                out.write(buf);
            }
            out.flush();
        } catch (Exception e) {
            LogLayout.error(logger,appName,"发送保存文件读取失败",e);
        } finally {
            try {
                out.close();
            } catch (IOException e) {
            }
            if(in!=null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * 检查是否有需要重传的文件
     */
    private void reSend(String status) {
        List<String> fileNameList = SqliteUtil.queryForResend(appName, status);
        SmbFile smbFile = null;
        for(String fileName : fileNameList) {
            try {
                smbFile = SourceSmbUtils.makeSmbUrl(config, fileName);
            } catch (MalformedURLException e) {
                LogLayout.error(logger,appName,e.getMessage(),e);
            }
            sendProcess(smbFile,status);
        }
    }

    private void deleteDir(SmbFile smbFile) {
        if(config.isDeletefile()) {
            try {
                String[] smbFiles = smbFile.list();
                if(smbFiles.length == 0) {
                    smbFile.delete();
                    LogLayout.info(logger,appName,"删除空文件夹["+smbFile.getPath()+"]");
                }
            } catch (SmbException e) {
                LogLayout.error(logger,appName,e.getMessage(),e);
            }
        }
    }

    private void deleteSmbFile(SmbFile smbFile) {
        try {
            String path = smbFile.getPath();
            smbFile.delete();
            LogLayout.info(logger,appName,"完成文件["+path+"]的删除");
        } catch (SmbException e) {
            LogLayout.error(logger,appName,e.getMessage(),e);
        }
    }

    /**
     * 校验本地是否已有本分文件
     * @param appName     应用名
     * @param fileName    文件名
     * @param lastModified
     * @param smbFileSize   文件大小
     * @return
     */
    private boolean checkFileIsSaved(String appName,
                                     String fileName, long lastModified, long smbFileSize) {
        File file = new File(ChangeConfig.getBackPath() + "/" + appName + fileName);
        if(file == null) {
            return false;
        }
        if(file.exists()
                && file.length() == smbFileSize) {
            return true;
        }
        return false;
    }


}
