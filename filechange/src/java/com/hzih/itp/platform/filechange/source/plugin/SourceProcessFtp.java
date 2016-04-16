package com.hzih.itp.platform.filechange.source.plugin;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.filechange.source.SourceOperation;
import com.hzih.itp.platform.filechange.source.plugin.ftp.FTPSendFileFilter;
import com.hzih.itp.platform.filechange.utils.*;
import com.hzih.itp.platform.filechange.utils.jdbc.SqliteUtil;
import com.hzih.itp.platform.filechange.utils.jdbc.TempFile;
import com.hzih.itp.platform.utils.SecurityUtils;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.hzih.logback.utils.IOUtils;
import com.hzit.itp.platform.FileCheck;
import com.hzit.itp.platform.getmd5.FileDigest;
import com.inetec.common.config.stp.nodes.SourceFile;
import com.inetec.common.config.stp.nodes.Type;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created by 钱晓盼 on 14-1-9.
 */
public class SourceProcessFtp implements ISourceProcess {
    final static Logger logger = LoggerFactory.getLogger(SourceProcessFtp.class);
    private SourceOperation source;
    private SourceFile config;
    private Type type;
    private boolean isRun = false;
    private FTPClient client;
    private String appName;
    private long interval = 1;
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

    @Override
    public void run() {
        isRun = true;
        String parent;
        boolean isFirst = true;
        while (isRun) {
            try{
                LogLayout.info(logger,appName,"第"+ (interval) +"个 FTP [源端]扫描文件列表周期开始...");
                parent = config.getDir();
                connect();
                if(isFirst){
                    reSend(TempFile.status_error); //断点续传
                    isFirst = false;
                }
                disconnect();
                connect();
                if(config.isIsincludesubdir()){
                    listDirectors(parent);//没有断点续传的文件
                } else {
                    listFiles(parent);
                }
                disconnect();
            } catch (Exception e) {
                LogLayout.error(logger,appName,e.getMessage(),e);
            } finally {
                LogLayout.info(logger,appName,"第"+ (interval) +"个 FTP [源端]扫描文件列表周期结束!等待"+config.getInterval()/1000+"秒...");
                try {
                    Thread.sleep(config.getInterval());
                } catch (InterruptedException e) {
                    logger.debug(e.getMessage());
                }
                interval ++;
            }
        }
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


    /**
     *
     * @param parent
     *
     */
    private void listDirectors(String parent) {
        listFiles(parent);
        FTPFileFilter fileFilter = new FTPFileFilter() {
            public boolean accept(FTPFile ftpFile) {
                if(ftpFile.isDirectory()){
                    return true;
                }
                return false;
            }
        };
        FTPFile[] dirs = new FTPFile[0];
        boolean isSuccess = false;
        do{
            try {
                dirs = client.listFiles(new String(parent.getBytes(config.getCharset()),"iso-8859-1"),fileFilter);
                isSuccess = true;
            } catch (Exception e) {
                LogLayout.error(logger,appName,"获取文件夹列表错误",e);
                disconnect();
            } finally {
                connect();
            }
        } while (!isSuccess);        //todo 死循环?
        int idx = 0;//无用的文件夹           //todo can't understand
        for(int i = 0; i < dirs.length;i++) {
            if(idx==2&&dirs.length==2) {
                return;
            }
            String dirName = dirs[i].getName();
            if (".".equals(dirName)||"..".equals(dirName)){
                idx ++;
                continue;
            }
            String _parent = null;
            if(parent.endsWith("/")){
                _parent = parent + dirName;
            } else {
                _parent = parent + "/" +dirName;
            }
            listDirectors(_parent);
        }
    }

    private void listFiles(String parent) {
        FTPFile[] files = new FTPFile[0];
        FTPSendFileFilter ftpSendFileFilter = new FTPSendFileFilter(config);
        boolean isSuccess = false;
        do{
            try {
                files = client.listFiles(new String(parent.getBytes(config.getCharset()),"iso-8859-1"),ftpSendFileFilter);
                isSuccess = true;
            } catch (Exception e) {
                LogLayout.error(logger, appName, "获取文件列表错误", e);
                disconnect();
            } finally {
                connect();
            }
        } while (!isSuccess);
        addToQuery(files,parent);
        if(config.isDeletefile()){
            deleteDir(parent);
        }
    }

    private void addToQuery(FTPFile[] files,String parent) {
        for (int i = 0; i < files.length; i ++ ) {
            if(files[i]==null){
                continue;
            }
            if(files[i].isDirectory()){
                continue;
            }
            String fileName = files[i].getName();
            if(parent.endsWith("/")){
                fileName = parent + fileName;
            } else {
                fileName = parent+ "/" +fileName;
            }
            long s = System.currentTimeMillis();
            sendProcess(files[i],fileName,TempFile.status_ok);
            long l = System.currentTimeMillis() -s ;
//            LogLayout.info(logger,appName,"完成一次下载备份发送耗时"+l+"毫秒");
            reSend(TempFile.status_resend);//重传
        }
    }

    /**
     * 发送文件
     * @param ftpFile
     * @param fileName
     * @param status   发送类型;
     */
    private void sendProcess(FTPFile ftpFile, String fileName,String status) {

        if(!".".equals(fileName)||!"..".equals(fileName)) {
            long lastModified = 0;
            long ftpFileSize = 0;
            try {
                lastModified = ftpFile.getTimestamp().getTimeInMillis();
                ftpFileSize = ftpFile.getSize();
            } catch (Exception e) {
                String rawListing = ftpFile.getRawListing();
                String[] atris = rawListing.split(";");
                String ftpName = atris[3].trim();
                String dateStr = atris[2].split("=")[1].split("\\.")[0];
                String fffStr = atris[2].split("=")[1].split("\\.")[1];
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                try {
                    lastModified = sdf.parse(dateStr).getTime() + Long.parseLong(fffStr);
                } catch (ParseException e1) {
                }
                ftpFileSize = Long.parseLong(atris[1].split("=")[1]);
            }

            boolean isSaved = checkFileIsSaved(appName,fileName,lastModified,ftpFileSize);
            if(isSaved) { //文件已下载
                if(config.isDeletefile()){
                    deleteFTPFile(fileName);
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
                        tempFile.setFileSize(ftpFileSize);
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
            fileBean.setFilesize(ftpFileSize);
            fileBean.setTime(lastModified);


            TempFile tempFile = SqliteUtil.query(appName,fileName);
            if(tempFile != null) {
                tempFile.setFileSize(ftpFileSize);
                tempFile.setLastModified(lastModified);
                tempFile.setStatus(status);
                SqliteUtil.update(tempFile);
            } else {
                tempFile = new TempFile();
                tempFile.setAppName(appName);
                tempFile.setFileFullName(fileName);
                tempFile.setFileSize(ftpFileSize);
                tempFile.setLastModified(lastModified);
                tempFile.setStatus(status);
                SqliteUtil.insert(tempFile);
            }
            LogLayout.info(logger, appName, "开始文件[" + fileName + "]的同步");
            long l = System.currentTimeMillis() - s ;
//            LogLayout.info(logger,appName,"完成一次 数据库查询和修改或新增 耗时"+l+"毫秒+++++++++++++++++++");
            s = System.currentTimeMillis();
            downAndSend(fileName, fileBean);
            l = System.currentTimeMillis() - s ;
//            LogLayout.info(logger,appName,"完成一次 下载备份发送 耗时"+l+"毫秒*******************");
            if(config.isDeletefile()){
                s = System.currentTimeMillis();
                deleteFTPFile(fileName);
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
    }

    private void downAndSend(String fileName, FileBean fileBean) {
        InputStream in = null;
        boolean checkResult = true;
        do{
            try {
                in = client.retrieveFileStream(new String(fileName.getBytes(config.getCharset()),"iso-8859-1"));
                if(type.isFilter()) { // isfilter
                    //校验
                    String result = SourceProcessFtp.fileCheck.checkFile(in,checkFile.getAbsolutePath());
                    if(!result.equals("success")){
                        checkResult = false;
                        LogLayout.warn(logger, appName, fileName + "校验不通过");
                    }
                    //非法文字过滤
                    /*if(fileName.endsWith(".txt") ||fileName.endsWith(".rtf")
                            ||fileName.endsWith(".doc") || fileName.endsWith(".wps") || fileName.endsWith(".docx")
                            ) {
                        KeywordsFilterUtil keywordsFilterUtil = KeywordsFilterFactory.getKeywordsFilterUtil(fileName);
                        String keyWords = Basic.getKeywords();
                        try {
                            in = keywordsFilterUtil.filter(in,keyWords,config);
                        } catch (Exception e) {
                            LogLayout.error(logger,appName,"过滤错误",e);
                        }
                    } else {
                        LogLayout.warn(logger,appName,"[FTP]文件"+fileName+"不是可过滤类型文件,跳过过滤");
                    }*/
                }
                //读取MD5值
                String md5 = FileDigest.getFileMD5(in);
                fileBean.setMd5(md5);
                if(checkResult){
                    int securityLevel = type.getInfoLevel();
                    if(securityLevel>0) {
                        String security = type.getSecurityFlag();
                        in = SecurityUtils.encrypt(security, securityLevel, in);     //ftp上的文件
                    }
                    process(in,fileName,fileBean);
//                down(in,fileName,fileBean);

                }
            } catch (IOException e) {
                LogLayout.error(logger,appName,"获取ftp中被下载文件流出错",e);
                disconnect();
            } finally {
                connect();
            }
        } while (in == null);
        try{
            in.close();
        } catch (Exception e) {
        }
    }

    /**
     * 检查是否有需要重传的文件
     */
    private void reSend(String status) {
        List<String> fileNameList = SqliteUtil.queryForResend(appName, status);
        for(String fileName : fileNameList) {
            boolean isSuccess = false;
            FTPFile ftpFile = null;
            do{
                try {
                    ftpFile = client.mlistFile(new String(fileName.getBytes(config.getCharset()),"iso-8859-1"));
                    isSuccess = true;
                } catch (IOException e) {
                    LogLayout.error(logger,appName,"查找文件错误",e);
                    disconnect();
                } finally {
                    connect();
                }
            } while (!isSuccess);
            if(ftpFile == null){
                LogLayout.info(logger,appName,"远程服务器上文件["+fileName+"]不存在");
                continue;
            }
            sendProcess(ftpFile,fileName,status);
        }
    }


    /**
     * 保存下载文件
     * @param in    ftp输入流
     * @param localFileName     最终保存文件名
     * @return
     */
    private File process(InputStream in, String localFileName,FileBean fileBean) {
        File tempFile = new File(ChangeConfig.getBackPath() + "/" + appName + localFileName);     //存放在本地的文件路径
        if(!tempFile.getParentFile().exists()) {
            tempFile.getParentFile().mkdirs();
        }
        OutputStream fout = null;
        long skipLen = 0;
        int packetSize = StaticField.MB * config.getPacketsize();
        byte[] buf = new byte[packetSize];
        int len = 0;
        int allSize = 0;
        long fileSize = fileBean.getFilesize();
        fileBean.setFile_flag(FileContext.Str_SourceFile);
        try{
            if(tempFile.exists()) {     //文件已存在,从最后开始传流,in也设置到相应长度
                skipLen = tempFile.length();
                fileBean.setFilepostlocation(skipLen);
                fout = new FileOutputStream(tempFile,true);
                in.skip(skipLen);
            } else {
                fileBean.setFilepostlocation(skipLen);
                fout = new FileOutputStream(tempFile);
            }
        } catch (IOException e) {
            LogLayout.error(logger,appName,"I/O错误",e);
        }
        BufferedInputStream bufferedInputStream = new BufferedInputStream(in);      //InputStream转BufferedInputStream
        ByteArrayOutputStream out = new ByteArrayOutputStream();              //Outputstream转ByteArrayOutputStream
        int post = 0;
        boolean isFirst = true;
        try{
            while ((len = bufferedInputStream.read(buf))!=-1){
                allSize += len;
                if(post >= packetSize){
                    post=0;
                    if(isFirst) {
                        if(fileSize==(long)allSize ){
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
                    int securityLevel = type.getInfoLevel();
                    if(securityLevel>0) {
                        String security = type.getSecurityFlag();
                        in = IOUtils.byte2Input(out.toByteArray());
                        in = SecurityUtils.encrypt(security, securityLevel, in);
                        buf = IOUtils.toByteArray(in);
                        this.process(buf, fileBean);    //发送
                        fout.write(buf);
                    } else {
                        this.process(out.toByteArray(), fileBean);
                        fout.write(out.toByteArray());
                    }
                    out.reset();
                    out.write(buf,0,len);
                    post += len;
                } else {
                    out.write(buf, 0, len);
                    post += len;
                }
            }
            if(post!=-1){
                fileBean.setSyncflag(FileContext.Str_SyncFileEnd);
                int securityLevel = type.getInfoLevel();
                if(securityLevel>0) {
                    String security = type.getSecurityFlag();
                    in = IOUtils.byte2Input(out.toByteArray());
                    in = SecurityUtils.encrypt(security, securityLevel, in);
                    buf = IOUtils.toByteArray(in);
                    this.process(buf, fileBean);
                    fout.write(buf);
                } else {
                    this.process(out.toByteArray(), fileBean);
                    fout.write(out.toByteArray());
                }
            }
            fout.flush();
//            client.completePendingCommand();
        } catch (FileNotFoundException e) {
            LogLayout.error(logger,appName,"下载保存目录不全",e);
        } catch (Exception e) {
            LogLayout.error(logger,appName,"下载写错误",e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                }
            }
        }
        return tempFile;
    }

    private File down(InputStream in, String localFileName, FileBean fileBean) {
        File tempFile = new File(ChangeConfig.getBackPath() + "/" + appName + localFileName);
        if(!tempFile.getParentFile().exists()) {
            tempFile.getParentFile().mkdirs();
        }
        int packetSize = 2 * StaticField.MB;
        byte[] buf = new byte[packetSize];
        int len = 0;
        int allSize = 0;
        int packet = 1;
        long fileSize = fileBean.getFilesize();
        boolean isWrite = false;
        fileBean.setFile_flag(FileContext.Str_SourceFile);
        try {
            BufferedInputStream bufferedInputStream = new BufferedInputStream(in);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int post = 0;
            FileOutputStream fout;
            if(fileBean.getFilepostlocation()>0){
                fout = new FileOutputStream(tempFile,true);
            } else {
                fout = new FileOutputStream(tempFile);
            }
            int tempFileSize = 0;
            while ((len=bufferedInputStream.read(buf))!=-1){
                allSize += len;
                if(packet ++ == 1) {
                    if(fileSize==(long)allSize){
                        fileBean.setSyncflag(FileContext.Str_SyncFileFlag);
                    } else {
                        fileBean.setSyncflag(FileContext.Str_SyncFileStart);
                    }
                } else {
                    if(fileSize==(long)allSize) {
                        fileBean.setSyncflag(FileContext.Str_SyncFileEnd);
                    } else {
                        fileBean.setSyncflag(FileContext.Str_SyncFileIng);
                    }
                }
                if(post >= packetSize){
//                    fileSize += post;
//                    LogLayout.info(logger,"platform","+++++++++++++ out  is : "+ out.toByteArray().length);
//                    LogLayout.info(logger,"platform","+++++++++++++ post is : "+ post);
                    tempFileSize += post;
                    fout.write(out.toByteArray());
                    post=0;

                    out.reset();
                    out.write(buf,0,len);
                    post+=len;
                } else {
                    out.write(buf, 0, len);
                    post+=len;
                }
            }
            if(post!=-1){
                tempFileSize += post;
                fout.write(out.toByteArray());
            }
            LogLayout.info(logger,"platform"," file temp size 2 is : " + tempFileSize);
            fout.flush();
            fout.close();
            out.close();
        } catch (IOException e) {
            LogLayout.error(logger,"platform","发送文件读取失败",e);
        } finally {
            if(in!=null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
        }
        return tempFile;
    }

    /**
     * 获取ftp被下载文件输入流
     * @return
     */
    private InputStream getInputStream(String fileName, FileBean fileBean) {
        InputStream in = null;
        do{
            try {
                in = client.retrieveFileStream(new String(fileName.getBytes(config.getCharset()),"iso-8859-1"));
                down(in,fileName,fileBean);
            } catch (IOException e) {
                LogLayout.error(logger,appName,"获取ftp中被下载文件流出错",e);
                disconnect();
            } finally {
                connect();
            }
        } while (in == null);
        return in;
    }

    /**
     * 删除文件
     * @param fileName
     */
    private void deleteFTPFile(String fileName) {
        boolean isSuccess = false;
        do{
            try {
                client.deleteFile(new String(fileName.getBytes(config.getCharset()),"iso-8859-1"));
                LogLayout.info(logger,appName,"文件["+fileName+"]已经备份,删除文件服务器上文件");
                isSuccess = true;
            } catch (Exception e) {
                disconnect();
                LogLayout.error(logger, appName, "删除文件夹错误", e);
            } finally {
                connect();
            }
        } while (!isSuccess);

    }

    /**
     * 校验本地是否已有备份文件
     * @param appName     应用名
     * @param fileName    文件名
     * @param lastModified
     * @param ftpFileSize   文件大小
     * @return
     */
    private boolean checkFileIsSaved(String appName, String fileName, long lastModified, long ftpFileSize) {
        File file = new File(ChangeConfig.getBackPath() + "/" + appName + fileName);
        if(file == null) {
            return false;
        }
        if(file.exists() && file.length()==ftpFileSize) {
            return true;
        }
        return false;
    }

    /**
     * 删除空文件夹
     * @param parent
     */
    private void deleteDir(String parent) {
        if(parent.length() > config.getDir().length()) {
            if(!parent.endsWith("/")) {
                parent += "/";
            }
            boolean isSuccess = false;
            do{
                try{
                    FTPFile[] files = client.listFiles(parent);
                    if(files!=null){
                        if(files.length==0) {
                            client.removeDirectory(new String(parent.getBytes(config.getCharset()),"iso-8859-1"));
                            LogLayout.info(logger, appName, "删除文件夹" + parent);
                        } else if(files.length==2) {
                            if( ".".equals(files[0].getName()) || "..".equals(files[0].getName())
                                    || ".".equals(files[1].getName()) || "..".equals(files[1].getName())){
//                            client.deleteFile(new String(parent.getBytes(config.getCharset()),"iso-8859-1"));
                                client.removeDirectory(new String(parent.getBytes(config.getCharset()),"iso-8859-1"));
                                LogLayout.info(logger,appName,"删除文件夹"+parent);
                            }
                        }
                    }
                    isSuccess = true;
                } catch (Exception e) {
                    LogLayout.error(logger,appName,"删除文件夹错误",e);
                    disconnect();
                } finally {
                    connect();
                }
            } while (!isSuccess);
        }
    }

    /**
     * 如果连接丢失,重新登录
     */
    public void reConnect() {
        while (!client.isConnected()){
            connect();
        }
    }

    /**
     * 建立ftp连接,并登录(被动模式)
     * @return
     */
    public boolean connect(){
        client = new FTPClient();
        String hostname = config.getServerAddress();
        int port = config.getPort();
        String username = config.getUserName();
        String password = config.getPassword();
        boolean isConnectOk = false;
        while (!isConnectOk) {
            try {
                client.connect(hostname, port);
                client.setControlEncoding(config.getCharset());
                if(FTPReply.isPositiveCompletion(client.getReplyCode())){
                    if(client.login(username, password)){
                        client.enterLocalPassiveMode();
                        client.setFileType(FTPClient.BINARY_FILE_TYPE);
                        client.setDefaultTimeout(1000 * 3);
                        client.setConnectTimeout(1000 * 3);
//                        client.setDataTimeout(1000 * 3);
                        isConnectOk = true;
                    } else {
                        disconnect();
                    }
                } else {
                    disconnect();
                }
            } catch (IOException e) {
                LogLayout.error(logger,appName,"[FTP]连接错误",e);
            }
        }
        return isConnectOk;
    }

    /**
     * ftp登录注销
     */
    public void disconnect() {
        try {
            client.logout();
        } catch (IOException e) {

        } finally {
            if(client.isConnected()){
                try {
                    client.disconnect();
                } catch (IOException e) {
                }
            }
        }
    }
}
