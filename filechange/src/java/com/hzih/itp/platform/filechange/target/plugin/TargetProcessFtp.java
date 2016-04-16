package com.hzih.itp.platform.filechange.target.plugin;

import com.hzih.itp.platform.filechange.target.TargetOperation;
import com.hzih.itp.platform.filechange.utils.FileBean;
import com.hzih.itp.platform.filechange.utils.FileContext;
import com.hzih.logback.LogLayout;
import com.hzit.itp.platform.getmd5.FileDigest;
import com.inetec.common.config.stp.nodes.TargetFile;
import com.inetec.common.config.stp.nodes.Type;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by 钱晓盼 on 14-1-9.
 */
public class TargetProcessFtp implements ITargetProcess {
    final Logger logger = LoggerFactory.getLogger(TargetProcessFtp.class);

    private TargetOperation target;
    private TargetFile config;
    private FTPClient client;
    private boolean isRun = false;
    private Type type;
    private String appName;

    @Override
    public boolean process(InputStream in, FileBean bean) {
        String targetFileFullName = null;
        if("/".equals(config.getDir())) {
            targetFileFullName = bean.getFullname();
        } else {
            if(config.getDir().endsWith("/")) {
                targetFileFullName = config.getDir() + bean.getFullname().substring(1);
            } else {
                targetFileFullName = config.getDir() + bean.getFullname();
            }
        }
        bean.setFullname(targetFileFullName);

        boolean isConnect = connect();
        if(isConnect){
            String dirName="";
            String dir = null;
            if(targetFileFullName.indexOf("/")>-1){
                dir = makeNewDir(targetFileFullName);
            }
            if(dir!=null) {
                try {
                    client.changeWorkingDirectory(new String(dir.getBytes(config.getCharset()),"iso-8859-1"));
                } catch (IOException e) {
                    LogLayout.error(logger,appName,"改变路径错误",e);
                }
            }
//            LogLayout.info(logger,"platform","[FTP]write position size is : "+bean.getFilepostlocation());
            if(bean.getFilepostlocation()>0) {
                writeRandomFile(in,bean);
            } else {
                writeFile(in, bean);
            }
            disconnect();
        }
        return isConnect;
    }



    @Override
    public boolean process(byte[] data, FileBean bean) {
        return false;
    }

    @Override
    public void init(TargetOperation target, TargetFile config) {
        this.target = target;
        this.type = target.getType();
        this.config = config;
        this.appName = type.getTypeName();

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
        while (isRun) {
            try {
                Thread.sleep(1000 * 60);
            } catch (InterruptedException e) {
            }
        }
    }

    private String makeNewDir(String fullName) {
        String[] dirs = fullName.substring(0, fullName.lastIndexOf("/")).split("/");
        String dir = "";
        for(int i=1;i<dirs.length;i++) {
            dir += "/"+dirs[i];
            FTPFile dirFile = getFTPFileByFullName(dir);
            if(dirFile!=null&&dirFile.isDirectory()) {
                continue;
            }
//        reConnect();
            try {
                client.makeDirectory(new String(dir.getBytes(config.getCharset()),"iso-8859-1"));
            } catch (IOException e) {
                LogLayout.error(logger,appName,"创建文件夹失败",e);
            }
        }
//        LogLayout.info(logger,"platform","...."+dir);
        return dir;
    }

    private FTPFile getFTPFileByFullName(String fullName) {
        FTPFile file = null;
        try {
            file = client.mlistFile(new String(fullName.getBytes(config.getCharset()),"iso-8859-1"));
        } catch (IOException e) {
            LogLayout.error(logger,appName,"查找文件失败["+fullName+"]",e);
        }
        return file;
    }

    private void writeRandomFile(InputStream in, FileBean bean) {
        String targetFileName = null;
        targetFileName = new String(bean.getFullname() + FileContext.Str_SyncFileSourceProcess_End_Flag);
        try {
            BufferedInputStream fiStream = new BufferedInputStream(in);
            client.appendFile(new String((targetFileName).getBytes(config.getCharset()),"iso-8859-1"),fiStream);
            fiStream.close();
        } catch (IOException e) {
            LogLayout.error(logger,appName,"创建输出流错误",e);
        } finally {
            if(in!=null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private void writeFile(InputStream in, FileBean bean) {

        String targetFileName = null;
        targetFileName = new String(bean.getFullname() + FileContext.Str_SyncFileSourceProcess_End_Flag);
        try {
            targetFileName = new String((targetFileName).getBytes(config.getCharset()),"iso-8859-1");
            if(FileContext.Str_SyncFileStart.equals(bean.getSyncflag()) || FileContext.Str_SyncFileFlag.equals(bean.getSyncflag())){
                BufferedInputStream fiStream = new BufferedInputStream(in);
                client.storeFile(targetFileName,fiStream);
                fiStream.close();
            } else {
                client.appendFile(targetFileName,in);
            }
            if(FileContext.Str_SyncFileEnd.equals(bean.getSyncflag())) {
                String fileFullName = new String((bean.getFullname()).getBytes(config.getCharset()),"iso-8859-1");
                changeFileName(targetFileName,fileFullName);
                LogLayout.info(logger,appName,"完成文件["+bean.getFullname()+"]的同步");
                //校验MD5值  todo
                InputStream result_in = client.retrieveFileStream(new String(fileFullName.getBytes(config.getCharset()),"iso-8859-1"));
                String result_md5 = FileDigest.getFileMD5(in);
                if(result_md5 != null && bean.getMd5().equals(result_md5)){
                    LogLayout.info(logger,appName,"源文件MD5值与同步文件MD5值不同:源文件MD5值为 " + bean.getMd5() + ",目标文件MD5值为 " + result_md5);
                }
                if(result_in != null){
                    result_in.close();
                }
            }
        } catch (IOException e) {
            LogLayout.error(logger,appName,"创建输出流错误",e);
        } finally {
            if(in!=null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * 把source文件改名成target文件
     * @param source
     * @param target
     */
    private void changeFileName(String source, String target) {
        boolean isSuccess = false;
        do{
            try {
                client.rename(source,target);
                isSuccess = true;
            } catch (IOException e) {
                LogLayout.error(logger,appName,"改名错误",e);
            }
        } while (!isSuccess);
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
        int index = 0;
        while (!isConnectOk) {
            try {
                client.connect(hostname, port);
                client.setControlEncoding(config.getCharset());
                client.setDataTimeout(60000);       //设置传输超时时间为60秒
                client.setConnectTimeout(60000);       //连接超时为60秒
                client.setBufferSize(1024*1024*5);
                if(FTPReply.isPositiveCompletion(client.getReplyCode())){
                    if(client.login(username, password)){
                        if (FTPReply.isPositiveCompletion(client.sendCommand("OPTS UTF8", "ON"))){ // 发送OPTS UTF8指令尝试支持utf-8
                            config.setCharset("utf-8");
                        }
                        client.setControlEncoding(config.getCharset());
                        client.enterLocalPassiveMode();
                        client.setBufferSize(1024);
                        client.setFileType(FTPClient.BINARY_FILE_TYPE);
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
            index ++;
            if(index>10) {
                break;
            }
        }
        return isConnectOk;
    }

    /**
     * 注销ftp登录
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
        client = null;
    }
}
