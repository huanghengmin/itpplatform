package com.hzih.itp.platform.filechange.target.plugin.smb;

import com.hzih.itp.platform.filechange.utils.FileBean;
import com.hzih.itp.platform.filechange.utils.FileContext;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.TargetFile;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.UnknownHostException;

/**
 * Created by 钱晓盼 on 14-2-9.
 */
public class TargetSmbUtils {
    final static Logger logger = LoggerFactory.getLogger(TargetSmbUtils.class);

    public static SmbFile getSmbFile(String url){
        SmbFile smbFile = null;
        try {
            smbFile = new SmbFile(url);
            smbFile.connect();
            return smbFile;
        } catch (MalformedURLException e) {
        } catch (IOException e) {
            LogLayout.error(logger,"platform","[SMB同步]",e);
        }
        return null;
    }

    /**
     * 通过目标配置信息组织url 最后不追加"/"
     * @param config
     * @return
     */
    public static String makeSmbUrlEndNotExist(TargetFile config){
        String filePath = config.getServerAddress() + ":" + config.getPort() + config.getDir();
        String userName = config.getUserName();
        String password = config.getPassword();
        String smbUrl = "smb://" + userName + ":" + password + "@" + filePath + "?iocharset="+config.getCharset();
        return smbUrl;
    }

    public static String makeSmbUrlAddFullName(TargetFile config, String fullName) {
        String url = makeSmbUrlEndNotExist(config);
        return url.split("\\?")[0] + fullName + "?" +url.split("\\?")[1];
    }

    public static SmbFile getConnectTargetSmbFile(String url){

        SmbFile smbFile = getSmbFile(url);
        int index = 1;
        while (smbFile == null) {
            if(index > 5){
                LogLayout.info(logger, "platform", "[SMB同步]服务连接超时");
                return null;
            }
            LogLayout.info(logger,"platform","[SMB同步]网络连接失败,等待60秒..,当前URL:"+url);
            try {
                Thread.sleep(1000*60);
                index ++;
            } catch (InterruptedException e) {
            }
            smbFile = getSmbFile(url);
        }
        return smbFile;
    }

    private static SmbFileOutputStream getSmbFileOutputStream(SmbFile smbFile) {
        SmbFileOutputStream out = null;
        try {
            out = new SmbFileOutputStream(smbFile);
        } catch (SmbException e) {
            LogLayout.error(logger,"platform","[SMB同步]"+e.getMessage(),e);
        } catch (MalformedURLException e) {
            LogLayout.error(logger,"platform","[SMB同步]"+e.getMessage(),e);
        } catch (UnknownHostException e) {
            LogLayout.error(logger,"platform","[SMB同步]"+e.getMessage(),e);
        }
        return out;
    }

    private static SmbFileOutputStream getConnectSmbFileOutputStream(SmbFile smbFile) {
        SmbFileOutputStream out = getSmbFileOutputStream(smbFile);
        int index = 1;
        while (smbFile == null){
            if(index > 5){
                LogLayout.info(logger,"platform","[SMB同步]服务连接超时");
                return null;
            }
            LogLayout.info(logger,"platform","[SMB同步]getConnectSmbFileOutputStream()网络连接失败,等待20秒..,当前URL:"+smbFile.getURL());
            try {
                Thread.sleep(1000*20);
                index ++;
            } catch (InterruptedException e) {
            }
            out = getSmbFileOutputStream(smbFile);
        }
        return out;
    }

    private static SmbFileOutputStream getSmbFileOutputStream(SmbFile smbFile,boolean append) {
        SmbFileOutputStream out = null;
        try {
            out = new SmbFileOutputStream(smbFile,append);
        } catch (SmbException e) {
            LogLayout.error(logger,"platform","[SMB同步]"+e.getMessage(),e);
        } catch (MalformedURLException e) {
            LogLayout.error(logger,"platform","[SMB同步]"+e.getMessage(),e);
        } catch (UnknownHostException e) {
            LogLayout.error(logger,"platform","[SMB同步]"+e.getMessage(),e);
        }
        return out;
    }

    private static SmbFileOutputStream getConnectSmbFileOutputStream(SmbFile smbFile,boolean append) {
        SmbFileOutputStream out = getSmbFileOutputStream(smbFile,append);
        int index = 1;
        while (out == null){
            if(index > 5){
                return null;
            }
            LogLayout.info(logger,"platform","[SMB同步]getConnectSmbFileOutputStream()网络连接失败,等待20秒..,当前URL:"+smbFile.getURL());
            try {
                Thread.sleep(1000*20);
                index ++;
            } catch (InterruptedException e) {
            }
            out = getSmbFileOutputStream(smbFile,append);
        }
        return out;
    }

    public static SmbFile getConnectSourceSmbFile(String url) {
        SmbFile smbFile = getSmbFile(url);
        int index = 1;
        while (smbFile == null) {
            if(index > 5){
                LogLayout.info(logger,"platform","[SMB同步]服务连接超时");
                return null;
            }
            LogLayout.info(logger,"platform","[SMB同步]网络连接失败,等待60秒..,当前URL:"+url);
            try {
                Thread.sleep(1000*60);
                index ++;
            } catch (InterruptedException e) {
            }
            smbFile = getSmbFile(url);
        }
        return smbFile;
    }


    /**
     *  通过smb协议写入文件输入流
     *  1. 取到正确url
     *  2. 判断smbFile是否文件夹,是则加上文件名 并且设置成可读写
     *  3. 判断是否存在文件(加锁前),不存在 4,5,6;存在 7.
     *  4. 设置锁(加上后缀)
     *  5. 判断加上后缀后的url文件是否存在,不是则先判断是否有文件夹不存在,有则建立,再分成多个2M的包传送
     *  6. 判断加上后缀后的url文件是否存在,是则判断该文件是否大于源端文件,大于则覆盖,小于追加,等于则直接返回
     *  7. 判断该文件是否大于源端文件,大于则覆盖,小于追加,等于则直接返回
     * @param in      输入流
     * @param config   目标文件信息
     */
    public static boolean writeFile(InputStream in,FileBean sourceBean,TargetFile config) {
        String smbUrl = makeSmbUrlEndNotExist(config);
        SmbFile smbFile = null;
        SmbFile reSmbFile = null;
        SmbFileOutputStream out = null;
        String fileFullName = sourceBean.getFullname();
        try {
            reSmbFile = getConnectTargetSmbFile(makeSmbUrlAddFullName(config, fileFullName + FileContext.Str_SyncFileSourceProcess_End_Flag));
            if (reSmbFile == null){
                return false;
            }
            if(FileContext.Str_SyncFileStart.equals(sourceBean.getSyncflag())
                    || FileContext.Str_SyncFileFlag.endsWith(sourceBean.getSyncflag())){
                reSmbFile.createNewFile();
                reSmbFile.canWrite();
                out = getConnectSmbFileOutputStream(reSmbFile);
                if (out == null){
                    return false;
                }
                return write(in,out);
            } else {
                out = getConnectSmbFileOutputStream(reSmbFile,true);
                if (out == null){
                    return false;
                }
                return write(in,out);
            }
        } catch (Exception e) {
            logger.debug("写文件("+sourceBean.getFullname()+"):" ,e);
            return false;
        }
    }

    public static boolean writeFileRandom(InputStream in,FileBean sourceBean,TargetFile config) {
        String smbUrl = makeSmbUrlEndNotExist(config);
        SmbFile reSmbFile = null;
        SmbFileOutputStream out = null;
        String fileFullName = sourceBean.getFullname();
        SmbFile smbFile = getConnectTargetSmbFile(makeSmbUrlAddFullName(config, fileFullName));
        try {
            reSmbFile = getConnectTargetSmbFile(makeSmbUrlAddFullName(config, fileFullName + FileContext.Str_SyncFileSourceProcess_End_Flag));
            if (reSmbFile == null){
                return false;
            }
            if(FileContext.Str_SyncFileStart.equals(sourceBean.getSyncflag())
                    || FileContext.Str_SyncFileFlag.endsWith(sourceBean.getSyncflag())){
                smbFile.renameTo(reSmbFile);
            }
            out = getConnectSmbFileOutputStream(reSmbFile,true);
            if (out == null){
                LogLayout.error(logger,"platform","目标端获取输出流错误out==null");
                return false;
            }
            return write(in,out);
        } catch (Exception e) {
            logger.debug("写文件("+sourceBean.getFullname()+"):"  ,e);
            return false;
        }
    }

    private static boolean write(InputStream in, SmbFileOutputStream out) {
        try {
            IOUtils.copy(in, out);
            in.close();
            out.close();
        } catch (IOException e) {
            logger.debug("写文件 ",e);
            return false;
        }
        return true;
    }

    /**
     * 目标文件同步完成后 改回原名
     * @param config     目标配置信息
     */
    public static boolean reNameTarget(String fullName, TargetFile config) {
        String reNameFileName = fullName + FileContext.Str_SyncFileSourceProcess_End_Flag;
        SmbFile smbFile = null;
        SmbFile reSmbFile = null;
        try {
            smbFile = getConnectTargetSmbFile(makeSmbUrlAddFullName(config, fullName));
            if(smbFile.exists()) {
                smbFile.delete();
            }
            reSmbFile = getConnectTargetSmbFile(makeSmbUrlAddFullName(config, reNameFileName));
            reSmbFile.renameTo(smbFile);
        } catch (IOException e) {
            logger.debug("reNameTarget() rename "+reNameFileName+" to "+ fullName +"",e);
            return false;
        }
        return true;
    }

    public static boolean checkTargetFile(String fullName, TargetFile config) {
        String fullNameEnd = fullName + FileContext.Str_SyncFileSourceProcess_End_Flag;
        String url = makeSmbUrlAddFullName(config, fullNameEnd);
        SmbFile smbFile = getSmbFile(url);
        if(smbFile!=null){
            try {
                if(smbFile.exists()) {
                    return true;
                }
            } catch (SmbException e) {

            }
        }
        return false;
    }


}
