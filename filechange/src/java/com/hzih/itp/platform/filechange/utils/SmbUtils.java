package com.hzih.itp.platform.filechange.utils;

import com.hzih.logback.LogLayout;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.UnknownHostException;

/**
 * Created by 钱晓盼 on 14-2-9.
 */
public class SmbUtils {
    final static Logger logger = LoggerFactory.getLogger(SmbUtils.class);


    private static InputStream getSmbFileInputStream(SmbFile smbFile,String appName) {
        InputStream in = null;
        try {
            in = new SmbFileInputStream(smbFile);
        } catch (SmbException e) {
            LogLayout.error(logger, appName, "[SMB同步]getSmbFileInputStream()" + e.getMessage(), e);
        } catch (MalformedURLException e) {
            LogLayout.error(logger,appName,"[SMB同步]getSmbFileInputStream()"+e.getMessage(),e);
        } catch (UnknownHostException e) {
            LogLayout.error(logger,appName,"[SMB同步]getSmbFileInputStream()"+e.getMessage(),e);
        }
        return in;
    }

    public static InputStream getConnectSmbFileInputStream(SmbFile smbFile,String appName) {
        InputStream in = null;
        try {
            if(smbFile.exists()){
                in = getSmbFileInputStream(smbFile,appName);
            }else{
                return null;
            }
        } catch (SmbException e) {
            LogLayout.error(logger,appName,"getConnectSmbFileInputStream",e);
        }
        int index = 1;
        while (in == null){
            if(index > 5){
                LogLayout.error(logger,appName,"[SMB同步]服务连接超时");
                return null;
            }
            LogLayout.error(logger,appName,"[SMB同步]getConnectSmbFileInputStream()网络连接失败,等待60秒..,当前URL:"+smbFile.getURL());
            try {
                Thread.sleep(1000*60);
                index ++;
            } catch (InterruptedException e) {
            }
            try {
                if(smbFile.exists()){
                    in = getSmbFileInputStream(smbFile,appName);
                }else{
                    return null;
                }
            } catch (SmbException e) {
                LogLayout.error(logger,appName,"[SMB同步]"+e.getMessage()+", 当前URL:"+smbFile.getURL(),e);
            }
        }
        return in;
    }
}
