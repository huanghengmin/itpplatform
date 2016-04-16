package com.hzih.itp.platform.filechange.source.plugin.smb;

import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.SourceFile;
import jcifs.smb.SmbFile;
import sun.util.logging.resources.logging;

import java.net.MalformedURLException;

/**
 * Created by 钱晓盼 on 14-2-9.
 */
public class SourceSmbUtils {

    /**
     * 通过源端配置信息组织url 最后追加"/"
     * @param config
     * @return
     */
    public static String makeSmbUrlHeader(SourceFile config){
        String parent = config.getDir() ;
        String server = config.getServerAddress() + ":" + config.getPort();
        String userName = config.getUserName();
        String password = config.getPassword();
        String smbUrl = "smb://" + userName + ":" + password + "@" + server + parent;

        return smbUrl;
    }

    /**
     * 通过源端配置信息组织url 最后追加"/"
     * @param config
     * @return
     */
    public static String appenderSmbUrlTail(SourceFile config,String urlHeader){
        if(urlHeader.endsWith("/")) {
            return urlHeader + "?iocharset="+config.getCharset();
        }
        return urlHeader + "/?iocharset="+config.getCharset();
    }

    public static String makeSmbFilePath(SourceFile config){
        //        String filePath = "smb://17.8.2.6/share/";
        return "smb://"+ config.getServerAddress() + config.getDir() + "/";
    }


    public static SmbFile makeSmbUrl(SourceFile config, String fileName) throws MalformedURLException {
        String header = makeSmbUrlHeader(config);
        header += fileName;
        String url = appenderSmbUrlTail(config,header);
        return new SmbFile(url);
    }
}
