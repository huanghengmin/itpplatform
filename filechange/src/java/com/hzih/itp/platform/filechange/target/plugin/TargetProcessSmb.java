package com.hzih.itp.platform.filechange.target.plugin;

import com.hzih.itp.platform.filechange.target.TargetOperation;
import com.hzih.itp.platform.filechange.target.plugin.smb.TargetSmbUtils;
import com.hzih.itp.platform.filechange.utils.FileBean;
import com.hzih.itp.platform.filechange.utils.FileContext;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.TargetFile;
import com.inetec.common.config.stp.nodes.Type;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Created by 钱晓盼 on 14-1-9.
 */
public class TargetProcessSmb implements ITargetProcess {
    final Logger logger = LoggerFactory.getLogger(TargetProcessSmb.class);

    private TargetOperation target;
    private TargetFile config;
    private boolean isRun = false;
    private Type type;
    private String appName;

    @Override
    public boolean process(InputStream in, FileBean bean) {
        boolean isOk = true;       // 把输入流写入目标文件

        String targetFileFullName = makeFullName(bean.getFullname());
        bean.setFullname(targetFileFullName);

//        LogLayout.info(logger,"platform","fullName:"+bean.getFullname() + " position is " + bean.getFilepostlocation());
        if(bean.getFilepostlocation()>0) {
            isOk = TargetSmbUtils.writeFileRandom(in,bean,config);
        } else {
            isOk = TargetSmbUtils.writeFile(in, bean, config);       // 把输入流写入目标文件
        }
        if(FileContext.Str_SyncFileEnd.equals(bean.getSyncflag())
                || FileContext.Str_SyncFileFlag.equals(bean.getSyncflag())){
            isOk = TargetSmbUtils.reNameTarget(bean.getFullname(),config);
            int count = 100;
            boolean isExistIspe = TargetSmbUtils.checkTargetFile(bean.getFullname(),config);
            while (isExistIspe){
                count --;
                isOk = TargetSmbUtils.reNameTarget(bean.getFullname(),config);
                isExistIspe = TargetSmbUtils.checkTargetFile(bean.getFullname(),config);
                if(count==0) {
                    if(isExistIspe) {
                        LogLayout.warn(logger,appName,bean.getFullname() + FileContext.Str_SyncFileSourceProcess_End_Flag + "改名失败");
                    }
                    break;
                }
            }
        }
        return isOk;
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

    private String makeFullName(String fullName) {
        String[] dirs = fullName.substring(0,fullName.lastIndexOf("/")).split("/");
        String fName = "";
        for(int i=0;i<dirs.length;i++) {
            fName += "/"+dirs[i];
            SmbFile smbFile = TargetSmbUtils.getConnectSourceSmbFile(TargetSmbUtils.makeSmbUrlAddFullName(config,fName));
            try {
                if(smbFile.isDirectory()){
                    continue;
                }
                smbFile.mkdirs();
            } catch (SmbException e) {
                LogLayout.error(logger,appName,"[SMB同步]创建文件夹错误");
            }
        }
        return fullName;
    }
}
