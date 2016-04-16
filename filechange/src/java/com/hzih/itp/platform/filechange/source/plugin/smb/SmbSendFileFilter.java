package com.hzih.itp.platform.filechange.source.plugin.smb;

import com.inetec.common.config.stp.nodes.SourceFile;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileFilter;

/**
 * Created by 钱晓盼 on 14-2-9.
 */
public class SmbSendFileFilter implements SmbFileFilter {
    private SourceFile config;
    private int list = 0;
    public SmbSendFileFilter(SourceFile config) {
        this.config = config;
        this.list = config.getFilelistsize();
    }

    @Override
    public boolean accept(SmbFile smbFile) throws SmbException {
        list --;
        if(smbFile.isFile()){
            boolean isLnkFile = smbFile.length() == 0;
            if(!isLnkFile){
                boolean isFixed = fixed(smbFile.getName(),config);
                if(!isFixed) {     //过滤条件不符合
                    return false;
                }
                //校验 todo
                if(smbFile.length()==0) {
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    private boolean fixed(String fileName, SourceFile sourceFile) {
        boolean isFilterTypes = false;
        boolean isFilterTypesAll = false;
        if(sourceFile.getFiltertypes()!=null&&(sourceFile.getNotfiltertypes()==null||sourceFile.getNotfiltertypes().equals(""))){
            String filterType = sourceFile.getFiltertypes();
            isFilterTypesAll = filterType.equals("*.*");
            if(isFilterTypesAll){
                return true;
            } else {
                String[] filterTypes = filterType.split(",");
                if(filterTypes.length>1){
                    for(int i = 0 ; i < filterTypes.length;i++){
                        isFilterTypes = fileName.endsWith(filterTypes[i].substring(filterTypes[i].lastIndexOf(".")));
                        if(isFilterTypes){
                            return true;
                        }
                    }
                }else if(filterTypes.length == 1){
                    isFilterTypes = fileName.endsWith(filterType.substring(filterType.lastIndexOf(".")));
                    if(isFilterTypes){
                        return true;
                    }
                }
            }
        }
        if(sourceFile.getNotfiltertypes()!=null&& (sourceFile.getFiltertypes()==null||sourceFile.getFiltertypes().equals(""))){
            String filterType = sourceFile.getNotfiltertypes();
            isFilterTypesAll = filterType.equals("*.*");
            if(isFilterTypesAll){
                return false;
            } else {
                String[] filterTypes = filterType.split(",");
                if(filterTypes.length>1){
                    int flag = 0;
                    for(int i = 0 ; i < filterTypes.length;i++){
                        isFilterTypes = fileName.endsWith(filterTypes[i].substring(filterTypes[i].lastIndexOf(".")));
                        if(isFilterTypes){
                            flag ++;
                        }
                    }
                    if (flag == 0){
                        return true;
                    }
                }else if(filterTypes.length == 1){
                    isFilterTypes = fileName.endsWith(filterType.substring(filterType.lastIndexOf(".")));
                    if(!isFilterTypes){
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
