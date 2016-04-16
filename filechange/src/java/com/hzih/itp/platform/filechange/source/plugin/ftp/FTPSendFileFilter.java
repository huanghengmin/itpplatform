package com.hzih.itp.platform.filechange.source.plugin.ftp;


import com.inetec.common.config.stp.nodes.SourceFile;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;

/**
 * Created with IntelliJ IDEA.
 * User: 钱晓盼
 * Date: 13-7-13
 * Time: 下午1:49
 * 按照文件类型过滤,格式校验,内容校验
 */
public class FTPSendFileFilter implements FTPFileFilter {
    SourceFile config;
    public FTPSendFileFilter(SourceFile config) {
        this.config = config;
    }

    @Override
    public boolean accept(FTPFile ftpFile) {
        if(ftpFile.isFile()){
            String fileName = ftpFile.getName();
            if(ftpFile.getSize() > 0){
                boolean isFixed = fixed(fileName,config);
                if(!isFixed) {     //过滤条件不符合
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
