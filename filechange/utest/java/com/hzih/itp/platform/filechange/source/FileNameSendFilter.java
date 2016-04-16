package com.hzih.itp.platform.filechange.source;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Created by 钱晓盼 on 14-2-9.
 */
public class FileNameSendFilter implements FilenameFilter {
    private int list = 0;
    private int size = 0;
    public FileNameSendFilter(int i) {
        list = i;
    }

    @Override
    public boolean accept(File file, String s) {
        if(size >= list){
            System.out.println("size " + size + " list " + list);
            return false;
        }
        if(file.isDirectory()) {
            size ++;
            return true;
        }
        return false;
    }
}
