package com.hzih.itp.platform.filechange.source;

import junit.framework.TestCase;

import java.io.File;

/**
 * Created by 钱晓盼 on 14-2-9.
 */
public class TestFileList extends TestCase {

    public void testFileList() {
        File file = new File("F:/1");
        String[] fns;
        FileNameSendFilter filter = new FileNameSendFilter(5);
        do{
            fns = file.list(filter);
            System.out.println(fns.length);
        } while (fns.length >= 5);
    }
}
