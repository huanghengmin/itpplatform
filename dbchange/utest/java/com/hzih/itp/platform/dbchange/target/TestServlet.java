package com.hzih.itp.platform.dbchange.target;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.utils.StaticField;
import junit.framework.TestCase;

import java.io.*;

/**
 * Created by 钱晓盼 on 14-1-17.
 */
public class TestServlet extends TestCase {

    public void testWrite() {

//        FileReceiveServlet servlet = new FileReceiveServlet("sql_7402","utf-8");

        FileInputStream in = null;
        try {
            in = new FileInputStream("F:\\itp\\data\\sql_7402\\2014-01-17\\10\\1389926380812_0.tmp");
            write(in, "F:/itp/data_t/sql_7402/2014-01-17/10/1389926380812_0.tmp");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void write(InputStream in, String s) {
        int len = 0;
        byte[] buff = new byte[StaticField.MB * 2];
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(s);
            while ((len = in.read(buff))!=-1) {
                out.write(buff,0,len);
            }
            out.flush();
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
