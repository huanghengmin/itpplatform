package com.hzih.itp.platform.config.http;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.utils.StaticField;
import junit.framework.TestCase;
import org.apache.http.client.HttpClient;

import java.io.*;
import java.net.InetSocketAddress;

/**
 * Created by 钱晓盼 on 14-1-16.
 */
public class TestClient extends TestCase {

    public void testHttpClient() {
        String appName = "test";
        String ip = "127.0.0.1";
        int port = 8080;
        InetSocketAddress address = new InetSocketAddress(ip, port);
        ChangeConfig.targetAddresses.put(appName,address);
//        ChangeConfig.addHttpClientPool(appName);
        String url = "http://" + ip + ":" + port + StaticField.FileReceiveServlet;
//        byte[] buff = "hhhhh\r\n".getBytes();
//        String dir = "F:/1/qq";
        String dir = "F:/1/wwww/222";
        File file = new File(dir);
        File[] files = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                if(file.isFile()) {
                    return true;
                }
                return false;
            }
        });
        HttpClientImpl httpClient = null;
        for (int i =0; i < files.length; i ++) {
            httpClient = ChangeConfig.getHttpDataConnection(appName);
            String fileName = "/2014-01-16/11/" + files[i].getName();
            System.out.println(i);
            long start = System.currentTimeMillis();
            boolean isSuccess = httpClient.changeFileByHttpClient(appName, url, "utf-8", fileName, files[i]);
            ChangeConfig.returnHttpConnection(appName,httpClient);
            long end = System.currentTimeMillis();
            System.out.println(isSuccess + " : " + (end - start) );

        }
    }

    public void testMakeFile() {
        String dir = "F:/1/qq";
        for (int i =0; i < 5000; i ++) {
            FileOutputStream out = null;
            try {
                out = new FileOutputStream(dir + "/itp-test-"+i+".txt");
                out.write("zhongggggggggg".getBytes());
                out.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void testClient() {
        String appName = "sql_7402";
        String ip = "192.168.1.206";
        int port = 7402;
        InetSocketAddress address = new InetSocketAddress(ip, port);
        ChangeConfig.targetAddresses.put(appName,address);
//        ChangeConfig.addHttpClientPool(appName);
        String url = "http://" + ip + ":" + port + StaticField.FileReceiveServlet;
        String dir = "F:\\itp\\data\\sql_7402\\2014-01-17\\10\\1389926380812_0.tmp";
        File file = new File(dir);
        HttpClientImpl httpClient = null;
        httpClient = ChangeConfig.getHttpDataConnection(appName);
        String fileName = "/2014-01-17/10/" + file.getName();
        long start = System.currentTimeMillis();
        boolean isSuccess = httpClient.changeFileByHttpClient(appName, url, "utf-8", fileName, file);
        ChangeConfig.returnHttpConnection(appName,httpClient);
        long end = System.currentTimeMillis();
        System.out.println(isSuccess + " : " + (end - start) );
    }

}
