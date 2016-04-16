package com.hzih.itp.platform.config.http;

import com.hzih.logback.LogLayout;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.mime.HttpMultipart;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Created by 钱晓盼 on 14-1-13.
 */
public class Client {
    final static Logger logger = LoggerFactory.getLogger(Client.class);

    /*public static boolean changeByHttpClient(String appName,String url,String charset,String fileName,byte[] data) {
        boolean isResult = false;
        PostMethod postMethod = new PostMethod(url);
        try {
            FilePart fp = new FilePart("data", new ByteArrayPartSource("data", data),null,charset);
            fp.setContentType("text/html");
            StringPart spFileName = new StringPart("fileName",fileName,charset);
            spFileName.setContentType("text/html");
            StringPart spFileSize = new StringPart("fileSize",String.valueOf(data.length),charset);
            spFileSize.setContentType("text/html");
            Part[] parts = { fp,spFileName,spFileSize };
            MultipartRequestEntity mre = new MultipartRequestEntity(parts, postMethod.getParams());
            postMethod.setRequestEntity(mre);
            HttpClient client = new HttpClient();
            client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);// 设置连接时间
            int status = client.executeMethod(postMethod);
            if (status == HttpStatus.SC_OK) {
//                System.out.println(new String((postMethod.getResponseBodyAsString()).getBytes("utf-8")));
                isResult = true;
            }
        } catch (IOException e) {
            LogLayout.error(logger,appName,"发送错误",e);
        } finally {
            postMethod.releaseConnection();
        }
        return isResult;
    }*/

    /**
     * 适合多线程的HttpClient,用httpClient4.2.1实现
     * @return DefaultHttpClient
     */
    public static DefaultHttpClient getDefaultHttpClient(int port) {
        // 设置组件参数, HTTP协议的版本,1.1/1.0/0.9
        HttpParams params = new BasicHttpParams();
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        HttpProtocolParams.setUserAgent(params, "HttpComponents/1.1");
        HttpProtocolParams.setUseExpectContinue(params, true);


        //设置连接超时时间
        int REQUEST_TIMEOUT = 10*1000;  //设置请求超时10秒钟
        int SO_TIMEOUT = 10*1000*6*60;       //设置等待数据超时时间10秒钟
        //HttpConnectionParams.setConnectionTimeout(params, REQUEST_TIMEOUT);
        //HttpConnectionParams.setSoTimeout(params, SO_TIMEOUT);
        params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, REQUEST_TIMEOUT);
        params.setParameter(CoreConnectionPNames.SO_TIMEOUT, SO_TIMEOUT);

        //设置访问协议
        SchemeRegistry schreg = new SchemeRegistry();
        schreg.register(new Scheme("http",port, PlainSocketFactory.getSocketFactory()));
//    schreg.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));

        //多连接的线程安全的管理器
//        PoolingClientConnectionManager pccm = new PoolingClientConnectionManager(schreg);
//        pccm.setDefaultMaxPerRoute(20); //每个主机的最大并行链接数
//        pccm.setMaxTotal(100);          //客户端总并行链接最大数

        DefaultHttpClient httpClient = new DefaultHttpClient(params);
        return httpClient;
    }

    public static boolean changeByHttpClient(String appName,String url,int port,
                                             String charset,String fileName,byte[] data) {
        boolean isResult = false;
        try {
            ContentBody cbString = null;
            ContentBody cbFile = null;
            MultipartEntity entity = new MultipartEntity();
            cbString = new StringBody("参数", Charset.forName("utf-8"));
            entity.addPart("传递的参数名", cbString);
            cbFile = new ByteArrayBody(data,"utf-8");
            entity.addPart("文件名", cbFile);
            HttpPost httpPost = new HttpPost(url);
            httpPost.setEntity(entity);
            httpPost.setHeader("Connection", "close");
            httpPost.setHeader("fileName", fileName);
            httpPost.setHeader("fileSize", String.valueOf(data.length));

            HttpClient client = Client.getDefaultHttpClient(port);
            HttpResponse response = client.execute(httpPost);
            if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                isResult = true;
//                client.getConnectionManager().shutdown();
//                httpPost.releaseConnection();
            }
        } catch (IOException e) {
            LogLayout.error(logger,appName,"发送错误",e);
        }
        return isResult;
    }

    public static boolean changeFileByHttpClient(String appName, String url, int port, String charset, String fileName, File file) {
        boolean isResult = false;
        try {
            ContentBody cbString = null;
            ContentBody cbFile = null;
            MultipartEntity entity = new MultipartEntity();
            cbString = new StringBody("参数", Charset.forName("utf-8"));
            entity.addPart("传递的参数名", cbString);
//            cbFile = new ByteArrayBody(data,"utf-8");
            cbFile = new FileBody(file);
            entity.addPart("文件名", cbFile);
            HttpPost httpPost = new HttpPost(url);
            httpPost.setEntity(entity);
            httpPost.setHeader("Connection", "close");
            httpPost.setHeader("fileName", fileName);
            httpPost.setHeader("fileSize", String.valueOf(file.length()));

            HttpClient client = Client.getDefaultHttpClient(port);
            HttpResponse response = client.execute(httpPost);
            if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                isResult = true;
                httpPost.releaseConnection();
            }
        } catch (IOException e) {
            LogLayout.error(logger,appName,"发送错误",e);
        }
        return isResult;
    }
}
