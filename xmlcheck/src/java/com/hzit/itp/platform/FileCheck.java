package com.hzit.itp.platform;

import com.hzih.logback.LogLayout;
import com.hzit.itp.platform.datacheck.BlackWords;
import com.hzit.itp.platform.formcheck.dom4j.ValidataXML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 14-5-27
 * Time: 下午3:43
 * To change this template use File | Settings | File Templates.
 */
public class FileCheck {
    final static Logger logger = LoggerFactory.getLogger(FileCheck.class);

    private boolean isRun = false;
    public static boolean isRunBlackWord = false;
    public static BlackWords blackWords = new BlackWords();

    public void init(){
        LogLayout.info(logger, "xmlcheck", "开启 查询内容黑名单 开始. . .");
        runBlackWord();
        LogLayout.info(logger, "xmlcheck", "开启 查询内容黑名单 成功. . .");
        isRun = true;
    }


    private static void runBlackWord(){
        if(isRunBlackWord){
            return;
        }else {
            blackWords.init();
            Thread thread = new Thread(blackWords);
            thread.start();
            FileCheck.isRunBlackWord = true;
        }
    }

    public boolean isRunning() {
        return isRun;
    }

    public void close() {
        isRun = false;
    }

    public String checkFile(String xmlFilePath,String xsdFilePath){
        String json = null;
        File xmlFile = new File(xmlFilePath);
        if(!xmlFile.exists()){
            json = "xml文件不存在!";
            return json;
        }
        ValidataXML validataXML = new ValidataXML(xmlFilePath,xsdFilePath);
        json = validataXML.validateXMLByXSD();
        /*if(json.indexOf("success") > -1){
            json = FileDigest.getFileMD5(xmlFile);
        }*/
        return json;
    }

    public String checkFile(InputStream in,String xsdFile){
        String json = null;
        if(in != null){
            json = "xml文件流为空";
            return  json;
        }
        ValidataXML validataXML = new ValidataXML(in,xsdFile);
        json = validataXML.validateXMLByXSD();
        /*if(json.indexOf("success") > -1){
            json = FileDigest.getFileMD5(xmlFile);
        }*/
        return json;

    }

    public static void main(String[] args){
        FileCheck fileCheck = new FileCheck();
        fileCheck.init();
        String json = fileCheck.checkFile("E://itp/ztt.xml","E://itp/ztt.xsd");
        System.out.println(json);

    }



}
