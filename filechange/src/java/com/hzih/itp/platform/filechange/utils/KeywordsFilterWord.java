package com.hzih.itp.platform.filechange.utils;

import com.inetec.common.config.stp.nodes.SourceFile;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.usermodel.Range;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

/**
 * Created by 钱晓盼 on 14-1-13.
 */
public class KeywordsFilterWord  implements KeywordsFilterUtil{

    @Override
    public byte[] filter(byte[] data,String keywords,SourceFile sourceFile) throws Exception {
        if(keywords == null || keywords.equals("")){
            return data;
        }
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        HWPFDocument hdt = new HWPFDocument(byteArrayInputStream);
        Range range = hdt.getRange();
        if(keywords.contains(",")){
            String[] keyword = keywords.split(",");
            for(int i = 0 ; i < keyword.length ; i++){
                range.replaceText(keyword[i],replace(keyword[i]));
            }
        }
        else {
            range.replaceText(keywords,replace(keywords));
        }
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        hdt.write(byteArrayOutputStream);
        return byteArrayOutputStream.toByteArray();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InputStream filter(InputStream inputStream,String keywords,SourceFile sourceFile) throws Exception {
        if(keywords == null || keywords.equals("")){
            return inputStream;
        }
        HWPFDocument hdt = new HWPFDocument(inputStream);
        Range range = hdt.getRange();
        if(keywords.contains(",")){
            String[] keyword = keywords.split(",");
            for(int i = 0 ; i < keyword.length ; i++){
                range.replaceText(keyword[i],replace(keyword[i]));
            }
        }
        else {
            range.replaceText(keywords,replace(keywords));
        }
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        hdt.write(byteArrayOutputStream);
        return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());  //To change body of implemented methods use File | Settings | File Templates.
    }
    private String replace(String keyword){
        String str = "**";
        for(int i = 1 ; i < keyword.length() ; i++){
            str = str+"**";
        }
        return  str ;
    }
}
