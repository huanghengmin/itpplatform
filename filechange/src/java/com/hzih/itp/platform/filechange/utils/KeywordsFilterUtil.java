package com.hzih.itp.platform.filechange.utils;

import com.inetec.common.config.stp.nodes.SourceFile;

import java.io.InputStream;

/**
 * Created by 钱晓盼 on 14-1-13.
 */
public interface KeywordsFilterUtil {

    public byte[] filter(byte[] data,String keywords,SourceFile sourceFile) throws Exception;

    public InputStream filter(InputStream inputStream,String keywords,SourceFile sourceFile) throws Exception;
}
