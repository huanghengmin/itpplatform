/*=============================================================
 * 文件名称: DataInformation.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-11-12
 * ============================================================
 * <p>版权所有  (c) 2005 杭州网科信息工程有限公司</p>
 * <p>
 * 本源码文件作为杭州网科信息工程有限公司所开发软件一部分，它包涵
 * 了本公司的机密很所有权信息，它只提供给本公司软件的许可用户使用。
 * </p>
 * <p>
 * 对于本软件使用，必须遵守本软件许可说明和限制条款所规定的期限和
 * 条件。
 * </p>
 * <p>
 * 特别需要指出的是，您可以从本公司软件，或者该软件的部件，或者合
 * 作商取得并有权使用本程序。但是不得进行复制或者散发本文件，也不
 * 得未经本公司许可修改使用本文件，或者进行基于本程序的开发，否则
 * 我们将在最大的法律限度内对您侵犯本公司版权的行为进行起诉。
 * </p>
 * ==========================================================*/
package com.hzih.itp.platform.dbchange.datautils;


import com.hzih.itp.platform.utils.DataAttributes;
import com.hzih.itp.platform.utils.HeaderProcess;
import com.hzih.logback.LogLayout;
import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;


public class DataInformation {
    private static Logger logger = LoggerFactory.getLogger(DataInformation.class);
    public final static String Str_DataType = "Data_Type";
    public final static String Str_DataType_Basic = "Basic";
    public final static String Str_DataType_Clob = "Clob";
    public final static String Str_DataType_Blob = "Blob";
    public final static String Str_DataType_Success = "Success";

    public final static String Str_FieldName = "FieldName";

    public final static String Str_Data_Length = "Length";

    public final static String Str_CharSet = "charset";

    public final static String Str_Pks = "pks";

    public final static String Str_CharacterSet = "utf-8";

    protected Properties header = new Properties();
    protected InputStream m_in = null;
    protected long contentPosition = 0;
    protected byte[] m_buff = null;
    protected File m_file = null;

    public DataInformation(InputStream in, long size) throws Ex {
        m_in = in;
        header = new HeaderProcess().parseHeader(m_in);
        if (size > MDataParseImp.I_lobDataSize) {
            LogLayout.info(logger, "platform", "Lob 数据长度 大于 " + size);
            oscach();
        } else {
            cach();
        }
        m_in = getContentStream();
    }

    public Properties getHeader() {
        return header;
    }

    public String getDateType() {
        return (String) header.getProperty(Str_DataType);
    }

    public int getContentLength() {
        return new Integer(header.getProperty(Str_Data_Length)).intValue();
    }

    public long getContentPosition() {
        return contentPosition;
    }

    public InputStream getContentStream() throws Ex {
        InputStream result = null;
        if (m_buff != null) {
            result = new ByteArrayInputStream(m_buff);
        }
        if (m_file != null) {
            try {
                result = new FileInputStream(m_file);
            } catch (FileNotFoundException e) {
                throw new Ex().set(E.E_FileNotFound, new Message("数据暂存读取失败."));
            }
        }
        return result;
    }

    public boolean isBasicData() {
        return getDateType().equals(Str_DataType_Basic);
    }

    public boolean isBlobData() {
        if(getDateType()==null) {
            LogLayout.info(logger,"platform","data type is null ");
        }
        return getDateType().equals(Str_DataType_Blob);
    }

    public boolean isClobData() {
        return getDateType().equals(Str_DataType_Clob);
    }

    public boolean isSuccessData() {
        return getDateType().equals(Str_DataType_Success);
    }

    public void cach() throws Ex {
        try {
            m_buff = DataAttributes.readInputStream(m_in);

            if (logger.isDebugEnabled()) {
                LogLayout.debug(logger,"Data is:" + new String(m_buff));
            }
            //System.out.println("Data is:"+new String(m_buff));
        } catch (IOException e) {
            throw new Ex().set(E.E_IOException, new Message("数据暂存出错."));
        }
    }

    public void oscach() throws Ex {
        FileOutputStream out = null;
        BufferedOutputStream buff = null;
        byte[] bytebuff = null;
        try {
            m_file = File.createTempFile("#com.hzih.itp.platform.dbchange-datainfo", "tmp");
            out = new FileOutputStream(m_file);
            buff = new BufferedOutputStream(out);
            int bufflength = 0;
            if (m_in.available() > 1024)
                bytebuff = new byte[1024];
            else {
                bufflength = (int) m_in.available();
                bytebuff = new byte[bufflength];
            }
            int rc = 0;
            long count = 0;
            long temp = 0;
            rc = m_in.read(bytebuff);
            while (rc > 0) {
                count = count + rc;
                buff.write(bytebuff);

                if (m_in.available() < 1024) {
                    bufflength = (int) m_in.available();
                    bytebuff = new byte[bufflength];
                } else {
                    bytebuff = new byte[1024];
                }

                rc = m_in.read(bytebuff);
            }
            buff.flush();
            buff.close();
            out.close();
        } catch (IOException e) {
            throw new Ex().set(E.E_IOException, e);
        }
    }

    public static InputStream readInputStream(InputStream in, long size) throws IOException {
        ByteArrayOutputStream swapStream = new ByteArrayOutputStream();
        int bufflength = 0;
        byte[] buff = null;
        if (size > 1024)
            buff = new byte[1024];
        else {
            bufflength = (int) size;
            buff = new byte[bufflength];
        }
        int rc = 0;
        long count = 0;
        long temp = 0;
        rc = in.read(buff);
        while (count < size && rc > 0) {
            count = count + rc;
            swapStream.write(buff);
            temp = size - count;
            if (temp < 1024) {
                bufflength = (int) temp;
                buff = new byte[bufflength];
            }
            rc = in.read(buff);

        }
        return new ByteArrayInputStream(swapStream.toByteArray());
    }

    public void close() {
        if(m_in!=null){
            try {
                m_in.close();
            } catch (IOException e) {
                 // okay.
            }
        }
        if (m_file != null)
            m_file.delete();
        if (m_buff != null)
            m_buff = null;
    }

}
