/*=============================================================
 * 文件名称: MDataConstructor.java
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

import com.hzih.itp.platform.dbchange.datautils.db.Rows;
import com.hzih.itp.platform.dbchange.datautils.db.pk.PkSet;
import com.hzih.itp.platform.utils.DataAttributes;
import com.hzih.logback.LogLayout;
import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;

import java.io.*;
import java.util.Properties;
import java.util.Enumeration;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MDataConstructor {

    public final static Logger logger = LoggerFactory.getLogger(MDataConstructor.class);

    private ArrayListInputStream mis = new ArrayListInputStream();
    private Rows m_basRows = null;
    private DefaultData m_defaultDate = null;
    private long m_position = 0;
    private DataHeaderAttributer m_header = new DataHeaderAttributer();
    private boolean m_bIsHeader = false;

    public MDataConstructor() {
        m_defaultDate = new DefaultData(new Properties());
    }

    public MDataConstructor(DataHeaderAttributer header, ArrayListInputStream in, Rows rows) {
        //m_header = header;
        mis = in;
        m_defaultDate = new DefaultData(new Properties());
        m_basRows = rows;
        m_bIsHeader = true;
    }

    public void setBasicData(String schemaName, String tableName, Rows basic) throws Ex {
        setBasicData(schemaName, tableName, basic, new Properties());
    }

    public void setBasicData(String schemaName, String tableName, Rows basic, Properties props) throws Ex {
        m_basRows = basic;
        m_defaultDate = new DefaultData();
        // add heads
        m_defaultDate.setSchemaName(schemaName);
        m_defaultDate.setTableName(tableName);
        m_defaultDate.setHeadValue(DataInformation.Str_DataType, DataInformation.Str_DataType_Basic);
        //add headers Properps
        Enumeration keys = props.keys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            m_defaultDate.setHeadValue(key, props.getProperty(key));
        }

        // add content
        InputStream is = basic.getDataInputStream();
        try {
            int basicLen = is.available();
            m_defaultDate.setContentStream(is, basicLen);
            m_position = m_defaultDate.getHeaderLength() + basicLen;
            m_header.setBasicLength(m_defaultDate.getHeaderLength() + basicLen);
        } catch (IOException ioEEx) {
            throw new Ex().set(ioEEx);
        }

    }

    public void addClobData(String schemaName, String tableName, String columnName,
                            String encoding, PkSet pks, Reader reader, long clobLen) throws Ex {
        if (reader != null) {
            ReaderInputStream inReader = new ReaderInputStream(reader, encoding);
            mis.addInputStream(getTwoLineInputStream());
            try {
                m_position = m_defaultDate.getDataStream().available() + mis.available();
            } catch (IOException e) {
                throw new Ex().set(E.E_IOException, new Message("Test Data Length error."));
            }

            CharLargeObjectData charLargeObjectData = new CharLargeObjectData();
            // add heads
            charLargeObjectData.setSchemaName(schemaName);
            charLargeObjectData.setTableName(tableName);
            charLargeObjectData.setHeadValue(DataInformation.Str_DataType, DataInformation.Str_DataType_Clob);
            charLargeObjectData.setHeadValue(DataInformation.Str_FieldName, columnName);
            charLargeObjectData.setHeadValue(DataInformation.Str_Data_Length, clobLen + "");
            charLargeObjectData.setHeadValue(DataInformation.Str_CharSet, encoding);
            charLargeObjectData.setHeadValue(DataInformation.Str_Pks, pks.getPkString());

            // add pks
            // charLargeObjectData.setPks(pks);

            // clob content
            DataAttributes temp = createStream(new ReaderInputStream(reader, DataInformation.Str_CharacterSet));
            clobLen = new Long(temp.getValue(DataAttributes.Str_FileSize)).longValue();
            charLargeObjectData.setContentStream(temp.getResultData(), clobLen);
            // charLargeObjectData.setContentStream(new ReaderInputStream(reader, "unicode"), clobLen);
            try {
                mis.addInputStream(charLargeObjectData.getDataStream());
                m_header.addLobHeader(m_position, clobLen + charLargeObjectData.getHeaderLength());
            } catch (IOException ioEEx) {
                throw new Ex().set(ioEEx);
            }
        }
    }

    public void addBlobData(String schemaName, String tableName, String columnName,
                            PkSet pks, InputStream is, long blobLen) throws Ex {
        if (is != null) {
            mis.addInputStream(getTwoLineInputStream());


            if (pks == null) {
                LogLayout.debug(logger,"pkset==null");
            }
            try {
                m_position = m_defaultDate.getDataStream().available() + mis.available();
            } catch (IOException e) {
                throw new Ex().set(E.E_IOException, new Message("Test Data Length error."));
            }
            ByteLargeObjectData byteLargeObjectData = new ByteLargeObjectData();
            // add heads
            byteLargeObjectData.setSchemaName(schemaName);
            byteLargeObjectData.setTableName(tableName);
            byteLargeObjectData.setHeadValue(DataInformation.Str_DataType, DataInformation.Str_DataType_Blob);
            byteLargeObjectData.setHeadValue(DataInformation.Str_FieldName, columnName);
            byteLargeObjectData.setHeadValue(DataInformation.Str_Data_Length, blobLen + "");
            byteLargeObjectData.setHeadValue(DataInformation.Str_Pks, pks.getPkString());
            byteLargeObjectData.setContentStream(is, blobLen);
            try {
                mis.addInputStream(byteLargeObjectData.getDataStream());
                m_header.addLobHeader(m_position, blobLen + byteLargeObjectData.getHeaderLength());
            } catch (IOException ioEEx) {
                throw new Ex().set(ioEEx);
            }
        }
    }

    public InputStream getDataInputStream() throws Ex {
        ArrayListInputStream is = new ArrayListInputStream();
        try {

            is.addInputStream(m_header.headerToStream());
            if (!m_bIsHeader)
                addMutilDataVersion(is);
            //is.addInputStream(getTwoLineInputStream());
            if (m_defaultDate.getDataStream() != null)
                is.addInputStream(m_defaultDate.getDataStream());
            is.addInputStream(mis);
        } catch (IOException e) {
            throw new Ex().set(E.E_IOException, e, new Message("DBChange DataConstructor IOException."));
        }

        return is;
    }

    public InputStream getDataWhitoutBaseInputStream() throws Ex {

        return mis;
    }


    public DataHeaderAttributer getHeader() {
        return m_header;
    }

    public void updateHeader(DataHeaderAttributer header) {
        m_header.updateHeader(header);
    }

    private void addMutilDataVersion(ArrayListInputStream is) {
        StringBuffer version = new StringBuffer();
        version.append(MDataUtil.constructMultiDataVersion());
        version.append(MDataUtil.constructEmptyLine());
        version.append(MDataUtil.constructEmptyLine());
        StringReader versionReader = new StringReader(version.toString());
        ReaderInputStream versionIs = new ReaderInputStream(versionReader, DataInformation.Str_CharacterSet);
        is.addInputStream(versionIs);
    }


    private InputStream getTwoLineInputStream() throws Ex {
        String s = MDataUtil.Str_LineSeperator + MDataUtil.Str_LineSeperator;
        try {
            return new ByteArrayInputStream(s.getBytes(DataInformation.Str_CharacterSet));
        } catch (UnsupportedEncodingException e) {
            throw new Ex().set(e);
        }
    }

    public Rows getData() throws Ex {
        if (m_basRows == null) {
            throw new Ex().set(E.E_InvalidObject, new Message("Basic data is invalid."));
        }
        return m_basRows;
    }


    private DataAttributes createStream(InputStream is) throws Ex {
        try {
            ByteArrayOutputStream gzip = new ByteArrayOutputStream();
            /*final int ONEMB = 1 * 1024 * 1024;
            int length = 0;
            int bytesAvailable = is.available();
            if (bytesAvailable > ONEMB) {
                bytesAvailable = ONEMB;
            }
            if (bytesAvailable == 0) {
                bytesAvailable = 1;
            }
            byte[] tempBuf = new byte[bytesAvailable];
            int bytesRead = -1;

            // Now, just read a chunk at a time and send over the wire.
            bytesRead = is.read(tempBuf);

            while (bytesRead != -1) {
                length = length + bytesRead;
                gzip.write(tempBuf, 0, bytesRead);
                bytesAvailable = is.available();
                if (bytesAvailable > ONEMB) {
                    bytesAvailable = ONEMB;
                }
                if (bytesAvailable == 0) {
                    bytesAvailable = 1;
                }
                tempBuf = new byte[bytesAvailable];
                bytesRead = is.read(tempBuf);
            }
            tempBuf = null;*/
            IOUtils.copy(is,gzip);
            gzip.flush();
            DataAttributes result = new DataAttributes();
            result.putValue(DataAttributes.Str_FileSize, String.valueOf(gzip.size()));
            result.setResultData(gzip.toByteArray());
            gzip.close();
            return result;
        } catch (IOException Ex) {
            LogLayout.error(logger,"platform","IOException caught while creating  stream.", Ex);
            throw new Ex().set(E.E_OperationFailed, Ex, new Message("IOException caught while creating  stream."));
        }
    }

}
