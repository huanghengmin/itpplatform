/*=============================================================
 * 文件名称: MDataParseImp.java
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

import com.hzih.itp.platform.utils.HeaderProcess;
import com.inetec.common.exception.Ex;
import com.inetec.common.exception.E;
import com.inetec.common.i18n.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class MDataParseImp {


    public final static Logger m_logger = LoggerFactory.getLogger(MDataParseImp.class);

    private String fileName = "";
    private long totalFileLength = 0;
    private long normalDataPosition = 0;

    private DataInformation basicDataInformation = null;
    private ArrayList blobDataInfos = new ArrayList();
    private ArrayList clobDataInfos = new ArrayList();
    private ArrayList successDataInfos = new ArrayList();
    private DataHeaderAttributer m_header = null;
    public static long I_lobDataSize = 4 * 1024 * 1024;
//    public static long I_lobDataSize = 10 * 1024 * 1024;

    public MDataParseImp(String fileName) throws Ex {
        this.fileName = fileName;
        try {
            FileInputStream in = new FileInputStream(fileName);
            parseHeader(in);
        } catch (FileNotFoundException e) {
            throw new Ex().set(E.E_FileNotFound, new Message("Data File not found."));
//        } catch (IOException e) {
//            throw new Ex().set(E.E_IOException, new Message("Data IOException. "));
        }
    }

    public MDataParseImp(InputStream in) throws Ex {
        parseHeader(in);
    }

    public boolean isRecover() {
        return successDataInfos.size() > 0;
    }

    public DataInformation getBasicDataInfo() {
        return basicDataInformation;
    }

    public DataInformation[] getBlobDataInfo() {
        return (DataInformation[]) blobDataInfos.toArray(new DataInformation[0]);
    }

    public DataInformation[] getClobDataInfo() {
        return (DataInformation[]) clobDataInfos.toArray(new DataInformation[0]);
    }

    public DataInformation[] getSuccessDataInfo() {
        return (DataInformation[]) successDataInfos.toArray(new DataInformation[0]);
    }


    public void failedToConsumer(String targetSchema, int errorCode, String errorMessage, Throwable cause) throws Ex {

        //noting
    }


    public void succeedToConsumer(String targetSchema) throws Ex {
        if (fileName == null) {
            return;
        }
        SuccessData rData = new SuccessData(targetSchema);

        appendSuccessData(fileName, rData);
        rData.close();
    }


    /*private void appendRecoverData(String nowfileName, SuccessData rd) throws Ex {

        // 在原始文件的尾部插入处理失败的信息，中间空两行
        OutputStream fosWithAppend = null;
        try {
            fosWithAppend = new FileOutputStream(nowfileName, true);
            MDataUtil.appendWithSeperator(fosWithAppend, rd.getDataStream());
            fosWithAppend.close();
        } catch (Exception eEx) {
            throw new Ex().set(eEx);
        } finally {
            if (fosWithAppend != null) {
                try {
                    fosWithAppend.close();
                } catch (IOException e) {
                    throw new Ex().set(E.E_IOException, new Message("append SuccessData is faild."));
                }
            }
        }

    }*/
    private void appendSuccessData(String nowfileName, SuccessData rd) throws Ex {

        // 在原始文件的尾部插入处理失败的信息，中间空两行
        OutputStream fosWithAppend = null;
        try {
            fosWithAppend = new FileOutputStream(nowfileName, true);
            MDataUtil.appendWithSeperator(fosWithAppend, rd.getDataStream());
            fosWithAppend.close();
        } catch (Exception eEx) {
            throw new Ex().set(eEx);
        } finally {
            if (fosWithAppend != null) {
                try {
                    fosWithAppend.close();
                } catch (IOException e) {
                    throw new Ex().set(E.E_IOException, new Message("append SuccessData is faild."));
                }
            }
        }

    }

    private void parseHeader(InputStream in) throws Ex {

        if (m_logger.isDebugEnabled()) {
            try {
                m_logger.debug("Data Stream length:" + in.available());
            } catch (IOException e) {
                throw new Ex().set(E.E_IOException, e);
            }

        }
        m_header = new DataHeaderAttributer();
        Properties properties = new HeaderProcess().parseHeader(in);
        m_header.load(properties);
        if (m_logger.isDebugEnabled()) {
            try {
                m_logger.debug("read header Data Stream length:" + in.available());
            } catch (IOException e) {
                throw new Ex().set(E.E_IOException, e);
            }

        }
        int length = 0;
        try {
            basicDataInformation = new DataInformation(DataInformation.readInputStream(in, m_header.getBasicLength()), (long) m_header.getBasicLength());
            length = in.available();
        } catch (IOException e) {
            throw new Ex().set(E.E_IOException, new Message("Data is IOException."));
        }
        if (length > 0) {
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("Lob Data Stream length:" + length);
            }
            long loblength = 0;

            while (m_header.isNext()) {
                DataInformation dataInformation = null;
                try {
                    in.read();
                    in.read();
                    loblength = m_header.getLobDataLength();
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("Lob Data Stream length:" + length);
                    }

                    dataInformation = new DataInformation(DataInformation.readInputStream(in, loblength), loblength);
                    if(dataInformation.getDateType()==null) {
                        continue;
                    }
                    if (m_logger.isDebugEnabled()) {

                        m_logger.debug("DataType is:" + dataInformation.getDateType());
                    }
                    if (dataInformation.isBlobData()) {
                        blobDataInfos.add(dataInformation);
                    } else if (dataInformation.isClobData()) {
                        clobDataInfos.add(dataInformation);
                    } else if (dataInformation.isSuccessData()) {
                        successDataInfos.add(dataInformation);
                    }
                } catch (IOException e) {
                    throw new Ex().set(E.E_IOException, e, new Message("Data is IOException."));
                }
            }
        }
        try {
            if (in != null)
                in.close();
        } catch (IOException e) {
            throw new Ex().set(E.E_IOException, e, new Message("Data is IOException."));
        }
    }

    public void close() {
        basicDataInformation.close();
        for (int i = 0; i < blobDataInfos.size(); i++) {
            DataInformation dataInformation = (DataInformation) blobDataInfos.get(i);
            dataInformation.close();
        }
        for (int i = 0; i < clobDataInfos.size(); i++) {
            DataInformation dataInformation = (DataInformation) clobDataInfos.get(i);
            dataInformation.close();
        }
    }
}
