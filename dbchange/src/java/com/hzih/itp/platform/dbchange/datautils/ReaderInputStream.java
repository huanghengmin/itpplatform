/*=============================================================
 * 文件名称: ReaderInputStream.java
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

import java.io.IOException;
import java.io.Reader;
import java.io.InputStream;

public class ReaderInputStream extends InputStream {

    protected Reader reader;
    private static final int CHAR_BUF_SIZE = 1024;
    private String charset = "";
    private char[] charBuffer = new char[CHAR_BUF_SIZE];
    private byte[] byteBuffer;
    private int byteBufferIndeEx = 0;
    private int byteBufferbufEnd = 0;

    private int total = 0;

    public ReaderInputStream(Reader reader) {
        this.reader = reader;
    }

    public ReaderInputStream(Reader reader, String charset) {
        this.reader = reader;
        this.charset = charset;
    }

    /**
     * Reads the next byte of data from this source stream. The value
     * byte is returned as an <code>int</code> in the range
     * <code>0</code> to <code>255</code>. If no byte is available
     * because the end of the stream has been reached, the value
     * <code>-1</code> is returned.
     * <p/>
     * This <code>read</code> method
     * cannot block.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the
     *         stream has been reached.
     */
    public synchronized int read() throws IOException {
        if (byteBufferIndeEx == byteBufferbufEnd) {
            byteBufferIndeEx = 0;
            int charBufferbufEnd = reader.read(charBuffer, 0, charBuffer.length);
            if (charBufferbufEnd < 0) {
                byteBufferbufEnd = 0;
                return -1;
            } else {
                String stringBuffer = new String(charBuffer, 0, charBufferbufEnd);
                if (charset == null || charset.equals("")) {
                    byteBuffer = stringBuffer.getBytes();
                } else {
                    byteBuffer = stringBuffer.getBytes(charset);
                }
                byteBufferbufEnd = byteBuffer.length;
            }

            total += byteBufferbufEnd;

            /*
            if (m_logger.isDebugEnabled()) {
                m_logger.debug("ReaderInputStream total readed:" + total);
            }
            */
        }

        return byteBuffer[byteBufferIndeEx++];
    }

}

