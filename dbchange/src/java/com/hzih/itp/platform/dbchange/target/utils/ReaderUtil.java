package com.hzih.itp.platform.dbchange.target.utils;

import com.hzih.itp.platform.dbchange.datautils.ReaderInputStream;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-12-7
 * Time: 22:30:25
 * To change this template use File | Settings | File Templates.
 */
public class ReaderUtil {
    private Reader m_reader = null;
    private long m_length = 0;

    public ReaderUtil(Reader reader) throws IOException {
        m_reader = reader;
        initReader();
    }
    public Reader getReader() {
        return m_reader;
    }

    public long getLength() {
        return m_length;
    }

    private void init(String charset) throws IOException {
        ReaderInputStream inputStream = new ReaderInputStream(m_reader);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int bufflength = 0;
        byte[] bytebuff = new byte[1024];
        int rc = 0;
        rc = inputStream.read(bytebuff);
        while (rc > 0) {
            out.write(bytebuff,0,rc);
            bytebuff = new byte[1024];
            rc = inputStream.read(bytebuff);
        }
        out.flush();
        m_length = out.toByteArray().length;
        m_reader = new InputStreamReader(new ByteArrayInputStream(out.toByteArray()), charset);
        out.close();
    }

    private void init() throws IOException {
        ReaderInputStream inputStream = new ReaderInputStream(m_reader);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int bufflength = 0;
        byte[] bytebuff = new byte[1024];
        int rc = 0;
        rc = inputStream.read(bytebuff);
        while (rc > 0) {
            out.write(bytebuff,0,rc);
            bytebuff = new byte[1024];
            rc = inputStream.read(bytebuff);
        }
        out.flush();
        m_length = out.toByteArray().length;
        m_reader = new InputStreamReader(new ByteArrayInputStream(out.toByteArray()));
        out.close();
    }

    private void initReader() throws IOException {
        CharArrayWriter out =new CharArrayWriter();

        int bufflength = 0;
        char[] bytebuff = new char[1024];
        int rc = 0;
        rc = m_reader.read(bytebuff);
        while (rc > 0) {
            out.write(bytebuff,0,rc);
            rc = m_reader.read(bytebuff);
        }
        out.flush();
        m_length = out.toCharArray().length;
        m_reader = new CharArrayReader(out.toCharArray());
        out.close();
    }
}
