package com.hzih.itp.platform.utils;

import com.inetec.common.i18n.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2007-6-19
 * Time: 15:58:33
 * To change this template use File | Settings | File Templates.
 */
public class ByteArrayBufferedInputStream extends InputStream {
    private static Logger logger = LoggerFactory.getLogger(ByteArrayBufferedInputStream.class);
    public static final int I_BuffMaxSize = 10 * 1024 * 1024;
    protected InputStream m_in = null;
    protected ByteArrayOutputStream m_buff = null;
    protected File m_file = null;

    public int getSize() {
        return size;
    }

    private int size = 0;
    private int maxsize = I_BuffMaxSize;

    public ByteArrayBufferedInputStream(int maxsize) {
        this.maxsize = maxsize;
    }

    public ByteArrayBufferedInputStream() {
    }

    public void write(byte[] buff) throws IOException {
        size = size + buff.length;
        if (size > maxsize) {
            oscach(buff);
        } else {
            cach(buff);
        }
    }

    public void write(byte[] buff, int postion, int length) throws IOException {
        size = size + length;
        if (length > buff.length) {
            length = buff.length;
        }
        if (length - postion > buff.length) {
            length = length - postion;
        }
        if (postion > length) {
            postion = length;
        }
        if (size > maxsize) {

            oscach(buff, postion, length);
        } else {
            cach(buff, postion, length);
        }
    }

    public void write(byte[] buff, int length) throws IOException {
        size = size + length;
        if (size > maxsize) {
            oscach(buff, 0, length);
        } else {
            cach(buff, 0, length);
        }
    }

    public void flush() throws IOException {
        if (m_in != null) {
            m_in.close();
            m_in = null;
        }
        m_in = getContentStream();
    }

    private InputStream getContentStream() throws IOException {
        InputStream result = null;
        if (m_buff != null) {
            result = new ByteArrayInputStream(m_buff.toByteArray());
            try {
                m_buff.close();
            } catch (IOException e) {
                //okay
            }
            m_buff = null;
        }
        if (m_file != null) {
            try {
                result = new FileInputStream(m_file);
            } catch (FileNotFoundException e) {
                throw new IOException(new Message("数据暂存读取失败.").toString());
                //throw new Ex().set(E.E_FileNotFound, new Message("数据暂存读取失败."));
            }
        }
        return result;
    }

    private void cach(byte[] buff, int postion, int length) throws IOException {

        try {

            if (m_buff == null) {
                m_buff = new ByteArrayOutputStream();
                if (m_in != null) {
                    m_buff.write(IOUtils.toByteArray(m_in));
                    m_buff.flush();

                    m_in.close();
                    m_in = null;
                }
            }
            m_buff.write(buff, postion, length);
            m_buff.flush();

        } catch (IOException e) {
            throw e;
        }
    }

    public void oscach(byte[] buff, int postion, int length) throws IOException {
        boolean isNoFile = false;
        FileOutputStream out = null;
        try {
            if (m_file == null) {
                m_file = File.createTempFile("#bytearray-buffered", ".tmp");
                isNoFile = true;
            }
            out = new FileOutputStream(m_file, true);
            if (isNoFile) {
                if (m_buff != null) {
                    out.write(m_buff.toByteArray());
                    out.flush();
                    m_buff.close();
                    m_buff = null;
                } else {
                    if (m_in != null) {
                        out.write(IOUtils.toByteArray(m_in));
                        out.flush();
                        m_in.close();
                        m_in = null;
                    }
                }

            }
            out.write(buff, postion, length);
            out.flush();

        } catch (IOException e) {
            throw e;
        } finally {
            if (out != null)
                out.close();
        }
    }

    private void cach(byte[] buff) throws IOException {

        try {

            if (m_buff == null) {
                m_buff = new ByteArrayOutputStream();
                if (m_in != null) {
                    m_buff.write(IOUtils.toByteArray(m_in));
                    m_buff.flush();
                    m_in.close();
                    m_in = null;
                }
            }
            m_buff.write(buff);
            m_buff.flush();

        } catch (IOException e) {
            throw e;
        }
    }

    public void oscach(byte[] buff) throws IOException {
        boolean isNoFile = false;
        FileOutputStream out = null;
        try {
            if (m_file == null) {
                m_file = File.createTempFile("#bytearray-buffered", ".tmp");
                isNoFile = true;
            }
            out = new FileOutputStream(m_file, true);
            if (isNoFile) {
                if (m_buff != null) {
                    out.write(m_buff.toByteArray());
                    out.flush();
                    m_buff.close();
                    m_buff = null;
                } else {
                    if (m_in != null) {
                        out.write(IOUtils.toByteArray(m_in));
                        out.flush();
                        m_in.close();
                        m_in = null;
                    }
                }
            }
            out.write(buff);
            out.flush();

        } catch (IOException e) {
            throw e;
        } finally {
            out.close();
        }
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
        if (m_in != null) {
            return m_in.read();
        }
        return 0;

    }

    public synchronized int read(byte b[]) throws IOException {
        if (m_in != null) {
            return m_in.read(b);
        }
        return 0;
    }

    public synchronized int available() throws IOException {
        if (m_in != null) {
            return m_in.available();
        }
        return 0;
    }

    public void close() {
        if (m_in != null) {
            try {
                m_in.close();
            } catch (IOException e) {
                // okay.
            }
        }
        if (m_file != null) {
            m_file.delete();
            m_file = null;
        }
        if (m_buff != null) {
            try {
                m_buff.close();
            } catch (IOException e) {
                //okay
            }
            m_buff = null;
        }

    }
}