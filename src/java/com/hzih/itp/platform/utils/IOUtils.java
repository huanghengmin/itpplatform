package com.hzih.itp.platform.utils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2007-6-20
 * Time: 20:22:01
 * To change this template use File | Settings | File Templates.
 */
public class IOUtils {
    public static final int I_BuffSize = 100 * 1024;

    public static byte[] toByteArray(InputStream in) throws IOException {
        final int oneMB = I_BuffSize;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BufferedOutputStream buff = new BufferedOutputStream(out);

        int bytesRead = 0;
        int bytesavailable = in.available();
        if (bytesavailable > oneMB) {
            bytesavailable = oneMB;
        }
        if (bytesavailable == 0) {
            bytesavailable = 1;
        }
        byte[] tempBuf = new byte[I_BuffSize];
        bytesRead = in.read(tempBuf);
        while (bytesRead > 0) {
            buff.write(tempBuf, 0, bytesRead);
            buff.flush();
            bytesavailable = in.available();
            if (bytesavailable > oneMB) {
                bytesavailable = oneMB;
            }
            if (bytesavailable == 0) {
                bytesavailable = 1;
            }
            bytesRead = in.read(tempBuf);
        }
        buff.flush();
        out.flush();
        tempBuf = out.toByteArray();
        buff.close();
        out.close();
        return tempBuf;
    }

    public static byte[] readByteArray(InputStream in, int size) throws IOException {
        int oneMB = size;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BufferedOutputStream buff = new BufferedOutputStream(out);

        int bytesRead = 0;
        int bytesavailable = in.available();
        if (bytesavailable > oneMB) {
            bytesavailable = oneMB;
        }
        if (bytesavailable == 0) {
            bytesavailable = 1;
        }
        byte[] tempBuf = new byte[bytesavailable];
        bytesRead = in.read(tempBuf);
        if (bytesRead > 0) {
            buff.write(tempBuf, 0, bytesRead);
            buff.flush();
            out.flush();
        }
        tempBuf = out.toByteArray();
        buff.close();
        out.close();
        return tempBuf;
    }

    public static byte[] readByteArray(InputStream in) throws IOException {
        return readByteArray(in, I_BuffSize);
    }

    public static byte[] readByteArrayNoAvailabel(InputStream in, int size) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BufferedOutputStream buff = new BufferedOutputStream(out);
        byte[] tempBuf = new byte[size];
        int byteRead = 0;
        byteRead = in.read(tempBuf);
        if (byteRead > 0) {
            buff.write(tempBuf, 0, byteRead);
            buff.flush();
            out.flush();
        }
        tempBuf = out.toByteArray();
        buff.close();
        out.close();
        return tempBuf;
    }

    public static byte[] readByteArrayNoAvailabel(InputStream in) throws IOException {
        return readByteArrayNoAvailabel(in, I_BuffSize);
    }
}
