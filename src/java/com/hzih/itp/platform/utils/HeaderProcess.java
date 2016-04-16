package com.hzih.itp.platform.utils;

import com.inetec.common.exception.E;
import com.inetec.common.exception.Ex;
import com.inetec.common.i18n.Message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

/**
 * 数据封装的公共数据。 功能完成数据控制信息，转化为控制数据和根据数据流读取控信息。
 *
 * @author inetec System
 * @version 1.0
 */
public class HeaderProcess {
    /**
     * 成员变量：存放控制参数。（Properties对象）
     */
    Properties properties;

    /**
     * 类构造器
     */
    public HeaderProcess() {
        properties = new Properties();
    }

    /**
     * 类构造器，指定的 Properties（对象）
     *
     * @param fp
     */
    public HeaderProcess(Properties fp) {
        properties = fp;
    }

    /**
     * 取得当前Properties对象的序列化对象流。
     *
     * @return 数据流。
     */
    public InputStream getInputStream() {
        StringBuffer result = new StringBuffer();
        Set keySet = properties.keySet();
        Iterator it = keySet.iterator();
        while (it.hasNext()) {
            String name = (String) it.next();
            String value = (String) properties.get(name);
            result.append(name);
            result.append('=');
            result.append(value);
            result.append('\n');
        }
        result.append('\n');
        byte[] bs = result.toString().getBytes();
        return new ByteArrayInputStream(bs);
    }

    /**
     * 解析数据流，读取相应的控制信息。
     *
     * @param is 要解析数据流。
     * @return 控制信息。
     * @throws com.inetec.common.exception.Ex 如果读取不成功，返回IOException 异常。
     */
    public Properties parseHeader(InputStream is) throws Ex {
        byte[] bs = new byte[8092];
        int index = 0;
        String name = "";
        String value = "";
        try {
            byte b = (byte) is.read();
            byte last = 0;
            while (b != -1) {
                if (b == '=') {
                    name = new String(bs, 0, index);
                    bs = new byte[8092];
                    index = 0;
                } else if (b == '\n') {
                    if (last == '\n') {
                        break;
                    }
                    value = new String(bs, 0, index);
                    properties.setProperty(name, value);
                    bs = new byte[8092];
                    index = 0;
                } else {
                    bs[index++] = b;
                }
                last = b;
                b = (byte) is.read();
            }
        } catch (IOException io) {
            throw new Ex().set(E.E_IOException, io);
        }
        return properties;
    }

    /**
     * 取得的当前 Properties 对象的长度。包括自定义的头部信息。
     *
     * @param header 头部信息
     * @return 长度（整型）
     * @throws com.inetec.common.exception.Ex 如果序列化不成功，返回IOException 异常。
     */
    public int getLength(String header) throws Ex {
        int result = 0;
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            properties.store(os, header);
            result = os.size();
        } catch (IOException e) {
            throw new Ex().set(E.E_IOException, e, new Message("Get Length error."));
        }
        return result;
    }

}

