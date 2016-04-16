package com.hzih.itp.platform.stpchange.source.plugin.http;

import com.google.protobuf.ByteString;
import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.config.mina.UdpClientImpl;
import com.hzih.itp.platform.config.mina.code.RequestMessage;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import com.inetec.common.io.IOUtils;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: sunny
 * Date: 14-1-11
 * Time: 下午4:24
 */
public class FileReceiveServlet extends HttpServlet {
    final static Logger logger = LoggerFactory.getLogger(FileReceiveServlet.class);
    private UdpClientImpl udpClient;
    private String appName;
    private String charset;

    public FileReceiveServlet() {
    }

    public FileReceiveServlet(String appName,String charset) {
        this.appName = appName;
        this.charset = charset;
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>hello jetty context is ok!</h1>");
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/html;charset=utf-8");
        String fileName = request.getHeader("fileName");
        fileName = new String(new BASE64Decoder().decodeBuffer(fileName));
        long fileSize = Long.parseLong(request.getHeader("fileSize"));
        FileItemFactory factory = new DiskFileItemFactory();
        ServletFileUpload upload = new ServletFileUpload(factory);
        List<FileItem> items = new ArrayList<FileItem>();
        InputStream is = null;
        try {
            items = upload.parseRequest(request);
            // 得到所有的文件
            Iterator<FileItem> it = items.iterator();
            while (it.hasNext()) {
                FileItem fItem = (FileItem) it.next();
                String fName = "";
                if (fItem.isFormField()) { // 普通文本框的值
                } else { // 获取上传文件的值
                    is = fItem.getInputStream();
                }
            }
            send(is,fileName,fileSize);
//            testWrite(is,fileName);
        } catch (Exception e) {
            LogLayout.error(logger,appName,"接收文件和发送文件错误",e);
        } finally {
            if(is != null) {
                is.close();
            }
            if(udpClient != null){
                ChangeConfig.returnUdpConnection(appName,udpClient);
            }
        }
        response.setStatus(HttpServletResponse.SC_OK);
    }

    private void testWrite(InputStream is, String fileName) {
        int len = 0;
        byte[] buff = new byte[StaticField.MB * 2];
        try {
            File file = new File("F:/itp/test/"+appName + fileName.substring(0,fileName.lastIndexOf("\\")));
            if(!file.exists()){
                file.mkdirs();
            }
            FileOutputStream out = new FileOutputStream("F:/itp/test/"+appName + fileName,true);
            while ((len = is.read(buff)) != -1) {
                out.write(buff,0,len);
            }
            out.flush();
            out.close();
        } catch (FileNotFoundException e) {
            LogLayout.error(logger,appName,"找不到文件",e);
        } catch (IOException e) {
            LogLayout.error(logger,appName,"测试时接收文件和写文件错误",e);
        }
    }

    private void send_old(InputStream is, String tempFileName, long tempFileSize) throws IOException {
        int len = 0;
        udpClient = ChangeConfig.getUdpDataConnection(appName);
        boolean isFirstPacket = true;
        int allLen = 0;
        int bodyLength = 0;
        byte[] buff = new byte[StaticField.MB * 2];
        while ((len = is.read(buff)) != -1) {
            RequestMessage message = new RequestMessage();
            if(len < StaticField.UDPPacketSizeBodyMax) {
                buff = IOUtils.copyArray(buff, len);
            }
            bodyLength = StaticField.UDPPacketSizeFileName + len;

            byte[] nameBuf = tempFileName.getBytes();
            byte[] sendBuff = new byte[bodyLength];
            if(nameBuf.length!=StaticField.UDPPacketSizeFileName){
                LogLayout.warn(logger, appName, "file length is not " + StaticField.UDPPacketSizeFileName);
            }
            System.arraycopy(nameBuf,0,sendBuff,0,StaticField.UDPPacketSizeFileName);//把nameBuf复制到sendBuff,重sendBuff的第0位到第StaticField.UDPPacketSizeFileName位
            System.arraycopy(buff,0,sendBuff,StaticField.UDPPacketSizeFileName,len);//把buff复制到sendBuff,重sendBuff的第StaticField.UDPPacketSizeFileName位到第len位
            ByteString msgBody = ByteString.copyFrom(sendBuff);
            message.setMsgBody(msgBody);
            message.setMsgLen(bodyLength);
            allLen += len;
            if(isFirstPacket){
                if(tempFileSize == len) {
                    message.setProtocolType(RequestMessage.ProtocolTypeOnly);
                } else if(tempFileSize > len) {
                    message.setProtocolType(RequestMessage.ProtocolTypeStart);
                }
                isFirstPacket = false;
            } else {
                if(tempFileSize==(long)allLen) {
                    message.setProtocolType(RequestMessage.ProtocolTypeEnd);
                } else {
                    message.setProtocolType(RequestMessage.ProtocolType);
                }
            }
            udpClient.send(message);
        }
    }
    private void send(InputStream is, String tempFileName, long tempFileSize) throws IOException {
        int len = 0;
        udpClient = ChangeConfig.getUdpDataConnection(appName);
        boolean isFirstPacket = true;
        int allLen = 0;
        int bodyLength = 0;
        byte[] nameBuf = tempFileName.getBytes();
        int nameLength = nameBuf.length;
        int bodyMaxLength = StaticField.UDPPacketSizeMax - nameLength -1;
        byte[] buff = new byte[bodyMaxLength];
        while ((len = is.read(buff)) != -1) {
            RequestMessage message = new RequestMessage();
            if( len < bodyMaxLength ) {
                buff = IOUtils.copyArray(buff, len);
            }
            bodyLength = len + nameLength + 1;
            byte[] sendBuff = new byte[bodyLength];


//            if(nameBuf.length!=StaticField.UDPPacketSizeFileName){
//                LogLayout.warn(logger, appName, "file length is not " + StaticField.UDPPacketSizeFileName);
//            }
            sendBuff[0] = (byte) nameLength;
            System.arraycopy(nameBuf,0,sendBuff,1,nameLength);//把nameBuf复制到sendBuff,从sendBuff的第1位到第nameLength位
            System.arraycopy(buff,0,sendBuff,nameLength+1,len);//把buff复制到sendBuff,从sendBuff的第(nameLength+1)位到第len位
            ByteString msgBody = ByteString.copyFrom(sendBuff);
            message.setMsgBody(msgBody);
            message.setMsgLen(bodyLength);
            allLen += len;
            if(isFirstPacket){
                if(tempFileSize == len) {
                    message.setProtocolType(RequestMessage.ProtocolTypeOnly);
                } else if(tempFileSize > len) {
                    message.setProtocolType(RequestMessage.ProtocolTypeStart);
                }
                isFirstPacket = false;
            } else {
                if(tempFileSize==(long)allLen) {
                    message.setProtocolType(RequestMessage.ProtocolTypeEnd);
                } else {
                    message.setProtocolType(RequestMessage.ProtocolType);
                }
            }
            udpClient.send(message);
        }
    }
}
