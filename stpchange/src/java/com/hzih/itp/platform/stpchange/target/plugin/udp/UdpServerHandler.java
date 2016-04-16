package com.hzih.itp.platform.stpchange.target.plugin.udp;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.config.mina.code.RequestMessage;
import com.hzih.itp.platform.stpchange.target.plugin.TargetProcessUdp;
import com.hzih.logback.LogLayout;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: 钱晓盼
 * Date: 12-12-9
 * Time: 下午6:21
 * To change this template use File | Settings | File Templates.
 */
public class UdpServerHandler extends IoHandlerAdapter {
    static final Logger logger = LoggerFactory.getLogger(UdpServerHandler.class);
    
    private TargetProcessUdp targetProcessUdp;
    private String appName;

    public UdpServerHandler() {

    }
    public void setReceiveService(TargetProcessUdp targetProcessUdp) {
        this.targetProcessUdp = targetProcessUdp;
        this.appName = targetProcessUdp.getAppName();

    }

    /**
     * 抛出异常触发的事件
     * @param session
     * @param cause
     * @throws Exception
     */
    public void exceptionCaught(IoSession session, Throwable cause)
            throws Exception {
        cause.printStackTrace();
        session.close(true);
    }

    /**
     * 连接关闭触发的事件
     * @param session
     * @throws Exception
     */
    public void sessionClosed(IoSession session) throws Exception {
//        LogLayout.info(logger,"platform","Session closed...");
    }

    /**
     * 建立连接触发的事件
     * @param session
     * @throws Exception
     */
    public void sessionCreated(IoSession session) throws Exception {
//        LogLayout.info(logger,"platform","Session created...");
    }

    /**
     * 声明这里message必须为IoBuffer类型
     * @param session
     * @param message
     * @throws Exception
     */
    public void messageReceived(IoSession session, Object message) {
        if(message instanceof RequestMessage) {
            RequestMessage requestMessage = (RequestMessage) message;
//            System.out.println("-------------------");
            httpReceive(requestMessage);
        }
    }

    private void httpReceive(RequestMessage requestMessage) {
        byte[] tempBuff = requestMessage.getMsgBody().toByteArray();
        int nameLength = tempBuff[0];
        byte[] nameBuff = Arrays.copyOfRange(tempBuff, 1, nameLength+1);
        byte[] bodyBuff = Arrays.copyOfRange(tempBuff,nameBuff.length+1,tempBuff.length);  //具体内容
        String tempFileName = new String(nameBuff);//临时文件名
        String dir = doBeforeReceive(tempFileName);
        String rFilePath;
        if(tempFileName.startsWith("/")){
            rFilePath = dir + tempFileName;
        } else {
            rFilePath = dir +"/" + tempFileName;
        }
        File file = new File(rFilePath);
        if(requestMessage.getProtocolType() == RequestMessage.ProtocolTypeOnly) { //单独一个包
//            file = changeFileName(dir,file);
            writeDBFileToTemp(file,bodyBuff,false,true);
        } else if(requestMessage.getProtocolType() == RequestMessage.ProtocolTypeStart) {
            writeDBFileToTemp(file,bodyBuff,false,false);
        } else if(requestMessage.getProtocolType() == RequestMessage.ProtocolType) {  //多个包(除最后一个包);
            writeDBFileToTemp(file,bodyBuff,true,false);
        } else if(requestMessage.getProtocolType() == RequestMessage.ProtocolTypeEnd) { //除最后一个包
            writeDBFileToTemp(file,bodyBuff,true,true);
        }
    }

    /**
     * 接收数据前处理 文件夹 平台重启前留下的临时文件
     * @param tempFileName
     */
    private String doBeforeReceive(String tempFileName) {
        String dir;
        String _dir;
        if(tempFileName.indexOf("/")> 1){//tempFileName = "/jjjjj.tmp" or tempFileName = "jjjjj.tmp" or tempFileName = "/dir/jjjjj.tmp"  or tempFileName = "dir/jjjjj.tmp"
            _dir = tempFileName.substring(0,tempFileName.lastIndexOf("/"));
        } else {
            _dir = "";
        }
        dir = ChangeConfig.getRunShmPath() + "/" + this.appName + _dir;
        File dirFile = new File(dir);
        if(!dirFile.isDirectory()){
            dirFile.mkdirs();
        }
        return dir;
    }

    /**
     * 最后一个包或者单独一个包(写数据库临时文件)
     * @param file
     * @param bodyBuff
     * @param isPoint  追加
     * @param isEnd  写文件结束
     */
    private void writeDBFileToTemp(File file, byte[] bodyBuff, boolean isPoint, boolean isEnd) {
        FileOutputStream out = null;
        try {
            if(isPoint){
                out = new FileOutputStream(file,true);
            } else {
                out = new FileOutputStream(file);
            }
            out.write(bodyBuff);
            out.flush();
        } catch (IOException e) {
            LogLayout.error(logger,"platform","UdpServerHandler写入临时文件错误",e);
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                LogLayout.error(logger,"platform","I/O关闭异常",e);
            }
        }
        if(isEnd){
            targetProcessUdp.queue.offer(file.getPath());
        }
    }

    /**
     * 会话空闲
     * @param session
     * @param status
     * @throws Exception
     */
    public void sessionIdle(IoSession session, IdleStatus status)
            throws Exception {
        LogLayout.info(logger, "platform", "Session idle...");
    }

    /**
     * 打开连接触发的事件，它与sessionCreated的区别在于，
     * 一个连接地址（A）第一次请求Server会建立一个Session默认超时时间为1分钟，
     * 此时若未达到超时时间这个连接地址（A）再一次向Server发送请求
     * 即是sessionOpened（连接地址（A）第一次向Server发送请求或者连接超时后
     * 向Server发送请求时会同时触发sessionCreated和sessionOpened两个事件）
     * @param session
     * @throws Exception
     */
    public void sessionOpened(IoSession session) throws Exception {
//        LogLayout.info(logger,"platform","Session opened...");

    }

}
