package com.hzih.itp.platform.config.mina;

import com.hzih.logback.LogLayout;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: 钱晓盼
 * Date: 12-12-13
 * Time: 下午9:21
 * To change this template use File | Settings | File Templates.
 */
public class ClientHandler extends IoHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

    public void messageSent(IoSession session, Object message) throws Exception {
//        System.out.println("client is beginning to send!");
        /*if (message instanceof IoBuffer) {
            IoBuffer buffer = (IoBuffer) message;
            String closed = new String(buffer.array());
            if(closed.equals("9999")){
                System.out.println(closed);
                session.close(true);
            }
        }*/
    }

    public void sessionClosed(IoSession session) throws Exception {
//        System.out.println("客户端关闭了当前会话");
        session.close(true);
    }

        @Override
    public void sessionCreated(IoSession session) throws Exception {
//        System.out.println("客户端成功创建session");
    }

    @Override
    public void sessionIdle(IoSession session, IdleStatus status)
            throws Exception {
//        System.out.println("wait...");
    }

    @Override
    public void sessionOpened(IoSession session) throws Exception {
//        System.out.println("客户端成功开启一个session id:");
    }

        @Override
    public void exceptionCaught(IoSession session, Throwable cause)
            throws Exception {
        LogLayout.error(logger, "platform", "客户端发送出错", (Exception) cause);
        System.setProperty("networkok_1", String.valueOf(false));
        session.close(true);
    }

}
