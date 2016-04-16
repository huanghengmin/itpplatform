package com.hzih.itp.platform.config.mina;

import com.hzih.itp.platform.config.mina.code.ClientMessageCodecFactory;
import com.hzih.itp.platform.config.mina.code.RequestMessage;
import com.hzih.logback.LogLayout;
import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.DatagramSessionConfig;
import org.apache.mina.transport.socket.nio.NioDatagramConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Created with IntelliJ IDEA.
 * User: 钱晓盼
 * Date: 12-8-29
 * Time: 下午3:37
 * To change this template use File | Settings | File Templates.
 */
public class UdpClientImpl {
    private static final Logger logger = LoggerFactory.getLogger(UdpClientImpl.class);
    private InetSocketAddress target;
    private String appName;
    private IoSession session;
    private NioDatagramConnector connector;
    private ConnectFuture connFuture;
    public UdpClientImpl(String appName,InetSocketAddress target) {
        this.target = target;
        this.appName = appName;
        connector = new NioDatagramConnector();
        connector.setConnectTimeoutMillis(60000L);
        connector.setConnectTimeoutCheckInterval(10);
        connector.setHandler(new ClientHandler());
        DefaultIoFilterChainBuilder chain = connector.getFilterChain();
        chain.addLast("codec", new ProtocolCodecFilter(new ClientMessageCodecFactory()));

//        chain.addLast("toMessageTyep", new MyMessageEn_Decoder());
//        chain.addLast("logger", new LoggingFilter());
        DatagramSessionConfig ccfg = connector.getSessionConfig();
        ccfg.setReuseAddress(true);
        ccfg.setBroadcast(false);
        ccfg.setReceiveBufferSize(6553600);
        ccfg.setSendBufferSize(6553600);
        ccfg.setMaxReadBufferSize(6553600);
        ccfg.setMinReadBufferSize(65536);
        ccfg.setTrafficClass(0x04|0x08); //高性能和可靠性
    }

    private boolean sendBuf(RequestMessage message) {
        try{
            session.write(message);
            System.setProperty("networkok_1", String.valueOf(true));
        } catch (Exception e) {
            LogLayout.error(logger, "platform", "send error ", e);
        }
        return Boolean.valueOf(System.getProperty("networkok_1"));
    }

    public boolean isConnect() {
        try{
            boolean isClosed = session.isClosing();
            if(isClosed){
                session = null;
            }
        } catch (Exception e) {
            session = null;
        }
        if(session==null){
            connFuture = connector.connect(target);
            connFuture.awaitUninterruptibly();
            session = connFuture.getSession();
        }
        return session==null?false:true;
    }

    public void send(RequestMessage message){
        boolean isOk = false;
        boolean isConned = isConnect();
        while (!isConned) {
            isConned = isConnect();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
        }
       isOk = sendBuf(message);
        while (!isOk){
            LogLayout.error(logger,"platform",appName+"应用UDP发送到"+target.getHostString()+"的端口"+target.getPort()+"不可达,等待10秒...");
            try {
                Thread.sleep(1000*10);
            } catch (InterruptedException e) {
            }
            if(isConnect()) {
                isOk = sendBuf(message);
            }
        }
    }

    public void close() {
        connFuture.cancel();
    }
    public void destroy(){
        connector.dispose();
    }

    public InetSocketAddress getInetSocketAddress() {
        return target;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }
}
