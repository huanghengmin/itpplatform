package com.hzih.itp.platform.stpchange.target.plugin;

import com.hzih.itp.platform.config.mina.code.ServerMessageCodecFactory;
import com.hzih.itp.platform.stpchange.target.TargetOperation;
import com.hzih.itp.platform.stpchange.target.plugin.udp.UdpServerHandler;
import com.hzih.logback.LogLayout;
import com.inetec.common.config.stp.nodes.SocketChange;
import com.inetec.common.config.stp.nodes.Type;
import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.DatagramSessionConfig;
import org.apache.mina.transport.socket.nio.NioDatagramAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by 钱晓盼 on 14-1-13.
 */
public class TargetProcessUdp implements ITargetProcess {
    final static Logger logger = LoggerFactory.getLogger(TargetProcessUdp.class);

    private TargetOperation target;
    private SocketChange config;
    private NioDatagramAcceptor acceptor;//创建一个UDP的接收器
    private Type type;
    private String appName;
    private int port = 0;
    private boolean isRun = false;
    public BlockingQueue<String> queue;

    public String getAppName() {
        return appName;
    }

    @Override
    public boolean process(InputStream in,String fileFullName) {
        return false;
    }

    @Override
    public boolean process(String filePath) {
        boolean isOk = false;
        do{
            isOk = target.process(filePath);
        } while (!isOk);
        return isOk;
    }

    @Override
    public void init(TargetOperation target,SocketChange config) {
        this.target = target;
        this.config = config;
        this.type = target.getType();
        this.appName = type.getTypeName();
        this.port = Integer.parseInt(config.getPort());
        queue = new LinkedBlockingQueue<String>(20);
        config();
    }

    private void config() {
        acceptor = new NioDatagramAcceptor();
        UdpServerHandler serverHandler = new UdpServerHandler();
        serverHandler.setReceiveService(this);
        acceptor.setHandler(serverHandler);//设置接收器的处理程序
//        Executor threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);//建立线程池
//        acceptor.getFilterChain().addLast("exector", new ExecutorFilter(threadPool));
        acceptor.getFilterChain().addLast("logger", new LoggingFilter());
        DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();
        chain.addLast("codec", new ProtocolCodecFilter(new ServerMessageCodecFactory()));
//        chain.addLast("keep-alive", new HachiKeepAliveFilterInMina());//心跳
        DatagramSessionConfig dcfg = acceptor.getSessionConfig();//建立连接的配置文件
        dcfg.setReuseAddress(true);//设置每一个非主监听连接的端口可以重用
        dcfg.setBroadcast(false);
        dcfg.setReceiveBufferSize(6553600);
        dcfg.setSendBufferSize(6553600);
        dcfg.setMaxReadBufferSize(6553600);
        dcfg.setMinReadBufferSize(65536);
        dcfg.setTrafficClass(0x04|0x08); //高性能和可靠性
    }

    @Override
    public void stop() {
        acceptor.unbind();
        isRun = false;
    }

    @Override
    public boolean isRun() {
        return isRun;
    }

    private String pollQueue() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
        }
        return null;
    }

    @Override
    public void run() {
        isRun = true;
        try {
            acceptor.bind(new InetSocketAddress(port));//绑定端口
        } catch (IOException e) {

        }
        LogLayout.info(logger,appName,"启动端口" + port + "的UDP监听...");
        String filePath;
        while (isRun) {
            filePath = pollQueue();
            this.process(filePath);
            new File(filePath).delete();

        }
    }
}
