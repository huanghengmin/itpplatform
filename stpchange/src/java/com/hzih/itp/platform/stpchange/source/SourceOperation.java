package com.hzih.itp.platform.stpchange.source;

import com.hzih.itp.platform.stpchange.source.plugin.ISourceProcess;
import com.hzih.itp.platform.stpchange.source.plugin.SourceProcessHttp;
import com.hzih.itp.platform.stpchange.source.plugin.SourceProcessHttps;
import com.inetec.common.config.stp.nodes.SocketChange;
import com.inetec.common.config.stp.nodes.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 钱晓盼 on 14-1-9.
 */
public class SourceOperation implements Runnable{
    final static Logger logger = LoggerFactory.getLogger(SourceOperation.class);

    private boolean isRun = false;
    private Type type;
    private String appName;

    public void init(Type type) {
        this.type  = type;
        this.appName = type.getTypeName();
    }

    public Type getType() {
        return type;
    }

    @Override
    public void run() {
        SocketChange config = type.getPlugin().getSourceSocket();
        ISourceProcess sourceProcess = null;
        if(config.isClientauthenable()) {
            sourceProcess = new SourceProcessHttps();
        } else {
            sourceProcess = new SourceProcessHttp();
        }
        sourceProcess.init(this, config);
        new Thread(sourceProcess).start();
        isRun = true;
        while (isRun) {
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
            }
        }
    }

    public boolean process(byte[] data) {
        if (data == null && data.length == 0) {

            return false;
        }
        boolean result = false;

        return result;
    }

    /**
     * 审计处理 入库 发送到下一级 发送到第三方
     * @param
     * @param isSuccess
     */
    private void auditProcess( boolean isSuccess) {

    }



}
