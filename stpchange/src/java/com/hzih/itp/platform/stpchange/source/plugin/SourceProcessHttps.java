package com.hzih.itp.platform.stpchange.source.plugin;

import com.hzih.itp.platform.stpchange.source.SourceOperation;
import com.inetec.common.config.stp.nodes.SocketChange;

import java.io.InputStream;

/**
 * Created by 钱晓盼 on 14-1-10.
 */
public class SourceProcessHttps implements ISourceProcess{
    @Override
    public boolean process(InputStream in) {
        return false;
    }

    @Override
    public boolean process(byte[] data) {
        return false;
    }

    @Override
    public void init(SourceOperation source, SocketChange config) {

    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isRun() {
        return false;
    }

    @Override
    public void run() {

    }
}
