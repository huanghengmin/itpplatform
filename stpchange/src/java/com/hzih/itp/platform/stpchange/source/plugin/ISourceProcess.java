package com.hzih.itp.platform.stpchange.source.plugin;

import com.hzih.itp.platform.stpchange.source.SourceOperation;
import com.inetec.common.config.stp.nodes.SocketChange;

import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 11-11-26
 * Time: ����3:21
 * To change this template use File | Settings | File Templates.
 */
public interface ISourceProcess extends Runnable {

    public boolean process(InputStream in);

    public boolean process(byte[] data);

    public void init(SourceOperation source, SocketChange config);

    public void stop();

    public boolean isRun();

}
