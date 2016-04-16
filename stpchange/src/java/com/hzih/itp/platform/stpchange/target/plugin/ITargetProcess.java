package com.hzih.itp.platform.stpchange.target.plugin;

import com.hzih.itp.platform.stpchange.target.TargetOperation;
import com.inetec.common.config.stp.nodes.SocketChange;

import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 11-11-26
 * Time: ����3:21
 * To change this template use File | Settings | File Templates.
 */
public interface ITargetProcess extends Runnable {

    public boolean process(InputStream in,String fileFullName);

    public boolean process(String filePath);

    public void init(TargetOperation target, SocketChange config);

    public void stop();

    public boolean isRun();

}
