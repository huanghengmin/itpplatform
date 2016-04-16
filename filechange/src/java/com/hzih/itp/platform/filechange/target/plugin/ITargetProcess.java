package com.hzih.itp.platform.filechange.target.plugin;

import com.hzih.itp.platform.filechange.target.TargetOperation;
import com.hzih.itp.platform.filechange.utils.FileBean;
import com.inetec.common.config.stp.nodes.TargetFile;

import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 11-11-26
 * Time: ����3:21
 * To change this template use File | Settings | File Templates.
 */
public interface ITargetProcess extends Runnable {

    public boolean process(InputStream in, FileBean bean);

    public boolean process(byte[] data, FileBean bean);

    public void init(TargetOperation target, TargetFile config);

    public void stop();

    public boolean isRun();

}
