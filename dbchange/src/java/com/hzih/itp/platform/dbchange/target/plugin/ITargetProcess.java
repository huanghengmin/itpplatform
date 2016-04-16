package com.hzih.itp.platform.dbchange.target.plugin;

import com.hzih.itp.platform.dbchange.source.info.DatabaseInfo;
import com.hzih.itp.platform.dbchange.target.TargetOperation;
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

    public boolean process(String tempFileName);

    public boolean process(byte[] data);

    public void init(TargetOperation target, DatabaseInfo databaseInfo);

    public void stop();

    public boolean isRun();

}
