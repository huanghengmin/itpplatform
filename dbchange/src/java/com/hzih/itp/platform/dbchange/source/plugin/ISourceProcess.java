package com.hzih.itp.platform.dbchange.source.plugin;

import com.hzih.itp.platform.dbchange.source.SourceOperation;
import com.hzih.itp.platform.dbchange.source.info.DatabaseInfo;
import com.hzih.itp.platform.utils.DataAttributes;
import com.inetec.common.config.stp.nodes.DataBase;

import java.io.InputStream;

/**
 *
 */
public interface ISourceProcess extends Runnable {

    public DataAttributes process(InputStream in);

    public boolean process(byte[] data);

    public void init(SourceOperation source, DatabaseInfo databaseInfo);

    public void stop();

    public boolean isRun();

}
