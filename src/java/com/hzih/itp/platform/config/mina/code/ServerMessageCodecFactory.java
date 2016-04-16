package com.hzih.itp.platform.config.mina.code;

import org.apache.mina.filter.codec.demux.DemuxingProtocolCodecFactory;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2009-4-10
 * Time: 20:44:20
 * To change this template use File | Settings | File Templates.
 */
public class ServerMessageCodecFactory extends DemuxingProtocolCodecFactory {
    public ServerMessageCodecFactory() {
        super.addMessageDecoder(RequestMessageDecoder.class);
    }
}

