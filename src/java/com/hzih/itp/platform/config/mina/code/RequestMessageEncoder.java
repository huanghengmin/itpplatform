package com.hzih.itp.platform.config.mina.code;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.apache.mina.filter.codec.demux.MessageEncoder;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2009-4-10
 * Time: 20:43:16
 * To change this template use File | Settings | File Templates.
 */
public class RequestMessageEncoder implements MessageEncoder {
    public RequestMessageEncoder() {
    }

    public void encode(IoSession session, Object message, ProtocolEncoderOutput out)
            throws Exception {
        RequestMessage mess = (RequestMessage) message;
        IoBuffer buff = IoBuffer.allocate(mess.getMsgLen() + 7);
        buff.setAutoExpand(true);
        buff.put(mess.getVersion());
        buff.putShort(mess.getProtocolType());
        buff.putInt(mess.getMsgLen());
        if (mess.getMsgBody() != null)
            buff.put(mess.getMsgBody().toByteArray());
        buff.flip();
        mess.clean();
        out.write(buff);
    }
}