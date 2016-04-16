package com.hzih.itp.platform.config.mina.code;

import com.google.protobuf.ByteString;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.demux.MessageDecoder;
import org.apache.mina.filter.codec.demux.MessageDecoderResult;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2009-4-10
 * Time: 20:43:16
 * To change this template use File | Settings | File Templates.
 */
public class RequestMessageDecoder implements MessageDecoder {
    public static short ProtocolTypeOnly = '\013';
    public static short ProtocolTypeStart = '\014';
    public static final short ProtocolType = '\015';
    public static final short ProtocolTypeEnd = '\016';
    public static final byte Version = '\017';
    private byte version;
    private short protocolType;
    private short protocolTypeEnd;
    private short protocolTypeOnly;
    private short protocolTypeStart;

    public RequestMessageDecoder() {
        this.version = Version;
        this.protocolType = ProtocolType;
        this.protocolTypeStart = ProtocolTypeStart;
        this.protocolTypeOnly = ProtocolTypeOnly;
        this.protocolTypeEnd = ProtocolTypeEnd;
    }

    public MessageDecoderResult decodable(IoSession session, IoBuffer in) {
        if (in.remaining() < 7) {
            return MessageDecoderResult.NEED_DATA;
        }
        //get version
        byte ver = in.get();
        //get protocol type
        short type = in.getShort();
        //get message length
        int len = in.getInt();
        if (in.remaining() < len)
            return MessageDecoderResult.NEED_DATA;
        if (ver == this.version
                && (type == this.protocolTypeStart
                || type == this.protocolType
                || type == this.protocolTypeOnly
                || type == this.protocolTypeEnd)) {
            return MessageDecoderResult.OK;
        }
        return MessageDecoderResult.NOT_OK;
    }

    public MessageDecoderResult decode(IoSession session, IoBuffer in,
                                       ProtocolDecoderOutput out) throws Exception {
        RequestMessage tMsg = new RequestMessage();
        tMsg.setVersion(in.get());
        tMsg.setProtocolType(in.getShort());
        int msgLen = in.getInt();
        byte[] buff = new byte[msgLen];
        in.get(buff);
        tMsg.setMsgLen(msgLen);
        tMsg.setMsgBody(ByteString.copyFrom(buff));
        out.write(tMsg);
        in.free();
        return MessageDecoderResult.OK;
    }

    public void finishDecode(IoSession session, ProtocolDecoderOutput out)
            throws Exception {
    }
}

