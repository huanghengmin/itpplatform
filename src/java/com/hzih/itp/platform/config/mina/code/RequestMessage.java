package com.hzih.itp.platform.config.mina.code;

import com.google.protobuf.ByteString;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: bluesky
 * Date: 2009-4-10
 * Time: 20:42:22
 * To change this template use File | Settings | File Templates.
 */
public class RequestMessage implements Serializable {
//    public static int FileNameLength = 19;
    public static short ProtocolTypeOnly = '\013';
    public static short ProtocolTypeStart = '\014';
    public static final short ProtocolType = '\015';
    public static final short ProtocolTypeEnd = '\016';
    public static final byte Version = '\017';
    private byte version;
    private short protocolType;
    private int msgLen;
    private ByteString msgBody;

    public RequestMessage() {
        version = Version;
//        protocolType = ProtoclType;
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public short getProtocolType() {
        return protocolType;
    }

    public void setProtocolType(short protocolType) {
        this.protocolType = protocolType;
    }

    public int getMsgLen() {
        return msgLen;
    }

    public void setMsgLen(int msgLen) {
        this.msgLen = msgLen;
    }

    public ByteString getMsgBody() {
        return msgBody;
    }

    public void setMsgBody(ByteString msgBody) {
        this.msgBody = msgBody;
        this.msgLen = msgBody.size();
    }

    public void clean() {
        msgLen = 0;
        msgBody = null;
    }
}
