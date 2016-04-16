package com.hzih.itp.platform.config.mina;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;


/**
 * Created with IntelliJ IDEA.
 * User: 钱晓盼
 * Date: 12-8-29
 * Time: 下午3:31
 * To change this template use File | Settings | File Templates.
 */
public class UdpClientPool extends GenericKeyedObjectPool {
    public UdpClientPool() {
        super();
    }
    public UdpClientPool(int max, int init) {
        super();
    }
    public UdpClientPool(UdpClientFactory udpClient) {
        super(udpClient);
    }

    public UdpClientPool(UdpClientFactory udpClient, int max) {
        super(udpClient,max);
    }


}
