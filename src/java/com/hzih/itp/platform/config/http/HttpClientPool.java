package com.hzih.itp.platform.config.http;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;

/**
 * Created by 钱晓盼 on 14-1-16.
 */
public class HttpClientPool extends GenericKeyedObjectPool {

    public HttpClientPool() {
        super();
    }
    public HttpClientPool(int max, int init) {
        super();
    }
    public HttpClientPool(HttpClientFactory httpClient) {
        super(httpClient);
    }

    public HttpClientPool(HttpClientFactory httpClient, int max) {
        super(httpClient,max);
    }
}
