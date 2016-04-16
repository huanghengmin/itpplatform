package com.hzih.itp.platform.config.http;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;

/**
 * Created by 钱晓盼 on 14-1-16.
 */
public class HttpClientUtils {
    static final Log log = LogFactory.getLog(HttpClientUtils.class);
	private static ThreadSafeClientConnManager cm = null;
	static {
		SchemeRegistry schemeRegistry = new SchemeRegistry();
		schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory
				.getSocketFactory()));

		cm = new ThreadSafeClientConnManager(schemeRegistry);
		try {
			int maxTotal = 100;
			cm.setMaxTotal(maxTotal);
		} catch (NumberFormatException e) {
			log.error( "Key[httpclient.max_total] Not Found in systemConfig.properties", e);
		}
		// 每条通道的并发连接数设置（连接池）
		try {
			int defaultMaxConnection = 50;
			cm.setDefaultMaxPerRoute(defaultMaxConnection);
		} catch (NumberFormatException e) {
			log.error(
					"Key[httpclient.default_max_connection] Not Found in systemConfig.properties",
					e);
		}
	}

	public static HttpClient getHttpClient() {
		HttpParams params = new BasicHttpParams();
		params.setParameter(CoreProtocolPNames.PROTOCOL_VERSION,
				HttpVersion.HTTP_1_1);
		params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 3000); // 3000ms
		return new DefaultHttpClient(cm, params);
	}

	public static void release() {
		if (cm != null) {
			cm.shutdown();
		}
	}

	public static void main(String[] args) throws ClientProtocolException,
			IOException {
		Random r = new Random();
		for (int i = 0; i < 10; i++) {
			long l1 = System.currentTimeMillis();
			HttpClient client = getHttpClient();

			HttpGet get = new HttpGet("http://www.baidu.com/s?wd="
					+ r.nextInt(5000));
			HttpResponse response = client.execute(get);
			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				HttpEntity entity = response.getEntity();
				long l = entity.getContentLength();
				System.out.println("回应结果长度:" + l);
			}
			System.out.println("查询耗时" + (System.currentTimeMillis() - l1));
		}
	}
}
