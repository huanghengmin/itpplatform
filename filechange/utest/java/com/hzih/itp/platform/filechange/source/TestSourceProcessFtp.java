package com.hzih.itp.platform.filechange.source;

import com.hzih.logback.LogLayout;
import junit.framework.TestCase;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.io.IOException;

/**
 * Created by 钱晓盼 on 14-1-23.
 */
public class TestSourceProcessFtp extends TestCase {

    public void testFtpLogin() {
        FTPClient client = new FTPClient();
        String hostname = "192.168.1.206";
        int port = 21;
        String username = "test";
        String password = "123456";
        boolean isConnectOk = false;
        while (!isConnectOk) {
            try {
                client.connect(hostname, port);
                client.setControlEncoding("utf-8");
                if(FTPReply.isPositiveCompletion(client.getReplyCode())){
                    if(client.login(username, password)){
                        client.enterLocalPassiveMode();
                        client.setFileType(FTPClient.BINARY_FILE_TYPE);
                        isConnectOk = true;
                    } else {
//                        disconnect();
                    }
                } else {
//                    disconnect();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
