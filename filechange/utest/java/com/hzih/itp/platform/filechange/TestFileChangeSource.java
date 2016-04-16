package com.hzih.itp.platform.filechange;

import com.hzih.itp.platform.config.ChangeConfig;
import junit.framework.TestCase;

/**
 * Created by 钱晓盼 on 14-1-23.
 */
public class TestFileChangeSource extends TestCase {

    public void testSource() {
        ChangeConfig.loadChannelAddress();
        FileChangeSource source = new FileChangeSource();
        source.init();
        source.start();
        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
            }
        }
    }
}
