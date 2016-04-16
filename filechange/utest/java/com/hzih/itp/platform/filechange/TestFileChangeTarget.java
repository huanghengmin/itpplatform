package com.hzih.itp.platform.filechange;

import com.hzih.itp.platform.config.ChangeConfig;
import junit.framework.TestCase;

/**
 * Created by 钱晓盼 on 14-1-23.
 */
public class TestFileChangeTarget extends TestCase {

    public void testTarget() {
        ChangeConfig.loadChannelAddress();
        FileChangeTarget target = new FileChangeTarget();
        target.init();
        target.start();
        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
            }
        }
    }
}
