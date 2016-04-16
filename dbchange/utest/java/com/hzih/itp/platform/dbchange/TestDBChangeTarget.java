package com.hzih.itp.platform.dbchange;

import com.hzih.itp.platform.config.ChangeConfig;
import junit.framework.TestCase;

/**
 * Created by 钱晓盼 on 14-1-14.
 */
public class TestDBChangeTarget extends TestCase {

    public void testTarget() {
        ChangeConfig.loadChannelAddress();
        DBChangeTarget target = new DBChangeTarget();
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
