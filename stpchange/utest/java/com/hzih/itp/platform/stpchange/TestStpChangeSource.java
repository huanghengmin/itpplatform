package com.hzih.itp.platform.stpchange;

import com.hzih.itp.platform.config.ChangeConfig;
import junit.framework.TestCase;

/**
 * Created by 钱晓盼 on 14-1-15.
 */
public class TestStpChangeSource extends TestCase {

    public void testSource() {
        ChangeConfig.loadChannelAddress();
        StpChangeSource stpChangeSource = new StpChangeSource();
        stpChangeSource.init();
        stpChangeSource.start();
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }

}
