package com.hzih.itp.platform.dbchange;

import com.hzih.itp.platform.config.ChangeConfig;
import junit.framework.TestCase;

/**
 * Created by 钱晓盼 on 14-1-14.
 */
public class TestDBChangeSource extends TestCase {

    public void testSource() {
        ChangeConfig.loadChannelAddress();
        DBChangeSource source = new DBChangeSource();
        source.init();
        source.start();
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }


}
