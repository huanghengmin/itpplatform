package com.hzih.itp.platform.filechange.utils;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.filechange.utils.jdbc.DatabaseSqliteUtil;
import junit.framework.TestCase;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;

/**
 * Created by 钱晓盼 on 14-1-24.
 */
public class TestSqlite extends TestCase {

    public void testSql() {
        DatabaseSqliteUtil util = new DatabaseSqliteUtil();
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
//        String fileName = ChangeConfig.getBackPath() + "/database/filechange.sqlite";
        String fileName = "F:/itp/data/database/filechange.sqlite";
        dataSource.setUrl("jdbc:sqlite:" + fileName);
        util.setDataSource(dataSource);

        Connection conn = util.getConnection();

    }
}
