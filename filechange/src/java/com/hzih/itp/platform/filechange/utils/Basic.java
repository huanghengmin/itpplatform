package com.hzih.itp.platform.filechange.utils;

import com.hzih.logback.LogLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * Created by 钱晓盼 on 14-1-13.
 */
public class Basic {
    final static Logger logger = LoggerFactory.getLogger(Basic.class);
    private static String ip = "127.0.0.1";
    private static String dbtype = "mysql";
    private static String username = "root";
    private static String password = "123456";
    private static String dbname = "stp";
    private static String table = "contentfilter";
    private static String column = "filter";
    private static Map<String, String> map = new HashMap<String, String>();
    private static Map<String, String> hashMap = new HashMap<String, String>();
    private static String keyword = null;

    static {
        map.put("mysql", "com.mysql.jdbc.Driver");
        Properties prop = new Properties();
        try {
            prop.load(Basic.class.getResourceAsStream("config-file.properties"));
        } catch (IOException e) {
            LogLayout.error(logger,"连接数据库失败",e);
        }
        ip = prop.getProperty("ip");
        dbtype = prop.getProperty("dbtype");
        username = prop.getProperty("username");
        password = prop.getProperty("password");
        dbname = prop.getProperty("dbname");
        table = prop.getProperty("table");
        column = prop.getProperty("column");
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                updateKeyword();
            }
        },1000,12*1000*60*60);
    }

    public static Connection getCon() {
        try {
            Class.forName(map.get(dbtype));
        } catch (ClassNotFoundException e) {
            LogLayout.info(logger,"platform",e.getMessage());  //To change body of catch statement use File | Settings | File Templates.
        }
        String url = "jdbc:" + dbtype + "://" + ip + ":3306/" + dbname;
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            LogLayout.info(logger,"platform",e.getMessage());  //To change body of catch statement use File | Settings | File Templates.
        }
        return connection;
    }

    private static void updateKeyword() {
        Connection connection = getCon();
        String sql = "select " + column + " from " + table;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        StringBuilder stringBuilder = new StringBuilder();
        try {
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                stringBuilder.append(resultSet.getString(column)).append(",");
            }
        } catch (SQLException e) {
            LogLayout.info(logger,"platform",e.getMessage());  //To change body of catch statement use File | Settings | File Templates.
        }
        finally {
            try{
                resultSet.close();
                connection.close();
                preparedStatement.close();
            }
            catch (Exception e){
                LogLayout.error(logger,"platform","关闭数据库连接出错");
            }
        }
        keyword = stringBuilder.toString();
    }
    public static String getKeywords(){
//        if(keyword == null) {
//        }
        updateKeyword();
        return keyword;
    }
}
