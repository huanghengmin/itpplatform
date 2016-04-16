package com.hzit.itp.platform.datacheck;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 14-5-27
 * Time: 下午3:33
 * To change this template use File | Settings | File Templates.
 */
public class BlackWords extends Thread {
    public Map<String,String> blackWords ;
    private String driverClass;
    private String url;
    private String user;
    private String password;
    private boolean isRun = false;
    private boolean isSelect = false;

    public boolean isRun() {
        return isRun;
    }

    public void setRun(boolean run) {
        isRun = run;
    }

    public boolean isSelect() {
        return isSelect;
    }

    public void setSelect(boolean select) {
        isSelect = select;
    }

    public void init(){
        blackWords = new HashMap<>();
        Properties prop = new Properties();
        try{
            prop.load(BlackWords.class.getResourceAsStream("/config.properties"));
            driverClass = prop.getProperty("jdbc.driverClass");
            url = prop.getProperty("jdbc.url");
            user = prop.getProperty("jdbc.user");
            password = prop.getProperty("jdbc.password");
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        isRun =true;
    }

    public void run() {
        while (isRun){
            blackWords.clear();
            getBlackWordsFormDb();
            try {
                Thread.sleep(5*60*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    private void getBlackWordsFormDb(){
        isSelect = true;
        Connection conn = null;
        ResultSet rs = null;
        try{
            Class.forName(driverClass);
            conn = DriverManager.getConnection(url,user,password);
            Statement stmt = conn.createStatement();
            rs = stmt.executeQuery("select * from contentfilter");
            while (rs.next()){
                String black = rs.getString("filter");
                if(!blackWords.containsKey(black)){
                    blackWords.put(black,black);
//                    System.out.println(black);
                }
            }
            rs.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            try{
                if(rs != null){
                    rs.close();
                }
                if(conn != null){
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        isSelect = false;
    }

    public static void main(String[] args) throws Exception{
        BlackWords b = new BlackWords();
        b.init();
        Thread thread = new Thread(b);
        thread.start();
    }
}
