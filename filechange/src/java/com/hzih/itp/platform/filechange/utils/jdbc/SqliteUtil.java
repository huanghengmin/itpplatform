package com.hzih.itp.platform.filechange.utils.jdbc;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.logback.LogLayout;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 钱晓盼 on 14-1-13.
 */
public class SqliteUtil {
    final static Logger logger = LoggerFactory.getLogger(SqliteUtil.class);

    private static DatabaseSqliteUtil sqliteUtil;

    public static void init() {
        String fileName = ChangeConfig.getBackPath()+"/database/filechange.sqlite";
        sqliteUtil = new DatabaseSqliteUtil();
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:" + fileName);
        sqliteUtil.setDataSource(dataSource);
    }

    public static Connection connect() {
        return sqliteUtil.getConnection();
    }

    public static void close(Connection conn) {
        DatabaseSqliteUtil.closeConnection(conn);
    }

    private static void close(Statement stmt, Connection conn) {
        DatabaseSqliteUtil.closeStatement(stmt);
        close(conn);
    }

    private static void close(ResultSet rs, Statement stmt, Connection conn) {
        DatabaseSqliteUtil.closeResultSet(rs);
        DatabaseSqliteUtil.closeStatement(stmt);
        close(conn);
    }

    public static synchronized List<TempFile> query(String appName) {
        List<TempFile> tempFiles = new ArrayList<TempFile>();
        Connection conn = connect();
        if(conn!=null) {
            String sql = "select * from filechangeindex where appName = '"+appName+"'";
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = DatabaseSqliteUtil.getStatement(conn);
                rs = DatabaseSqliteUtil.getResultSet(stmt, sql);
                while (rs.next()) {
                    int id = rs.getInt("id");
                    appName = rs.getString("appName"); // Column 1
                    String fileFullName = rs.getString("fileFullName"); // Column 2
                    long lastModified = rs.getLong("lastModified"); // Column 3
                    long fileSize = rs.getLong("fileSize"); // Column 4
                    String status = rs.getString("status"); //Column 5
                    TempFile tempFile = new TempFile();
                    tempFile.setId(id);
                    tempFile.setAppName(appName);
                    tempFile.setFileFullName(fileFullName);
                    tempFile.setLastModified(lastModified);
                    tempFile.setFileSize(fileSize);
                    tempFile.setStatus(status);
                    tempFiles.add(tempFile);
                }
            } catch (SQLException e) {
                LogLayout.error(logger,appName,"query error" + e.getMessage(),e);
            } finally {
                close(rs,stmt,conn);
            }
        }
        return tempFiles;
    }

    public static List<TempFile> queryByPage(String appName, int start, int limit) {
        List<TempFile> tempFiles = new ArrayList<TempFile>();
        Connection conn = connect();
        if(conn!=null) {
            String sql = "select * from filechangeindex where appName = '"+appName+"' order by rowid limit "+start+","+limit;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = DatabaseSqliteUtil.getStatement(conn);
                rs = DatabaseSqliteUtil.getResultSet(stmt, sql);
                while (rs.next()) {
                    int id = rs.getInt("id");
                    appName = rs.getString("appName"); // Column 1
                    String fileFullName = rs.getString("fileFullName"); // Column 2
                    long lastModified = rs.getLong("lastModified"); // Column 3
                    long fileSize = rs.getLong("fileSize"); // Column 4
                    String status = rs.getString("status"); // Column 5
                    TempFile tempFile = new TempFile();
                    tempFile.setId(id);
                    tempFile.setAppName(appName);
                    tempFile.setFileFullName(fileFullName);
                    tempFile.setLastModified(lastModified);
                    tempFile.setFileSize(fileSize);
                    tempFile.setStatus(status);
                    tempFiles.add(tempFile);
                }
            } catch (SQLException e) {
                LogLayout.error(logger,appName,"queryByPage error" + e.getMessage(),e);
            } finally {
                close(rs,stmt,conn);
            }

        }
        return tempFiles;
    }

    public static TempFile query(String appName,String fileFullName) {
        Connection conn = connect();
        if(conn!=null) {
            String sql = "select * from filechangeindex where appName = '"+appName+"' and fileFullName = '"+fileFullName+"'";
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = DatabaseSqliteUtil.getStatement(conn);
                rs = DatabaseSqliteUtil.getResultSet(stmt, sql);
                while (rs.next()) {
                    int id = rs.getInt("id");
                    appName = rs.getString("appName"); // Column 1
                    fileFullName = rs.getString("fileFullName"); // Column 2
                    long lastModified = rs.getLong("lastModified"); // Column 3
                    long fileSize = rs.getLong("fileSize"); // Column 4
                    String status = rs.getString("status");
                    TempFile tempFile = new TempFile();
                    tempFile.setId(id);
                    tempFile.setAppName(appName);
                    tempFile.setFileFullName(fileFullName);
                    tempFile.setLastModified(lastModified);
                    tempFile.setFileSize(fileSize);
                    tempFile.setStatus(status);
                    return tempFile;
                }
            } catch (SQLException e) {
                LogLayout.error(logger,appName,e.getMessage(),e);
            } finally {
                close(rs,stmt,conn);
            }

        }
        return null;
    }

    public static int count(String appName) {
        Connection conn = connect();
        int count = 0;
        if(conn!=null) {
            String sql = "select count(*) from filechangeindex where appName = '"+appName+"'";
//            LogLayout.info(logger,"platform",sql);
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = DatabaseSqliteUtil.getStatement(conn);
                rs = DatabaseSqliteUtil.getResultSet(stmt, sql);
                if(rs.next()) {
                    count = rs.getInt(1);
                }
            } catch (SQLException e) {
                LogLayout.error(logger,appName,"count error" + e.getMessage(),e);
            } finally {
                close(rs,stmt,conn);
            }

        }
        return count;
    }

    public static void insert(TempFile tempFile) {
        Connection conn = connect();
        String appName = tempFile.getAppName();
        if(conn!=null) {
            String sql = "insert into filechangeindex (appName,fileFullName,lastModified,fileSize,status )" +
                        " values ('" + tempFile.getAppName() + "','" + tempFile.getFileFullName() +
                        "'," + tempFile.getLastModified() + "," + tempFile.getFileSize() + "," + tempFile.getStatus() + ")";
//            LogLayout.info(logger,"platform",sql);
            Statement stmt = null;
            try {
                stmt = DatabaseSqliteUtil.getStatement(conn);
                int result = stmt.executeUpdate(sql);
//                if(result==1) {
//                    LogLayout.info(logger,"platform","insert filechangeindex success");
//                }
            } catch (SQLException e) {
                LogLayout.error(logger,appName,"insert error" + e.getMessage(),e);
            } finally {
                close(stmt,conn);
            }

        }
    }

    public static void delete(String appName,String fileFullName) {
        Connection conn = connect();
        if(conn!=null) {
            String sql = "delete FROM filechangeindex where appName ='"+appName+"' and fileFullName = '"+fileFullName+"'";
            Statement stmt = null;
            try {
                stmt = DatabaseSqliteUtil.getStatement(conn);
                int result = stmt.executeUpdate(sql);
//                if(result==0) {
//                    LogLayout.info(logger,"platform","delete filechangeindex success");
//                }
            } catch (SQLException e) {
               LogLayout.error(logger,appName,"delete error" + e.getMessage(),e);
            } finally {
                close(stmt,conn);
            }

        }
    }

    public static void update(TempFile tempFile) {
        Connection conn = connect();
        String appName = tempFile.getAppName();
        if(conn!=null) {
            String sql = "update filechangeindex " +
                    "set lastModified = "+tempFile.getLastModified()+",fileSize = "+tempFile.getFileSize() +
                    ",status = '" + tempFile.getStatus() + "' " +
                    "where appName ='"+tempFile.getAppName()+"' and fileFullName = '"+tempFile.getFileFullName()+"'";
            Statement stmt = null;
            try {
                stmt = DatabaseSqliteUtil.getStatement(conn);
                int result = stmt.executeUpdate(sql);
//                if(result==1) {
//                    LogLayout.info(logger,"platform","update filechangeindex success");
//                }
            } catch (SQLException e) {
                LogLayout.error(logger,appName,"update error" + e.getMessage(),e);
            } finally {
                close(stmt,conn);
            }

        }
    }

    public static List<String> queryForResend(String appName, String status_resend) {
        Connection conn = connect();
        List<String> list = new ArrayList<String>();
        if(conn!=null) {
            String sql = "select * from filechangeindex where appName = '"+appName+"' and status = '"+status_resend+"'";
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = DatabaseSqliteUtil.getStatement(conn);
                rs = DatabaseSqliteUtil.getResultSet(stmt, sql);
                String fileFullName = null;
                while (rs.next()) {
                    fileFullName = rs.getString("fileFullName"); // Column 2
                    list.add(fileFullName);
                }
            } catch (SQLException e) {
                LogLayout.error(logger,appName,"queryForResend error" + e.getMessage(),e);
            } finally {
                close(rs,stmt,conn);
            }

        }
        return list;
    }
}
