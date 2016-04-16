package com.hzih.itp.platform.filechange.utils.jdbc;

/**
 * Created by 钱晓盼 on 14-1-13.
 */
public class TempFile {
    public static final String status_error = "0";  // 正在处理,断点续传
    public static final String status_ok = "1";     // 处理完成
    public static final String status_resend = "2";// 需要重传的文件
    private int id;
    private String appName;
    private String FileFullName;
    private long lastModified;
    private long fileSize;//文件大小
    private String status;//0: 正在处理,断点续传 1: 处理完成 2: 重传的文件
    //  创建数据库filechange.sqlite文件
    /*
      CREATE TABLE "filechangeindex"
            ("id" INTEGER PRIMARY KEY  AUTOINCREMENT  NOT NULL ,
            "appName" VARCHAR,
            "fileFullName" VARCHAR,
            "lastModified" INTEGER,
            "fileSize" INTEGER,
            "status" VARCHAR DEFAULT 0);
    */

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getFileFullName() {
        return FileFullName;
    }

    public void setFileFullName(String fileFullName) {
        FileFullName = fileFullName;
    }

    public long getLastModified() {
        return lastModified;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
