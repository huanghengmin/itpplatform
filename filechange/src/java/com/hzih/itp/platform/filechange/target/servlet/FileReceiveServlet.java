package com.hzih.itp.platform.filechange.target.servlet;

import com.hzih.itp.platform.config.ChangeConfig;
import com.hzih.itp.platform.filechange.target.TargetOperation;
import com.hzih.itp.platform.filechange.target.plugin.TargetProcessFtp;
import com.hzih.itp.platform.utils.StaticField;
import com.hzih.logback.LogLayout;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: sunny
 * Date: 14-1-11
 * Time: 下午4:24
 */
public class FileReceiveServlet extends HttpServlet {
    final static Logger logger = LoggerFactory.getLogger(FileReceiveServlet.class);

    private TargetOperation targetOperation;
    private String appName;
    private String charset;

    public FileReceiveServlet() {
    }

    public FileReceiveServlet(String appName, String charset) {
        this.appName = appName;
        this.charset = charset;
    }

    public FileReceiveServlet(TargetOperation targetOperation, String appName, String charset) {
        this.appName = appName;
        this.charset = charset;
        this.targetOperation = targetOperation;
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>hello jetty context is ok!</h1>");
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/html;charset=utf-8");
        String fileName = request.getHeader("fileName");
        fileName = new String(new BASE64Decoder().decodeBuffer(fileName));
        long fileSize = Long.parseLong(request.getHeader("fileSize"));
        FileItemFactory factory = new DiskFileItemFactory();
        ServletFileUpload upload = new ServletFileUpload(factory);
        List<FileItem> items = new ArrayList<FileItem>();
        InputStream is = null;
        try {
            items = upload.parseRequest(request);
            // 得到所有的文件
            Iterator<FileItem> it = items.iterator();
            while (it.hasNext()) {
                FileItem fItem = (FileItem) it.next();
                String fName = "";
                if (fItem.isFormField()) { // 普通文本框的值
                } else { // 获取上传文件的值
                    is = fItem.getInputStream();
                }
            }

            String outFile = write(is, fileName);
            targetOperation.process(outFile);
        } catch (Exception e) {
            LogLayout.error(logger,appName,"接收文件和发送文件错误",e);
        } finally {
            if(is != null) {
                is.close();
            }
        }
        response.setStatus(HttpServletResponse.SC_OK);
    }

    public String write(InputStream is, String tempFileName) throws IOException {
//        String outFile = "F:/itp/data_t/" + appName + tempFileName;   //单元测试
//        String outFile = ChangeConfig.getBackPath() + "_t/" + appName + tempFileName;   //联合测试
        String outFile = ChangeConfig.getBackPath() + "/" + appName + tempFileName;     //正式版本
        doBeforeWrite(outFile);
        int len = 0;
        byte[] buff = new byte[StaticField.MB];
        FileOutputStream out = new FileOutputStream(outFile);
        while ((len = is.read(buff))!=-1) {
            out.write(buff,0,len);
        }
        out.flush();
        out.close();
        is.close();
        return outFile;
    }

    private void doBeforeWrite(String outFile) {
        if(StaticField.SystemPath.indexOf(":")>-1) {
            outFile = outFile.replace("\\","/");
        }
        String dir = outFile.substring(0,outFile.lastIndexOf("/"));
        File dirFile = new File(dir);
        if(!dirFile.exists()) {
            dirFile.mkdirs();
        }
    }
}
