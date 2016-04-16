package com.hzih.itp.platform.utils;

/**
 * Created by 钱晓盼 on 14-1-10.
 */
public class StaticField {

    public static final String SystemPath = System.getProperty("ichange.home");
    public static final String ExternalConfig = SystemPath + "/repository/external/config.xml";
    public static final String InternalConfig = SystemPath + "/repository/config.xml";
    public static final String ExternalHistoryConfig = SystemPath + "/repository/external/history/config.xml";
    public static final String InternalHistoryConfig = SystemPath + "/repository/history/config.xml";

    public static final String Str_LogBackConfig = SystemPath + "/others/logback.xml";
    public static final String Str_LogBackConfigTest = SystemPath + "/others/logback-test.xml";

    public static final String ChannelName = "1";
    public static final String AuditName = "A"; //审计信息通道名
    public static final String ConfigName = "C";//配置文件通道名

    public static final String Platform_Name_EX_ITP = "导入前置机";
    public static final String Platform_Name_EX_STP = "单向前置机";
    public static final String Platform_Name_IN_STP = "单向后置机";
    public static final String Platform_Name_IN_ITP = "导入后置机";

    public static final String Platform_Type_EX_ITP = "itp-ex";//导入前置机
    public static final String Platform_Type_EX_STP = "stp-ex";//单向设备前置机
    public static final String Platform_Type_IN_STP = "stp-in";//单向设备后置机
    public static final String Platform_Type_IN_ITP = "itp-in";//导入设备

    public static final int UDPPacketSizeMax = 1024*2 - 7;   //udp包的一包大小最大值
    public static final int UDPPacketSizeType = 1;           //udp包的类别区分代码长度
    public static final int UDPPacketSizeFileName = 19;     //udp包分包时总包的文件名长度 : /2014-01-17/09/1389935817734.tmp /*.zip
    public static final int UDPPacketSizeBodyMax = UDPPacketSizeMax - UDPPacketSizeFileName;         //udp包的一包内容大小最大值

    public static final String CONFIGFLAG = "C";     //配置文件标记
    public static final String BIZLOGFLAG = "B";     //业务日志文件标记
    public static final String EQULOGFLAG = "E";     //设备日志文件标记
    public static final String DEVICEFLAG = "D";     //设备信息
    public static final String DEVICERUNFLAG = "R";     //设备运行信息
    public static final String INITFLAG = "I";     //初始化标记

    /**
     * http包头参数名
     */
    public static final String Command = "Command";//发送命令类型的参数名
    public static final String SendConfig = "sendConfig";//发送配置文件命令
    public static final String AlertConfig = "alertConfig";//告警命令
    public static final String SendChannelTest = "sendChannelTest";//平台通道测试

    public static final int MB = 1024 * 1024;
    public static final int GB = 1024 * 1024 * 1024;

    /**
     * servlet path
     */
    public static final String FileReceiveServlet = "/fileReceive";

}
