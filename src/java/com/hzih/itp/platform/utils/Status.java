package com.hzih.itp.platform.utils;

import com.inetec.common.i18n.Key;

/**
 * 状态信息类，包括状态信息，和一些定义好的状态信息。
 *
 * @author inetec System
 * @version 1.0
 */

public class Status {
    /**
     * 数据发送成功的编码
     */

    public static final int I_Success_SendData = 1000;
    /**
     * 数据发送成功的信息关键字（K对象）
     */
    public static final Key K_Success_SendData = new Key("data send succeed. ");
    /**
     * 数据发送成功的状态对象
     */
    public static final Status S_Success_SendData = new Status(I_Success_SendData, K_Success_SendData);
    /**
     * 数据发送成功的编码
     */
    public static final int I_Success_DataReceiver = 1100;
    /**
     * 数据接收成功的信息关键字（K对象）
     */
    public static final Key K_Success_DataReceiver = new Key("data receiver  succeed.");
    /**
     * 数据接收成功的状态对象
     */
    public static final Status S_Success_Data_Recevier = new Status(I_Success_DataReceiver, K_Success_DataReceiver);
    /**
     * 数据接收成功的编码
     */
    public static final int I_Success_StartSourcePlugin = 1200;
    /**
     * 启动输入适配器成功信息关键字（K对象）
     */
    public static final Key K_Success_StartSourcePlugin = new Key("Start Source Plugin succeed. ");
    /**
     * 启动输入适配器成功的状态对象
     */
    public static final Status S_Success_StartSourcePlugin = new Status(I_Success_StartSourcePlugin, K_Success_StartSourcePlugin);
    /**
     * 启动输入适配器成功的编码
     */

    public static final int I_Success_SendControlCommand = 1300;
    /**
     * 发送控制命令成功的信息关键字（K对象）
     */
    public static final Key K_Success_SendControlCommand = new Key("send control command succeed.");
    /**
     * 发送控制命令成功的状态对象
     */
    public static final Status S_Success_SendControlCommand = new Status(I_Success_SendControlCommand, K_Success_SendControlCommand);
    /**
     * 输出处理操作成功的编码
     */
    public static final int I_Success_TargetProcess = 1400;
    /**
     * 输出处理操作成功的信息关键字（K对象）
     */
    public static final Key K_Success_TargetProcess = new Key("target process succeed.");
    /**
     * 输出处理操作成功的状态对象
     */
    public static final Status S_Success_TargetProcess = new Status(I_Success_TargetProcess, K_Success_TargetProcess);
    /**
     * 数据发送失败的编码
     */
    public static final int I_Faild_SendData = -1000;
    /**
     * 数据发送失败的信息关键字（K对象）
     */
    public static final Key K_Faild_SendData = new Key("send data faild.");
    /**
     * 数据发送失败的状态对象
     */
    public static final Status S_Faild_SendData = new Status(I_Faild_SendData, K_Faild_SendData);
    /**
     * 发送数据失败（IO错误）的编码
     */
    public static final int I_Faild_SendData_IO_Error = -1002;
    /**
     * 发送数据失败（IO错误）的信息关键字（K对象）
     */
    public static final Key K_Faild_SendData_IO_Error = new Key("send data faild of io error.");
    /**
     * 发送数据失败（IO错误）的状态对象
     */
    public static final Status S_Faild_SendData_IO_Error = new Status(I_Faild_SendData_IO_Error, K_Faild_SendData_IO_Error);

    /**
     * 写文件失败的编码
     */
    public static final int I_Faild_WriteFile = -1101;
    /**
     * 写文件失败的信息关键字（K对象）
     */
    public static final Key K_Faild_WriteFile = new Key("write file faild .");
    /**
     * 写文件失败的状态对象
     */
    public static final Status S_Faild_WriteFile = new Status(I_Faild_WriteFile, K_Faild_WriteFile);
    /**
     * 启动输入适配器失败的编码
     */
    public static final int I_StartSourcePlugin_Faild= -1200;
    /**
     * 启动输入适配器失败的信息关键字（K对象）
     */
    public static final Key K_StartSourcePlugin_Faild = new Key("start source plugin faild .");
    /**
     * 启动输入适配器失败的状态对象
     */
    public static final Status S_StartSourcePlugin_Faild = new Status(I_StartSourcePlugin_Faild, K_StartSourcePlugin_Faild);
    /*
     * 发送控制命令失败的编码
     */
    public static final int I_Faild_SendControlCommand = -1300;
    /**
     * 发送控制命令失败的信息关键字（K对象）
     */
    public static final Key K_Faild_SendControlCommand = new Key("send control command faild .");
    /**
     * 发送控制命令失败的状态对象
     */
    public static final Status S_Faild_SendControlCommand = new Status(I_Faild_SendControlCommand, K_Faild_SendControlCommand);
    /*
     * 输出处理失败的编码
     */
    public static final int I_Faild_TargetProcess = -1400;
    /**
     * 输出处理失败的信息关键字（K对象）
     */
    public static final Key K_Faild_TargetProcess = new Key(" output process faild .");
    /**
     * 输出处理失败的状态对象
     */
    public static final Status S_Faild_TargetProcess = new Status(I_Faild_TargetProcess, K_Faild_TargetProcess);
    /**
     * 成功的编码
     */
    public static final int I_Success = 10000;
    /**
     * 成功的信息关键字（K对象）
     */
    public static final Key K_Success = new Key("successed.");
    /**
     * 成功的状态对象
     */
    public static final Status S_Success = new Status(I_Success, K_Success);



    /**
     * 失败的编码
     */
    public static final int I_Faild = -10000;
    /**
     * 失败的信息关键字（K对象）
     */
    public static final Key K_Faild = new Key("faild .");
    /**
     * 失败的状态对象
     */
    public static final Status S_Faild = new Status(I_Faild, K_Faild);
    /**
     * 不可知状态的编码
     */
    public final static int I_Unknown = -1;
    /**
     * 不可知状态的信息关键字（K对象）
     */
    public static final Key K_Unknown = new Key("unknown.");
    /**
     * 不可知状态的状态对象
     */
    public static final Status S_Unknown = new Status(I_Unknown, K_Unknown);
    private Key m_key = new Key("Unknown ");
    private int m_statusCode = I_Unknown;

    /**
     * 取得当前状态对象的状态编码
     *
     * @return 返回对应整型编码。
     */
    public int getStatusCode() {
        return m_statusCode;
    }

    /**
     * 取得当前状态对象的状态信息
     *
     * @return 返回对应的状态信息（字符串）
     */
    public String getMessage() {
        return m_key.toString();
    }

    /**
     * 查看当前状态是否为成功状态。
     *
     * @return 如是成功状态，返回真，反之，返回假
     */
    public boolean isSuccess() {
        if (m_statusCode > 0)
            return true;
        else
            return false;
    }

    private Status(int statusCode, Key key) {
        m_key = key;
        m_statusCode = statusCode;
    }

    /**
     * 根据状态编码，查询对应用状态。
     *
     * @param id  状态编码
     * @param Key 状态信息（K对象）
     * @return 状态对象
     */
    public static Status query(int id, Key Key) {
        return new Status(id, Key);
    }

}
