package com.hzih.itp.platform.utils;

import com.hzih.logback.LogLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: 钱晓盼
 * Date: 13-5-17
 * Time: 下午12:30
 * To change this template use File | Settings | File Templates.
 */
public class GenAESKey {
    private static final Logger logger = LoggerFactory.getLogger(GenAESKey.class);
    public static final String AES = "AES";
    public static final String keyPath_1 = StaticField.SystemPath + "/security/aes/aes_1.dat";
    public static final String keyPath_2 = StaticField.SystemPath + "/security/aes/aes_2.dat";
    public static final String keyPath_3 = StaticField.SystemPath + "/security/aes/aes_3.dat";
    public static final String keyPath_4 = StaticField.SystemPath + "/security/aes/aes_4.dat";
    public static final String keyPath_5 = StaticField.SystemPath + "/security/aes/aes_5.dat";

    /**
	 * 生成密钥
	 * 自动生成AES128位密钥
	 * 传入保存密钥文件路径
	 * filePath 表示文件存储路径加文件名；例如d:\aes.txt
	 * @throws java.security.NoSuchAlgorithmException
	 * @throws java.io.IOException
	 */
	public static String getAutoCreateAESKey(String filePath){
        FileOutputStream fos = null;
        try{
            KeyGenerator kg = KeyGenerator.getInstance(GenAESKey.AES);
            kg.init(128);//要生成多少位，只需要修改这里即可128, 192或256
            SecretKey sk = kg.generateKey();
            byte[] b = sk.getEncoded();
            fos = new FileOutputStream(filePath);
            fos.write(b);
            fos.flush();
        } catch (Exception e) {
            LogLayout.error(logger, "platform", "生成AES错误", e);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                }
            }
        }
        return filePath;
	}

    public static void main(String[] args) {
        String path = "F:/stp/security/aes";
        for (int i=1;i<6;i++) {
            String securityFileName = "aes_" + i +".dat";
           String keyName = getAutoCreateAESKey(path+"/"+securityFileName);
           System.out.println("生成了AES加密证书:"+keyName);
        }
    }

    /**
	 * 加密
	 * 使用对称密钥进行加密
	 * keyFilePath 密钥存放路径
	 * text 要加密的字节数组
	 * 加密后返回一个字节数组
	 * @throws java.io.IOException
	 * @throws javax.crypto.NoSuchPaddingException
	 * @throws java.security.NoSuchAlgorithmException
	 * @throws java.security.InvalidKeyException
	 * @throws javax.crypto.BadPaddingException
	 * @throws javax.crypto.IllegalBlockSizeException
	 */
	public static byte[] encryptAES(String keyFilePath,byte[] text) {
		File file = new File(keyFilePath);
        byte[] key = new byte[(int) file.length()];
        FileInputStream fis = null;
        try{
            fis = new FileInputStream(file);
            fis.read(key);
            SecretKeySpec sKeySpec = new SecretKeySpec(key, GenAESKey.AES);
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, sKeySpec);
            return cipher.doFinal(text);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","AES加密错误",e);
        } finally {
            if(fis!=null){
                try {
                    fis.close();
                } catch (IOException e) {
                }
            }
        }
        return null;
	}

	/**
	 * 解密
	 * 使用对称密钥进行解密
	 * keyFilePath 密钥存放路径
	 * text 要解密的字节数组
	 * 解密后返回一个字节数组
	 * @throws java.io.IOException
	 * @throws javax.crypto.NoSuchPaddingException
	 * @throws java.security.NoSuchAlgorithmException
	 * @throws java.security.InvalidKeyException
	 * @throws javax.crypto.BadPaddingException
	 * @throws javax.crypto.IllegalBlockSizeException
	 */
	public static byte[] decryptAES(String keyFilePath,byte[] text) {
		File file = new File(keyFilePath);
		byte[] key = new byte[(int) file.length()];
        FileInputStream fis = null;
        try{
            fis = new FileInputStream(file);
            fis.read(key);
            SecretKeySpec sKeySpec = new SecretKeySpec(key, GenAESKey.AES);
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, sKeySpec);
            return cipher.doFinal(text);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","AES解密错误",e);
        } finally {
            if(fis!=null){
                try {
                    fis.close();
                } catch (IOException e) {
                }
            }
        }
        return null;
	}

    public static byte[] toByteArray(InputStream input) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int n = 0;
        try {
            while (-1 != (n = input.read(buffer))) {
                output.write(buffer, 0, n);
            }
            output.flush();
        } catch (IOException e) {
            LogLayout.error(logger,"platform","AES stream to byte[] error!",e);
        }
        return output.toByteArray();
    }

    public static InputStream byte2Input(byte[] buf) {
		return new ByteArrayInputStream(buf);
	}
}
