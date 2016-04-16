package com.hzih.itp.platform.utils;

import com.hzih.logback.LogLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.io.*;
import java.security.SecureRandom;

/**
 * Created with IntelliJ IDEA.
 * User: 钱晓盼
 * Date: 13-5-15
 * Time: 下午4:56
 * To change this template use File | Settings | File Templates.
 */
public class GenDESKey {
    private static final Logger logger = LoggerFactory.getLogger(GenDESKey.class);
    public static final String DES = "DES";
	public static final String SKEY_NAME = "key.des";
    public static final String keyPath_1 = StaticField.SystemPath + "/security/des/des_1.dat";
    public static final String keyPath_2 = StaticField.SystemPath + "/security/des/des_2.dat";
    public static final String keyPath_3 = StaticField.SystemPath + "/security/des/des_3.dat";
    public static final String keyPath_4 = StaticField.SystemPath + "/security/des/des_4.dat";
    public static final String keyPath_5 = StaticField.SystemPath + "/security/des/des_5.dat";

	/**
	 * @param path ： 生成密钥的路径
     * @param securityFileName : 生成密钥文件名
	 * SecretKeyFactory 方式生成des密钥
	 * */
	public static String genKey(String path,String securityFileName) {
		// 密钥随机数生成
		SecureRandom sr = new SecureRandom();
		// byte[] bytes = {11,12,44,99,76,45,1,8};
		byte[] bytes = sr.generateSeed(20);
		// 密钥
		SecretKey skey = null;
        //生成密钥文件路径
		File file = genFile(path,securityFileName);

		try {
			//创建deskeyspec对象
			DESKeySpec desKeySpec = new DESKeySpec(bytes,9);

			//实例化des密钥工厂
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(DES);
			//生成密钥对象
			skey = keyFactory.generateSecret(desKeySpec);
			//写出密钥对象
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream(file));
			oos.writeObject(skey);
			oos.close();
            return file.getPath();
//			Log.sKeyPath(path);
//		} catch (NoSuchAlgorithmException e) {
//
//		} catch (InvalidKeyException e) {
//			e.printStackTrace();
//		} catch (InvalidKeySpecException e) {
//			e.printStackTrace();
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
		} catch (Exception e) {
			LogLayout.error(logger, "platform", "生成证书错误", e);
		}
        return null;
	}

    public static void main(String[] args) {
        String path = "F:/stp/security/des";
        for (int i=1;i<6;i++) {
            String securityFileName = "des_" + i +".dat";
           String keyName = genKey(path,securityFileName);
           System.out.println("生成了DES加密证书:"+keyName);
        }
    }

	private static File genFile(String path, String securityFileName) {
		String temp = null;
		File newFile = null;
		if (path.endsWith("/") || path.endsWith("\\")) {
			temp = path;
		} else {
			temp = path + "/";
		}

		File pathFile = new File(temp);
		if (!pathFile.exists())
			pathFile.mkdirs();

		newFile = new File(temp + securityFileName);

		return newFile;
	}

    public static CipherInputStream encryptDES(String keyPath,InputStream in) {
        SecretKey key = null;
        try {
            ObjectInputStream keyFile = new ObjectInputStream(
                    //读取加密密钥
                    new FileInputStream(keyPath));
            key = (SecretKey) keyFile.readObject();
            keyFile.close();
        } catch (FileNotFoundException e) {
            LogLayout.error(logger,"platform","DES密钥文件找不到",e);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","DES读取加密密钥错误",e);
        }
        //用key产生Cipher
        Cipher cipher = null;
        try {
            //设置算法,解密时应该同样是这样设置
            cipher = Cipher.getInstance(GenDESKey.DES);
            //设置解密模式
            cipher.init(Cipher.ENCRYPT_MODE, key);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","DES加密设置错误",e);
        }
        //取得要解密的文件并解密
        try {
            //输入流
            return new CipherInputStream(new BufferedInputStream(in), cipher);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","DES加密输入流读取错误",e);
        }
        return null;
    }

    /**
     * 解密
     * @param keyPath    密钥文件
     * @param in       解密后的文件liu
     */
    public static CipherInputStream decryptDES(String keyPath, InputStream in) {
        SecretKey key = null;
        try {
            ObjectInputStream keyFile = new ObjectInputStream(
                    //读取加密密钥
                    new FileInputStream(keyPath));
            key = (SecretKey) keyFile.readObject();
            keyFile.close();
        } catch (FileNotFoundException e) {
            LogLayout.error(logger,"platform","DES密钥文件找不到",e);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","DES读取加密密钥错误",e);
        }
        //用key产生Cipher
        Cipher cipher = null;
        try {
            //设置算法,应该与加密时的设置一样
            cipher = Cipher.getInstance(GenDESKey.DES);
            //设置解密模式
            cipher.init(Cipher.DECRYPT_MODE, key);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","DES加密设置错误",e);
        }
        //取得要解密的文件并解密
        try {
            //输入流
            return new CipherInputStream(new BufferedInputStream(in), cipher);
        } catch (Exception e) {
            LogLayout.error(logger,"platform","DES解密输入流读取错误",e);
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
            LogLayout.error(logger,"platform","DES stream to byte[] error!",e);
        }
        return output.toByteArray();
    }

    public static InputStream byte2Input(byte[] buf) {
		return new ByteArrayInputStream(buf);
	}

}
