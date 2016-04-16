package com.hzih.itp.platform.utils;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.CipherInputStream;
import java.io.InputStream;

/**
 * Created with IntelliJ IDEA.
 * User: 钱晓盼
 * Date: 13-5-16
 * Time: 下午3:32
 * To change this template use File | Settings | File Templates.
 */
public class SecurityUtils {


    public static InputStream encrypt(String security, int qd, InputStream is) {
        InputStream in = null;
        if(GenDESKey.DES.equals(security)) {
            switch (qd) {
                case 1:in = DES1(is,true);break;
                case 2:in = DES2(is,true);break;
                case 3:in = DES3(is,true);break;
                case 4:in = DES4(is,true);break;
                case 5:in = DES5(is,true);break;
            }
        } else if(GenAESKey.AES.equals(security)) {
            switch (qd) {
                case 1:in = AES1(is,true);break;
                case 2:in = AES2(is,true);break;
                case 3:in = AES3(is,true);break;
                case 4:in = AES4(is,true);break;
                case 5:in = AES5(is,true);break;
            }
        }
        return in;
    }

    public static InputStream decrypt(String security, int qd, InputStream is) {
        InputStream in = null;
        if(GenDESKey.DES.equals(security)) {
            switch (qd) {
                case 1:in = DES1(is,false);break;
                case 2:in = DES2(is,false);break;
                case 3:in = DES3(is,false);break;
                case 4:in = DES4(is,false);break;
                case 5:in = DES5(is,false);break;
            }

        } else if(GenAESKey.AES.equals(security)) {
            switch (qd) {
                case 1:in = AES1(is,false);break;
                case 2:in = AES2(is,false);break;
                case 3:in = AES3(is,false);break;
                case 4:in = AES4(is,false);break;
                case 5:in = AES5(is,false);break;
            }
        }
        return in;
    }

    /**
     *
     * @param is  流
     * @param isEn  true加密false解密
     * @return
     */
    public static InputStream AES1(InputStream is, boolean isEn) {
        if(isEn) {
            byte[] bytes = Base64.encodeBase64(GenDESKey.toByteArray(is));
            byte[] bytes1 = GenAESKey.encryptAES(GenAESKey.keyPath_1,bytes);
            return GenAESKey.byte2Input(bytes1);
        } else {
            byte[] bytes = GenAESKey.decryptAES(GenAESKey.keyPath_1,GenAESKey.toByteArray(is));
            byte[] bytes1 = Base64.decodeBase64(bytes);
            return GenAESKey.byte2Input(bytes1);
        }
    }
    public static InputStream AES2(InputStream is, boolean isEn) {
        if(isEn) {
            byte[] bytes = Base64.encodeBase64(GenDESKey.toByteArray(is));
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_1,bytes);
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_2,bytes);
            return GenAESKey.byte2Input(bytes);
        } else {
            byte[] bytes = GenAESKey.decryptAES(GenAESKey.keyPath_2,GenAESKey.toByteArray(is));
            bytes = GenAESKey.decryptAES(GenAESKey.keyPath_1,bytes);
            bytes = Base64.decodeBase64(bytes);
            return GenAESKey.byte2Input(bytes);
        }
    }
    public static InputStream AES3(InputStream is, boolean isEn) {
        if(isEn) {
            byte[] bytes = Base64.encodeBase64(GenDESKey.toByteArray(is));
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_1,bytes);
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_2,bytes);
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_3,bytes);
            return GenAESKey.byte2Input(bytes);
        } else {
            byte[] bytes = GenAESKey.decryptAES(GenAESKey.keyPath_3,GenAESKey.toByteArray(is));
            bytes = GenAESKey.decryptAES(GenAESKey.keyPath_2,bytes);
            bytes = GenAESKey.decryptAES(GenAESKey.keyPath_1,bytes);
            bytes = Base64.decodeBase64(bytes);
            return GenAESKey.byte2Input(bytes);
        }
    }
    public static InputStream AES4(InputStream is, boolean isEn) {
        if(isEn) {
            byte[] bytes = Base64.encodeBase64(GenDESKey.toByteArray(is));
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_1,bytes);
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_2,bytes);
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_3,bytes);
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_4,bytes);
            return GenAESKey.byte2Input(bytes);
        } else {
            byte[] bytes = GenAESKey.decryptAES(GenAESKey.keyPath_4,GenAESKey.toByteArray(is));
            bytes = GenAESKey.decryptAES(GenAESKey.keyPath_3,bytes);
            bytes = GenAESKey.decryptAES(GenAESKey.keyPath_2,bytes);
            bytes = GenAESKey.decryptAES(GenAESKey.keyPath_1,bytes);
            bytes = Base64.decodeBase64(bytes);
            return GenAESKey.byte2Input(bytes);
        }
    }
    public static InputStream AES5(InputStream is, boolean isEn) {
        if(isEn) {
            byte[] bytes = Base64.encodeBase64(GenDESKey.toByteArray(is));
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_1,bytes);
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_2,bytes);
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_3,bytes);
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_4,bytes);
            bytes = GenAESKey.encryptAES(GenAESKey.keyPath_5,bytes);
            return GenAESKey.byte2Input(bytes);
        } else {
            byte[] bytes = GenAESKey.decryptAES(GenAESKey.keyPath_5,GenAESKey.toByteArray(is));
            bytes = GenAESKey.decryptAES(GenAESKey.keyPath_4,bytes);
            bytes = GenAESKey.decryptAES(GenAESKey.keyPath_3,bytes);
            bytes = GenAESKey.decryptAES(GenAESKey.keyPath_2,bytes);
            bytes = GenAESKey.decryptAES(GenAESKey.keyPath_1,bytes);
            bytes = Base64.decodeBase64(bytes);
            return GenAESKey.byte2Input(bytes);
        }
    }

    /**
     *
     * @param is   流
     * @param isEn true加密false解密
     * @return
     */
    private static InputStream DES1(InputStream is, boolean isEn) {
        if(isEn){
            byte[] bytes = Base64.encodeBase64(GenDESKey.toByteArray(is));
            return GenDESKey.encryptDES(GenDESKey.keyPath_1,GenDESKey.byte2Input(bytes));
        } else {
            InputStream cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_1,is);
            byte[] bytes = Base64.decodeBase64(GenDESKey.toByteArray(cipIn));
            return GenDESKey.byte2Input(bytes);
        }
    }

    private static InputStream DES2(InputStream is, boolean isEn) {
        if(isEn){
            byte[] bytes = Base64.encodeBase64(GenDESKey.toByteArray(is));
            CipherInputStream cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_1,GenDESKey.byte2Input(bytes));
            return GenDESKey.decryptDES(GenDESKey.keyPath_2,cipIn);
        } else {
            InputStream cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_2,is);
            cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_1,cipIn);
            byte[] bytes = Base64.decodeBase64(GenDESKey.toByteArray(cipIn));
            return GenDESKey.byte2Input(bytes);
        }
    }

    private static InputStream DES3(InputStream is, boolean isEn) {
        if(isEn){
            byte[] bytes = Base64.encodeBase64(GenDESKey.toByteArray(is));
            CipherInputStream cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_1,GenDESKey.byte2Input(bytes));
            cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_2, cipIn);
            return GenDESKey.encryptDES(GenDESKey.keyPath_3, cipIn);
        } else {
            CipherInputStream cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_3,is);
            cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_2, cipIn);
            cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_1, cipIn);
            byte[] bytes = Base64.decodeBase64(GenDESKey.toByteArray(cipIn));
            return GenDESKey.byte2Input(bytes);
        }
    }

    private static InputStream DES4(InputStream is, boolean isEn) {
        if(isEn){
            byte[] bytes = Base64.encodeBase64(GenDESKey.toByteArray(is));
            CipherInputStream cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_1,GenDESKey.byte2Input(bytes));
            cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_2, cipIn);
            cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_3, cipIn);
            return GenDESKey.decryptDES(GenDESKey.keyPath_4, cipIn);
        } else {
            CipherInputStream cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_4,is);
            cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_3, cipIn);
            cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_2, cipIn);
            cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_1, cipIn);
            byte[] bytes = Base64.decodeBase64(GenDESKey.toByteArray(cipIn));
            return GenDESKey.byte2Input(bytes);
        }
    }

    private static InputStream DES5(InputStream is, boolean isEn) {
        if(isEn){
            byte[] bytes = Base64.encodeBase64(GenDESKey.toByteArray(is));
            CipherInputStream cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_1,GenDESKey.byte2Input(bytes));
            cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_2, cipIn);
            cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_3, cipIn);
            cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_4, cipIn);
            return GenDESKey.encryptDES(GenDESKey.keyPath_5, cipIn);
        } else {
            CipherInputStream cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_5,is);
            cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_4, cipIn);
            cipIn = GenDESKey.decryptDES(GenDESKey.keyPath_3, cipIn);
            cipIn = GenDESKey.encryptDES(GenDESKey.keyPath_2, cipIn);
            cipIn =  GenDESKey.decryptDES(GenDESKey.keyPath_1, cipIn);
            byte[] bytes = Base64.decodeBase64(GenDESKey.toByteArray(cipIn));
            return GenDESKey.byte2Input(bytes);
        }
    }
}
