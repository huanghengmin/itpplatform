package com.hzih.itp.platform.filechange.utils;

import com.hzih.logback.LogLayout;
import com.inetec.common.client.util.EChange;
import com.inetec.common.exception.E;
import com.inetec.common.exception.Ex;
import com.inetec.common.i18n.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 钱晓盼 on 14-1-13.
 */
public class KeywordsFilterFactory {
    private static final Logger logger = LoggerFactory.getLogger(KeywordsFilterFactory.class);
    private static final String keywordsFilter_Txt = KeyWordsFilter.class.getName();
    private static final String keywordsFilter_Rtf = KeywordsFilterRTF.class.getName();
    private static final String keywordsFilter_Word = KeywordsFilterWord.class.getName();

    public static KeywordsFilterUtil getKeywordsFilterUtil(String filename) {
        if (filename == null) {
            filename = "";
        }
        try {
            if (filename.endsWith(".doc") || filename.endsWith(".wps") || filename.endsWith(".docx")) {
                return (KeywordsFilterUtil) newObjectByClass(keywordsFilter_Word, KeywordsFilterUtil.class);
            }
            else if (filename.endsWith(".rtf")) {
                return (KeywordsFilterUtil) newObjectByClass(keywordsFilter_Rtf, KeywordsFilterUtil.class);
            }
            else  {
                return (KeywordsFilterUtil) newObjectByClass(keywordsFilter_Txt, KeywordsFilterUtil.class);
            }
        } catch (Ex ex) {
            LogLayout.error(logger, "platform", "KeywordsFilterUtil create object by filename is:" + filename, ex);
        }
        return null;
    }

    public static Object newObjectByClass(String classname, Class cls) throws Ex {
        // Make a class object with the plug-in name
        Class c = null;
        try {
            c = Class.forName(classname, true, KeywordsFilterFactory.class.getClassLoader());
        } catch (ClassNotFoundException Ex) {
            throw new Ex().set(E.E_Unknown, new Message("Class not found:{0} ", Ex.getMessage()));
        }
        // Make sure c implements the required interfaces:
        if (!cls.isAssignableFrom(c)) {
            throw new Ex().set(EChange.E_CF_InterfaceNotImplemented, new Message("The  class  does not implement the required interface {0}", cls.getName()));
        }
        // Now, ready to create an instance.
        Object fr = null;
        try {
            fr = c.newInstance();
        } catch (InstantiationException Ex) {
            throw new Ex().set(EChange.E_UNKNOWN, new Message("Failed to instantiate class: ", Ex.getMessage()));
        } catch (IllegalAccessException Ex) {
            throw new Ex().set(EChange.E_UNKNOWN, new Message("Failed to instantiate class; access exception: ", Ex.getMessage()));
        }
        return fr;
    }
}
