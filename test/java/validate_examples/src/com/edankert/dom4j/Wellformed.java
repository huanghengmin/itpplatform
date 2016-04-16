/*
 * Generated at : 09-May-2006 20:45:32
 *
 * Copyright (c) 2005 - 2006, Edwin Dankert 
 * All rights reserved. 
 */

package validate_examples.src.com.edankert.dom4j;

import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import validate_examples.src.com.edankert.SimpleErrorHandler;

public class Wellformed {

    public static void main(String[] args) {
        try {
            SAXReader reader = new SAXReader();
            reader.setValidation(false);
            reader.setErrorHandler(new SimpleErrorHandler());
            reader.read("contacts.xml");
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }
}
