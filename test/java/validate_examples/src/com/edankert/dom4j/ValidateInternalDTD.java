package validate_examples.src.com.edankert.dom4j;/*
 * Generated at : 09-May-2006 20:45:32
 *
 * Copyright (c) 2005 - 2006, Edwin Dankert 
 * All rights reserved. 
 */

import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import validate_examples.src.com.edankert.SimpleErrorHandler;

import java.io.File;

public class ValidateInternalDTD {

    public static void main(String[] args) {
        File file = new File("E://itp/contacts.xml");
        try {
            SAXReader reader = new SAXReader();
            reader.setValidation(true);
            reader.setErrorHandler(new SimpleErrorHandler());
            reader.read(file);
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }
}
