/*
 * Generated at : 09-May-2006 20:45:32
 *
 * Copyright (c) 2005 - 2006, Edwin Dankert 
 * All rights reserved. 
 */

package validate_examples.src.com.edankert.xom;

import nu.xom.Builder;
import nu.xom.ParsingException;
import nu.xom.ValidityException;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import validate_examples.src.com.edankert.SimpleErrorHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;

public class ValidateInternalDTD {

    public static void main(String[] args) {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setValidating(true);
            
            XMLReader reader = factory.newSAXParser().getXMLReader();
            reader.setErrorHandler(new SimpleErrorHandler());

            Builder builder = new Builder(reader);
            builder.build("contacts.xml");
        } catch (ValidityException e) {
            e.printStackTrace();
        } catch (ParsingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
    }
}
