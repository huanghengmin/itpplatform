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
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;

public class ValidateInternalDTDAndExternalSchema {

    public static void main(String[] args) {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setValidating(true);
            
            SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
            factory.setSchema(schemaFactory.newSchema(new Source[] {new StreamSource("contacts.xsd")}));

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
