/*
 	This file is part of XML2DTD.

    XML2DTD is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as 
    published by the Free Software Foundation. 

    XML2DTD is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General 
    Public License along with XML2DTD.  If not, see 
    <http://www.gnu.org/licenses/>.
    
 */
package org.xml2dtd;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;


/**
 * The Class Main.
 */
public class Main {
	/**
	 * The main method.
	 * 
	 * @param args the arguments
	 */
	public static void main(String[] args) {
        String inXml = "E:\\fartec\\ichange\\gdhzca\\xmlvalidate\\Employeexy.xml";
        String outDtd = "E:\\fartec\\ichange\\gdhzca\\xmlvalidate\\Employeexy.dtd";;
        XML2DTD xml2Dtd = new XML2DTD();
        try {
			String result = xml2Dtd.run(inXml);
            PrintWriter out = new PrintWriter(new FileWriter(new File(outDtd)));
            out.print(result);
            out.close();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
