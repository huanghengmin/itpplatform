import java.io.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

public class DOMValidateDTD {

	public static void main(String args[]) {
		try{
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setValidating(true);
			DocumentBuilder builder = factory.newDocumentBuilder();
			builder.setErrorHandler(new ErrorHandler() {
				//Ignore the fatal errors
				public void fatalError(SAXParseException e)throws SAXException {
                    System.out.println(e.getMessage());
                    System.exit(0);
                }
				//Validation errors
				public void error(SAXParseException e)throws SAXParseException {
					System.out.println("Error at " +e.getLineNumber() + " line.");
					System.out.println(e.getMessage());
					System.exit(0);
				}
				//Show warnings
				public void warning(SAXParseException e)throws SAXParseException{
					System.out.println(e.getMessage());
					System.exit(0);
				}
			});
			Document xmlDocument = builder.parse(new FileInputStream("E:\\fartec\\ichange\\gdhzca\\xmlvalidate\\Employeexy.xml"));
			DOMSource source = new DOMSource(xmlDocument);
			StreamResult result = new StreamResult(System.out);
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, "E:\\fartec\\ichange\\gdhzca\\xmlvalidate\\Employee.dtd");
			transformer.transform(source, result);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}



}
