package com.hzit.itp.platform.formcheck.dom4j;

import com.hzih.logback.LogLayout;
import com.hzit.itp.platform.FileCheck;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.io.SAXValidator;
import org.dom4j.util.XMLErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 14-5-27
 * Time: 下午1:50
 * To change this template use File | Settings | File Templates.
 */
public class ValidataXML {
    final static Logger logger = LoggerFactory.getLogger(ValidataXML.class);

    private String xmlFileName;
    private String xsdFileName;

    private InputStream in;

    public String getXmlFileName() {
        return xmlFileName;
    }

    public void setXmlFileName(String xmlFileName) {
        this.xmlFileName = xmlFileName;
    }

    public String getXsdFileName() {
        return xsdFileName;
    }

    public void setXsdFileName(String xsdFileName) {
        this.xsdFileName = xsdFileName;
    }

    public ValidataXML() {
    }

    public ValidataXML(String xmlFileName, String xsdFileName) {
        this.xmlFileName = xmlFileName;
        this.xsdFileName = xsdFileName;
    }

    public ValidataXML(InputStream in,String xsdFileName){
        this.in = in;
        this.xsdFileName = xsdFileName;
    }
    /**
     * 通过 XSD （ XML Schema ）校验 XML
     */
    public String validateXMLByXSD() {
        String json = null;
        try {
            // 创建默认的 XML 错误处理器
            XMLErrorHandler errorHandler = new XMLErrorHandler();
            // 获取基于 SAX 的解析器的实例
            SAXParserFactory factory = SAXParserFactory.newInstance();
            // 解析器在解析时验证 XML 内容。
            factory.setValidating(true);
            // 指定由此代码生成的解析器将提供对 XML 名称空间的支持。
            factory.setNamespaceAware(true);
            // 使用当前配置的工厂参数创建 SAXParser 的一个新实例。
            SAXParser parser = factory.newSAXParser();
            // 创建一个读取工具
            SAXReader xmlReader = new SAXReader();
            // 获取要校验 xml 文档实例
//            Document xmlDocument = (Document) xmlReader.read(
//                    new InputStreamReader(new FileInputStream(xmlFileName)));
            Document xmlDocument = null;
            if(in != null){
                xmlDocument = (Document)xmlReader.read(in);
            }else {
                xmlDocument = (Document)xmlReader.read(new File(xmlFileName));
            }
//            Document xmlDocument = (Document)xmlReader.read();
            //xml内容校验
            json = xmlContentCheck(xmlDocument);
//            testxmlContent(xmlDocument);

            /*if(json.indexOf("success") <= -1){
                //内容校验不通过
                json = "xml文件 " + xmlFileName + " 内容校验不通过 " + json;
                LogLayout.error(logger, "xmlcheck", json);
                return json;
            }*/


//            List<Element> list = root.getChildren();//获得根节点的子节点
            // 设置 XMLReader 的基础实现中的特定属性。核心功能和属性列表可以在
            //[url]http://sax.sourceforge.net/?selected=get-set[/url] 中找到。

            //查找xml命名空间
            String xmlSchema = getXmlSchema();
            if(xmlSchema == null || xmlSchema.length() <= 0){
                xmlSchema = "http://www.w3.org/2001/XMLSchema";
            }
            parser.setProperty(
                    "http://java.sun.com/xml/jaxp/properties/schemaLanguage",
                    xmlSchema);
            parser.setProperty(
                    "http://java.sun.com/xml/jaxp/properties/schemaSource",
                    "file:" + xsdFileName);
            // 创建一个 SAXValidator 校验工具，并设置校验工具的属性
            SAXValidator validator = new SAXValidator(parser.getXMLReader());
            // 设置校验工具的错误处理器 ， 当发生错误时 ， 可以从处理器对象中得到错误信息。
            validator.setErrorHandler(errorHandler);
            // 校验
            validator.validate(xmlDocument);
//            XMLWriter writer = new XMLWriter(OutputFormat.createPrettyPrint());
            // 如果错误信息不为空，说明校验失败，打印错误信息
            if (errorHandler.getErrors().hasContent()) {
                System.out.println("XML 文件通过 XSD 文件校验失败！ ");
                json = "XML 文件通过 XSD 文件校验失败！ ";
//                writer.write(errorHandler.getErrors());
                LogLayout.error(logger, "xmlcheck", "XML 文件通过 XSD 文件校验失败！ " + errorHandler.getErrors().getText());
            } else {
                System.out.println("Good! XML 文件通过 XSD 文件校验成功！ ");
                LogLayout.info(logger, "xmlcheck", "XML 文件通过 XSD 文件校验成功！ ");
                json = "success";
            }
        } catch (Exception ex) {
//            System.out.println("XML 文件 : " + xmlFileName + " 通过 XSD 文件 :" +
//                    xsdFileName + " 检验失败。 \n 原因： " + ex.getMessage());
            json = "XML 文件通过 XSD 文件校验失败！ ";
            LogLayout.error(logger, "xmlcheck", "XML 文件 : " + xmlFileName + " 通过 XSD 文件 :" +
                    xsdFileName + " 检验失败。", ex);
            ex.printStackTrace();
        }
        return json;
    }

    /**
     * 通过 DTD 校验 XML
     */
    public static void validateXMLByDTD() {
            //todo ：暂时不用，以后再说吧
    }


    private String getXmlSchema(){
        Properties prop = new Properties();
        String xmlSchema = null;
        try{
            prop.load(ValidataXML.class.getResourceAsStream("/config.properties"));
            xmlSchema = prop.getProperty("xmlSchema");
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return xmlSchema;
    }

    private void testxmlContent(Document document){
        Element root = document.getRootElement();
        System.out.println("Root: " + root.getName());

        // 获取所有子元素
        List<Element> childList = root.elements();
        System.out.println("total child count: " + childList.size());
        for(Element e:childList){
            System.out.println("name:" + e.getName() + ",value:" + e.getText());
        }
    }

    private String xmlContentCheck(Document document){
        String json = "success";
        // 获取根元素
        Element root = document.getRootElement();
//        System.out.println("Root: " + root.getName());

        // 获取所有子元素
        List<Element> childList = root.elements();
//        System.out.println("total child count: " + childList.size());
        for(Element e:childList){
//            System.out.println("name:" + e.getName() + ",value:" + e.getText());
            while (FileCheck.blackWords.isSelect()){
                try {
                    LogLayout.info(logger,"xmlcheck","正在查询黑名单,等待1s. . .");
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
            if(FileCheck.blackWords.blackWords.containsKey(e.getTextTrim())){
                json = "非法内容:" + e.getTextTrim();
                break;
            }
        }
        return json;
    }

    public static void main(String[] args) {
        String xmlFileName = "E://itp/ztt.xml";
        String xsdFileName = "E://itp/ztt.xsd";
        try {
            InputStream in = new FileInputStream(new File(xmlFileName));
            ValidataXML validataXML = new ValidataXML(in,xsdFileName);
            validataXML.validateXMLByXSD();
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

//        validateXMLByDTD();
    }

}
