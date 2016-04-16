package com.hzih.itp.platform.dbchange.target.utils;

import com.hzih.itp.platform.dbchange.datautils.db.*;
import com.hzih.itp.platform.dbchange.datautils.dboperator.DbopUtil;
import com.hzih.logback.LogLayout;
import com.inetec.common.exception.E;
import com.inetec.common.exception.Ex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;
import sun.management.resources.agent;

import java.io.IOException;
import java.io.InputStream;


public class XmlSaxParser extends DefaultHandler {

    /**
     * Validation feature id (http://Exml.org/saEx/features/validation).
     */
    protected static final String VALIDATION_FEATURE_ID = "http://xml.org/sax/features/validation";
    /**
     * LeExical handler property id (http://Exml.org/saEx/properties/leExical-handler).
     */
    protected static final String LEExICAL_HANDLER_PROPERTY_ID = "http://xml.org/sax/properties/lexical-handler";

    /**
     * Default parser name.
     */
    protected static final String DEFAULT_PARSER_NAME = "org.apache.xerces.parsers.SAXParser";

    private final static Logger logger = LoggerFactory.getLogger(XmlSaxParser.class);
    private Rows m_rows = new Rows();
    private Row m_curRow;
    private Column m_curColumn;
    private String m_lastData = "";


    //Constructor
    public XmlSaxParser() {
        super();
    }


    public Rows parse(InputStream is) throws Ex {

        try {
            //SAExParserFactory factory = SAExParserFactory.newInstance();
            XMLReader ExmlReader = XMLReaderFactory.createXMLReader(DEFAULT_PARSER_NAME);
            //factory.setValidating(false);
            //SAExParser parser = factory.newSAExParser();
            //ExMLReader ExmlReader = parser.getExMLReader();
            ExmlReader.setFeature(VALIDATION_FEATURE_ID, false);
            //ExmlReader.setProperty(LEExICAL_HANDLER_PROPERTY_ID, false);
            ExmlReader.setContentHandler(this);
            ExmlReader.setErrorHandler(this);
            InputSource inputSource = new InputSource(is);
            ExmlReader.parse(inputSource);
        } catch (IOException io) {
            throw new Ex().set(E.E_IOException, io);
        } catch (SAXException eExc) {
            throw new Ex().set(eExc);
        }
        return m_rows;
    }

    //Response the startDocument event
    public void startDocument() {
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger, "start document");
        }
    }

    public void endDocument() {
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,"end document");
        }
    }

    public void startElement(String uri, String localName, String qName, Attributes attrs) {
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,"satart element:" + qName);
        }
        if (qName.equalsIgnoreCase("row")) {
            m_curRow = new Row(attrs.getValue("database"), attrs.getValue("table"));
            String operator = attrs.getValue(Operator.Str_Operator);
            m_curRow.setAction(Operator.getOperator(operator));
            String op_time = attrs.getValue("op_time");
            m_curRow.setOp_time(new Long(op_time).longValue());
        }
        if (qName.equalsIgnoreCase("field")) {
            String name = attrs.getValue("name");
            String jdbcType = attrs.getValue("jdbctype");
            String dbType = attrs.getValue("dbtype");
            String ispk = attrs.getValue("ispk");
            String isnull = attrs.getValue("isnull");
            m_curColumn = new Column(name, DbopUtil.getJdbcType(jdbcType), dbType, ispk.equals("true"));
            if (!isnull.equals("false")) {
                m_curColumn.setValue(null);
            } else {
                m_curColumn.setValue(new Value(""));
            }
        }
    }

    public void characters(char[] ch, int start, int length) {
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,"read data:" + new String(ch, start, length));
        }
        m_lastData = m_lastData + new String(ch, start, length);
    }

    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (logger.isDebugEnabled()) {
            LogLayout.debug(logger,"end element:" + qName);
        }
        if (qName.equalsIgnoreCase("row")) {
            m_rows.addRow(m_curRow);
        }
        if (qName.equalsIgnoreCase("field")) {
            // if (!m_curColumn.isNull()) {
            int len = m_lastData.length();
            //m_lastData = m_lastData.trim();
            if (!m_curColumn.isNull()) {
                if (!m_lastData.equals("")) {
                    if (m_lastData.charAt(0) != '\"' || m_lastData.charAt(len - 1) != '\"') {
                        LogLayout.warn(logger,"The data in the xml is not complete:" + m_lastData);
                    } else {
                        m_lastData = m_lastData.substring(1, len - 1);
                    }
                    m_curColumn.setValue(new Value(m_lastData.trim()));
                } // else {}
            }  // else {}
            m_lastData = "";

            m_curRow.addColumn(m_curColumn);
        }
    }

    public void fatalError(SAXParseException e) {
        LogLayout.error(logger,"platform","faltal error of xml sax parser.", e);
    }

    public void error(SAXParseException e) {
        LogLayout.error(logger,"platform","error of xml sax parser.", e);
    }

    public void warning(SAXParseException e) {
        LogLayout.warn(logger,"platform","faltal error of xml sax parser."+ e.getMessage());
        LogLayout.error(logger,"platform","faltal error of xml sax parser."+ e.getMessage(),e);
    }


}
