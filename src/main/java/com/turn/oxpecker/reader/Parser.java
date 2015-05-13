package com.turn.oxpecker.reader;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * Common interface to read XML files
 * @author jshum
 */
public abstract class Parser {

    public Logger logger;
    protected Element confroot;

    /**
     * @function: ConfigFileReader.parseXML
     * @doc: read in the xml file and parse it to an Element.
	 * @param xmlFile
	 * @return
	 * @throws java.io.FileNotFoundException
	 * @throws org.dom4j.DocumentException
	 */
    public Element getXmlRoot( String xmlFile ) throws FileNotFoundException, DocumentException {

		try {
			SAXReader xmlReader = new SAXReader();
			File url = new File(xmlFile);
			/* check if the XML file exists */
			if ( !url.exists() )
				throw new FileNotFoundException(String.format("Parser: config file %s does not exist.", xmlFile));
			/* parse XML file to get the root element */
			Document doc = xmlReader.read( url );
			return doc.getRootElement();

		} catch (FileNotFoundException e) {
			logger.fatal("File: "+xmlFile+" not found.", e);
			throw e;
		} catch (DocumentException e) {
			logger.fatal("File: "+xmlFile+" cannot be parsed by XMLReader.", e);
			throw e;
		}
	}

	public Element getConfroot() {
		return confroot;
	}

	public void setConfroot(Element confroot) {
		this.confroot = confroot;
	}
}
