/**
 * Copyright (C) 2015 Turn Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    private Logger logger;
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
			if ( !url.exists() ) {
				throw new FileNotFoundException(String.format("Parser: config file %s does not exist.", xmlFile));
			}
			/* parse XML file to get the root element */
			Document doc = xmlReader.read( url );
			return doc.getRootElement();

		} catch (FileNotFoundException e) {
			logger.error("File: "+xmlFile+" not found.", e);
			throw e;
		} catch (DocumentException e) {
			logger.error("File: "+xmlFile+" cannot be parsed by XMLReader.", e);
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
