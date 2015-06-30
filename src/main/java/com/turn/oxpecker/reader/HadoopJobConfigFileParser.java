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
package com.turn.oxpecker.reader;;

import com.turn.oxpecker.instrumentation.HadoopJob;
import static com.turn.oxpecker.reader.Constant.NAME;
import static com.turn.oxpecker.reader.Constant.PROPERTY;
import static com.turn.oxpecker.reader.Constant.VALUE;

import java.io.FileNotFoundException;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.dom4j.DocumentException;
import org.dom4j.Element;

/**
 * Parses properties file for HDFS configurations
 * @author jshum
 */
public class HadoopJobConfigFileParser extends Parser {

	public static Logger LOGGER = Logger.getLogger(HadoopJobConfigFileParser.class);

	public HadoopJobConfigFileParser(String configFile) throws FileNotFoundException, DocumentException {
		setConfroot(getXmlRoot(configFile));
	}

	public void addJobConfToHadoopJob(HadoopJob hj) throws HadoopJobParseException {
		if (getConfroot() == null)
			throw new HadoopJobParseException("confroot is null.");

		for ( Object ele: confroot.elements(PROPERTY)) {
			Element propNode = (Element) ele;
			Element	nameNode = (Element) ParserUtil.parseRequiredNode(propNode, NAME);
			Element	valueNode = (Element) ParserUtil.parseRequiredNode(propNode, VALUE);

			hj.addConfigValue(nameNode.getText(), valueNode.getText());
		}
	}

	public HashMap<String, String> getJobConf() throws HadoopJobParseException {
		HashMap<String, String> hm = new HashMap<String, String>();
		if (getConfroot() == null)
			throw new HadoopJobParseException("confroot is null.");
		
		for ( Object ele: confroot.elements(PROPERTY)) {
            Element propNode = (Element) ele;
    		Element	nameNode = (Element) ParserUtil.parseRequiredNode(propNode, NAME);
    		Element	valueNode = (Element) ParserUtil.parseRequiredNode(propNode, VALUE);

    		hm.put(nameNode.getText(), valueNode.getText());
		}
		return hm;
	}
}
