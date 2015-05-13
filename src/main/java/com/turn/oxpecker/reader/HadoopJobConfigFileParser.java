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
