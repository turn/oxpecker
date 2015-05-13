package com.turn.oxpecker.reader;

import org.dom4j.Element;

/**
 * @author jshum
 */
public class ParserUtil {

	public static Object parseRequiredNode(Element parentNode, String nodeName) throws HadoopJobParseException {
		Element node = parentNode.element(nodeName);
		if (node == null)
			throw new HadoopJobParseException(String.format("<%s> is not set in the config file.",nodeName));

		return node;
	}
}
