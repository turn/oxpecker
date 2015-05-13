package com.turn.oxpecker.reader;;

/**
 * @author jshum
 */
public class HadoopJobParseException extends Exception{

	public HadoopJobParseException(String message) {
		super(message);
	}

	public HadoopJobParseException(String message, Throwable cause){
		super(message, cause);
	}

}
