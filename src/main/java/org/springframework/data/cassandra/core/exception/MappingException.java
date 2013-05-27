package org.springframework.data.cassandra.core.exception;

/**
 * High level exception for reporting Bean to Cassandra Column Mapping Exceptions.
 * 
 * @author David Webb
 *
 */
public class MappingException extends Exception {

	/**
	 * @param message
	 */
	public MappingException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public MappingException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public MappingException(String message, Throwable cause) {
		super(message, cause);
	}

}
