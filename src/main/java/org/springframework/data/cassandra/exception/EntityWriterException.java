/**
 * All BrightMove Code is Copyright 2004-2013 BrightMove Inc.
 * Modification of code without the express written consent of
 * BrightMove, Inc. is strictly forbidden.
 *
 * Author: David Webb (dwebb@brightmove.com)
 * Created On: Nov 12, 2013 
 */
package org.springframework.data.cassandra.exception;

/**
 * Exception to handle failing to write a PersistedEntity to a CQL String or Query object
 * 
 * @author David Webb (dwebb@brightmove.com)
 *
 */
public class EntityWriterException extends Exception {
	
	/**
	 * @param message
	 */
	public EntityWriterException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public EntityWriterException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public EntityWriterException(String message, Throwable cause) {
		super(message, cause);
	}

}
