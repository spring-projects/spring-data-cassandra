package org.springframework.cassandra.core;

import com.datastax.driver.core.ResultSet;

/**
 * Listener used to receive asynchronous results expected as an object of type <code>T</code>.
 * 
 * @author Matthew T. Adams
 * @param <T>
 */
public interface QueryForObjectListener<T> {

	/**
	 * Called upon query completion.
	 */
	void onQueryComplete(T result);

	/**
	 * Called if an exception is raised while getting or converting the {@link ResultSet}.
	 */
	void onException(Exception x);
}
