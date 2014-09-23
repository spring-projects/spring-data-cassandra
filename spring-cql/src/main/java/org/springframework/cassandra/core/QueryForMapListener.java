package org.springframework.cassandra.core;

import java.util.Map;

import com.datastax.driver.core.ResultSet;

/**
 * Listener used to receive asynchronous results expected as a <code>List&lt;Map&lt;String,Object&gt;&gt;</code>.
 * 
 * @author Matthew T. Adams
 * @param <T>
 */
public interface QueryForMapListener {

	/**
	 * Called upon query completion.
	 */
	void onQueryComplete(Map<String, Object> results);

	/**
	 * Called if an exception is raised while getting or converting the {@link ResultSet}.
	 */
	void onException(Exception x);
}
