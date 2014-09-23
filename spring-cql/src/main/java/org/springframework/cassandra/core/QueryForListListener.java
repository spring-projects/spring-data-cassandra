package org.springframework.cassandra.core;

import java.util.List;

import com.datastax.driver.core.ResultSet;

/**
 * Listener used to receive asynchronous results expected as a <code>List&lt;T&gt;</code>.
 * 
 * @author Matthew T. Adams
 * @param <T>
 */
public interface QueryForListListener<T> {

	/**
	 * Called upon query completion.
	 */
	void onQueryComplete(List<T> results);

	/**
	 * Called if an exception is raised while getting or converting the {@link ResultSet}.
	 */
	void onException(Exception x);
}
