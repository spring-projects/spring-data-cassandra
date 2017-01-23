package org.springframework.data.cassandra.core;

import java.util.Collection;

import com.datastax.driver.core.ResultSet;

/**
 * Listener for asynchronous repository insert or update methods.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @deprecated as of 2.0, not used anymore.
 */
@Deprecated
public interface DeletionListener<T> {

	void onDeletionComplete(Collection<T> entities);

	/**
	 * Called if an exception is raised while getting or converting the {@link ResultSet}.
	 */
	void onException(Exception x);
}
