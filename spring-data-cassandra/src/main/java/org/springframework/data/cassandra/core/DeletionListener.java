package org.springframework.data.cassandra.core;

import java.util.Collection;

import com.datastax.driver.core.ResultSet;

public interface DeletionListener<T> {

	void onDeletionComplete(Collection<T> entities);

	/**
	 * Called if an exception is raised while getting or converting the {@link ResultSet}.
	 */
	void onException(Exception x);
}
