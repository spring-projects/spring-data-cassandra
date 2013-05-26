package org.springframework.data.cassandra.core;

import java.util.List;
import java.util.Set;

import com.netflix.astyanax.connectionpool.TokenRange;

/**
 * Interface for specifying the operations that need to be implemented by {@link CassandraTemplate}.
 * 
 * @author David Webb
 *
 */
public interface CassandraOperations {

	/**
	 * Get a list of the keyspaces.
	 * 
	 * @return a list of keyspace names
	 */
	public Set<String> getKeyspaceNames();
	
	/**
	 * Describe the current Ring
	 */
	public List<TokenRange> describeRing();
	
	/**
	 * Describe the Keyspace
	 */
	public String describeKeyspace();
	
	/**
	 * Returns a Row with the given id mapped onto the given class. 
	 * 
	 * @param <T>
	 * @param id the id of the row to return.
	 * @param entityClass the type the document shall be converted into.
	 * @return the document with the given id mapped onto the given target class.
	 */
	<T> T findById(Object id, Class<T> entityClass, String columnFamilyName);
}
