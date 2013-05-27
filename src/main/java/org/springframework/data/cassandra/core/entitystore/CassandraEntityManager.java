package org.springframework.data.cassandra.core.entitystore;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.data.cassandra.core.exception.MappingException;

import com.google.common.base.Function;

/**
 * @param <T> entity type 
 * @param <K> rowKey type
 */
public interface CassandraEntityManager<T, K> {

	/**
	 * write entity to cassandra with mapped rowId and columns
	 * @param entity entity object
	 */
	public void put(T entity) throws MappingException;
	
	/**
	 * fetch whole row and construct entity object mapping from columns
	 * @param id row key
	 * @return entity object. null if not exist
	 */
	public T get(K id) throws MappingException;
	
    /**
     * delete the whole row by id
     * @param id row key
     */
    public void delete(K id) throws MappingException;
    
    /**
     * remove an entire entity
     * @param id row key
     */
    public void remove(T entity) throws MappingException;
    
	/**
	 * @return Return all entities.  
	 * 
	 * @throws MappingException
	 */
	public List<T> getAll() throws MappingException;
	
	/**
	 * @return Get a set of entities
	 * @param ids
	 * @throws MappingException
	 */
	public List<T> get(Collection<K> ids) throws MappingException;
	
	/**
	 * Delete a set of entities by their id
	 * @param ids
	 * @throws MappingException
	 */
	public void delete(Collection<K> ids) throws MappingException;
	
    /**
     * Delete a set of entities 
     * @param ids
     * @throws MappingException
     */
	public void remove(Collection<T> entities) throws MappingException;
	
	/**
	 * Store a set of entities.
	 * @param entites
	 * @throws MappingException
	 */
	public void put(Collection<T> entities) throws MappingException;
	
	/**
	 * Visit all entities.
	 * 
	 * @param callback Callback when an entity is read.  Note that the callback 
	 *                 may be called from multiple threads.
	 * @throws MappingException
	 */
	public void visitAll(Function<T, Boolean> callback) throws MappingException;
	
	/**
	 * Execute a CQL query and return the found entites
	 * @param cql
	 * @throws MappingException
	 */
	public List<T> find(String cql) throws MappingException;
	
	/**
	 * Create the underlying storage for this entity.  This should only be called
	 * once when first creating store and not part of the normal startup sequence.
	 * @throws MappingException
	 */
    public void createStorage(Map<String, Object> options) throws MappingException;
    
    /**
     * Delete the underlying storage for this entity.  
     * @param options
     * @throws MappingException
     */
    public void deleteStorage() throws MappingException;
    
    /**
     * Truncate all data in the underlying
     * @param options
     * @throws MappingException
     */
    public void truncate() throws MappingException;
}
