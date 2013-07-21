package org.springframework.data.cassandra.core;

import java.util.Collection;
import java.util.List;

import org.springframework.data.cassandra.core.bean.RingMember;

/**
 * Interface for specifying the operations that need to be implemented by {@link CassandraThriftTemplate}.
 * 
 * @author David Webb
 *
 */
public interface CassandraOperations {

	/**
	 * Describe the current Ring
	 * 
	 * @return The list of ring tokens that are active in the cluster
	 */
	List<RingMember> describeRing();
	
	/**
	 * Describe the Keyspace
	 * 
	 * @return	The Keyspace Name
	 * 
	 * TODO - Expand this to include all meta-data about the keyspace
	 */
	String describeKeyspace();
	
	/**
	 * Creates a new column family in the current keyspace with the default 
	 * UTF8Type validation, key and comparator classes
	 * 
	 * @param columnFamilyName
	 */
	void createColumnFamily(String columnFamilyName);
	
	/**
	 * Drop a column family with the supplied name
	 */
	void dropColumnFamily(String columnFamilyName);
	
	/**
	 * Returns a Row with the given id mapped onto the given class. 
	 * 
	 * @param <T>
	 * @param id the id of the row to return.
	 * @param entityClass the type the document shall be converted into.
	 * @return the document with the given id mapped onto the given target class.
	 */
	<T> T findById(Object id, Class<T> entityClass, String columnFamilyName);
	
	/**
	 * Query for a list of objects of type T from the current keyspace.
	 * <p/>
	 * 
	 * @param entityClass the parameterized type of the returned list
	 * @return the converted collection
	 */
	<T> List<T> findAll(Class<T> entityClass, String columnFamilyName);
	
	/**
	 * Query Cassandra with a CQL Expression
	 * 
	 * @param cql
	 * @param entityClass
	 * @param columnFamilyName
	 * @return
	 */
	<T> List<T> findByCQL(String cql, Class<T> entityClass, String columnFamilyName);
	
	
	/**
	 * Insert the object into the specified columnFamily.
	 * <p/>
	 * Insert is used to initially store the object into the database. To update an existing object use the save method.
	 * 
	 * @param objectToSave the object to store in the collection
	 * @param columnFamilyName name of the columnFamily to insert the object in
	 */
	<T> void insert(T objectToSave, Class<T> entityClass, String columnFamilyName);

	/**
	 * Insert a list of objects into the specified collection in a single batch write to the database.
	 * 
	 * @param batchToSave the list of objects to save.
	 * @param columnFamilyName name of the columnFamily to insert the objects in
	 */
	<T> void insert(Collection<T> batchToSave, Class<T> entityClass, String columnFamilyName);	
	
	/**
	 * Update the object into the specified columnFamily.
	 * 
	 * @param objectToSave the object to store in the collection
	 * @param columnFamilyName name of the columnFamily to save the object in
	 */
	<T> void save(T objectToSave, Class<T> entityClass, String columnFamilyName);

	/**
	 * Update a list of objects into the specified columnFamily in a single batch write to the database.
	 * 
	 * @param batchToSave the list of objects to save.
	 * @param columnFamilyName name of the columnFamily to save the objects in
	 * 
	 */
	<T> void save(Collection<T> batchToSave, Class<T> entityClass, String columnFamilyName);	
	
	/**
	 * Insert the object into the specified columnFamily.
	 * <p/>
	 * Insert is used to initially store the object into the database. To update an existing object use the save method.
	 * 
	 * @param objectToRemove the object to remove from the collection
	 * @param columnFamilyName name of the columnFamily to remove the object from
	 */
	<T> void remove(T objectToRemove, Class<T> entityClass, String columnFamilyName);

	/**
	 * Remove a list of objects from the specified columnFamily in a single batch write to the database.
	 * 
	 * @param batchToRemove the list of objects to save.
	 * @param columnFamilyName name of the columnFamily to remove the objects from
	 */
	<T> void remove(Collection<T> batchToRemove, Class<T> entityClass, String columnFamilyName);	
	
	/**
	 * delete a list of objects from the specified columnFamily in a single batch write to the database.
	 * 
	 * @param batchToRemove the list of objects to save.
	 * @param columnFamilyName name of the columnFamily to remove the objects from
	 */
	<T> void delete(List<String> rowKeys, Class<T> entityClass, String columnFamilyName);	
	
}
