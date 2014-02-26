package org.springframework.data.cassandra.repository;

import java.io.Serializable;
import java.util.Map;

/**
 * Interface that represents the id of a persistent entity, where the keys correspond to the entity's JavaBean
 * properties.
 * 
 * @author Matthew T. Adams
 */
public interface MapId extends Serializable, Map<String, Serializable> {

	/**
	 * Builder method that adds the value for the named property, then returns <code>this</code>.
	 * 
	 * @param name The property name containing the value.
	 * @param value The property value.
	 * @return <code>this</code>
	 */
	MapId with(String name, Serializable value);
}
