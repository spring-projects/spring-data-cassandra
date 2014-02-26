package org.springframework.data.cassandra.repository;

/**
 * Interface that entity classes may choose to implement in order to allow a client of the entity to easily get the
 * entity's {@link MapId}.
 * 
 * @author Matthew T. Adams
 */
public interface MapIdentifiable {

	/**
	 * Gets the identity of this instance. Throws {@link IllegalStateException} if this instance does not use
	 * {@link MapId}.
	 */
	MapId getMapId();
}
