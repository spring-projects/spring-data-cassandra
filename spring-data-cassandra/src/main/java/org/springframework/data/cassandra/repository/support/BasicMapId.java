package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.util.Assert;

/**
 * Simple implementation of {@link MapId}.
 * <p/>
 * <em>Note:</em> This could be extended in various cool ways, like one that takes a type and validates that the given
 * name corresponds to an actual field or bean property on that type. There could also be another one that uses a
 * {@link CassandraPersistentEntity} and {@link CassandraPersistentProperty} instead of a String name.
 * 
 * @author Matthew T. Adams
 */
@SuppressWarnings("serial")
public class BasicMapId implements MapId {

	/**
	 * Factory method. Convenient if imported statically.
	 * 
	 * @return {@link BasicMapId}
	 */
	public static MapId id() {
		return new BasicMapId();
	}

	/**
	 * Factory method. Convenient if imported statically.
	 * 
	 * @return {@link BasicMapId}
	 */
	public static MapId id(String name, Serializable value) {
		return new BasicMapId().with(name, value);
	}

	/**
	 * Factory method. Convenient if imported statically.
	 * 
	 * @return {@link BasicMapId}
	 */
	public static MapId id(MapId id) {
		return new BasicMapId(id);
	}

	protected Map<String, Serializable> map = new HashMap<String, Serializable>();

	public BasicMapId() {}

	public BasicMapId(Map<String, Serializable> map) {

		Assert.notNull(map);
		map.putAll(map);
	}

	@Override
	public BasicMapId with(String name, Serializable value) {
		put(name, value);
		return this;
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public boolean containsKey(Object name) {
		return map.containsKey(name);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<String, Serializable>> entrySet() {
		return map.entrySet();
	}

	@Override
	public boolean equals(Object that) {
		if (this == that) {
			return true;
		}
		if (that == null) {
			return false;
		}
		if (!(that instanceof Map)) { // we can be equal to a Map
			return false;
		}
		return map.equals(that);
	}

	@Override
	public Serializable get(Object name) {
		return map.get(name);
	}

	@Override
	public int hashCode() {
		return map.hashCode();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public Set<String> keySet() {
		return map.keySet();
	}

	@Override
	public Serializable put(String name, Serializable value) {
		return map.put(name, value);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Serializable> source) {
		map.putAll(source);
	}

	@Override
	public Serializable remove(Object name) {
		return map.remove(name);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public Collection<Serializable> values() {
		return map.values();
	}

	@Override
	public String toString() {

		StringBuilder s = new StringBuilder("{ ");

		boolean first = true;
		for (Map.Entry<String, Serializable> entry : map.entrySet()) {

			if (first) {
				first = false;
			} else {
				s.append(", ");
			}

			s.append(entry.getKey()).append(" : ").append(entry.getValue());
		}

		return s.append(" }").toString();
	}
}
