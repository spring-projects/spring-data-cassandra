package org.springframework.data.cassandra.cql.builder;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Builder for maps, which also conveniently implements {@link Map} via delegation for convenience so you don't have to
 * actually {@link #build()} it if you don't want to (or forget).
 * 
 * @author Matthew T. Adams
 * @param <K> The key type of the map.
 * @param <V> The value type of the map.
 */
public class MapBuilder<K, V> implements Map<K, V> {

	/**
	 * Factory method to construct a new <code>MapBuilder&lt;Object,Object&gt;</code>. Convenient if imported statically.
	 */
	public static MapBuilder<Object, Object> map() {
		return map(Object.class, Object.class);
	}

	/**
	 * Factory method to construct a new builder with the given key &amp; value types. Convenient if imported statically.
	 */
	public static <K, V> MapBuilder<K, V> map(Class<K> keyType, Class<V> valueType) {
		return new MapBuilder<K, V>();
	}

	/**
	 * Factory method to construct a new builder with a shallow copy of the given map. Convenient if imported statically.
	 */
	public static <K, V> MapBuilder<K, V> map(Map<K, V> source) {
		return new MapBuilder<K, V>(source);
	}

	private Map<K, V> map;

	public MapBuilder() {
		this(new LinkedHashMap<K, V>());
	}

	/**
	 * Constructs a new instance with a copy of the given map.
	 */
	public MapBuilder(Map<K, V> source) {
		this.map = new LinkedHashMap<K, V>(source);
	}

	/**
	 * Adds an entry to this map, then returns <code>this</code>.
	 * 
	 * @return this
	 */
	public MapBuilder<K, V> entry(K key, V value) {
		map.put(key, value);
		return this;
	}

	/**
	 * Returns a new map based on the current state of this builder's map.
	 * 
	 * @return A new Map<K, V> with this builder's map's current content.
	 */
	public Map<K, V> build() {
		return new LinkedHashMap<K, V>(map);
	}

	public int size() {
		return map.size();
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	public V get(Object key) {
		return map.get(key);
	}

	public V put(K key, V value) {
		return map.put(key, value);
	}

	public V remove(Object key) {
		return map.remove(key);
	}

	public void putAll(Map<? extends K, ? extends V> m) {
		map.putAll(m);
	}

	public void clear() {
		map.clear();
	}

	public Set<K> keySet() {
		return map.keySet();
	}

	public Collection<V> values() {
		return map.values();
	}

	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}

	public boolean equals(Object o) {
		return map.equals(o);
	}

	public int hashCode() {
		return map.hashCode();
	}
}
