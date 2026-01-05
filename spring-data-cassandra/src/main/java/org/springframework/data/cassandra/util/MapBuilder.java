/*
 * Copyright 2013-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * Builder for maps, which also conveniently implements {@link Map} via delegation for convenience so you don't have to
 * actually {@link #build()} it.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @param <K> The key type of the map.
 * @param <V> The value type of the map.
 */
public class MapBuilder<K, V> implements Map<K, V> {

	private final Map<K, V> map;

	/**
	 * Create a new {@link MapBuilder}.
	 */
	public MapBuilder() {
		this(Collections.emptyMap());
	}

	/**
	 * Create a new instance with a copy of the given map.
	 *
	 * @param source must not be {@literal null}
	 */
	public MapBuilder(Map<K, V> source) {

		Assert.notNull(source, "Source map must not be null");

		this.map = new LinkedHashMap<>(source);
	}

	/**
	 * Factory method to construct a new {@code MapBuilder}. Convenient if imported statically.
	 */
	public static MapBuilder<Object, Object> map() {
		return map(Object.class, Object.class);
	}

	/**
	 * Factory method to construct a new builder with the given key &amp; value types. Convenient if imported statically.
	 */
	public static <K, V> MapBuilder<K, V> map(Class<K> keyType, Class<V> valueType) {
		return new MapBuilder<>();
	}

	/**
	 * Factory method to construct a new builder with a shallow copy of the given map. Convenient if imported statically.
	 */
	public static <K, V> MapBuilder<K, V> map(Map<K, V> source) {
		return new MapBuilder<>(source);
	}

	/**
	 * Add an entry to this map, then returns {@code this}.
	 *
	 * @return this
	 */
	public MapBuilder<K, V> entry(K key, V value) {
		map.put(key, value);
		return this;
	}

	/**
	 * Return a new map based on the current state of this builder's map.
	 *
	 * @return a new Map with this builder's map's current content.
	 */
	public Map<K, V> build() {
		return new LinkedHashMap<>(map);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	@Nullable
	public V get(Object key) {
		return map.get(key);
	}

	@Override
	public V put(K key, V value) {
		return map.put(key, value);
	}

	@Override
	@Nullable
	public V remove(Object key) {
		return map.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		map.putAll(m);
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public Set<K> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<V> values() {
		return map.values();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}

	@Override
	public boolean equals(@Nullable Object o) {
		return map.equals(o);
	}

	@Override
	public int hashCode() {
		return map.hashCode();
	}
}
