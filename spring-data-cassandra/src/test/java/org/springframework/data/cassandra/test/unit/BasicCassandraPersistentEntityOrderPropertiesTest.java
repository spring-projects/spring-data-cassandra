/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.unit;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.mapping.PropertyHandler;

/**
 * @author David Webb
 */
public class BasicCassandraPersistentEntityOrderPropertiesTest {

	private List<CassandraPersistentProperty> expected;
	private BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();

	@Before
	public void init() {}

	@Test
	public void testCompositeKeyPropertyOrder() {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(CompositePK.class);

		expected = new LinkedList<CassandraPersistentProperty>();
		expected.add(entity.getPersistentProperty("key0"));
		expected.add(entity.getPersistentProperty("key1"));
		expected.add(entity.getPersistentProperty("key2"));

		final List<CassandraPersistentProperty> actual = new LinkedList<CassandraPersistentProperty>();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty persistentProperty) {
				actual.add(persistentProperty);
			}
		});

		assertEquals(expected, actual);

	}

	@Test
	public void testTablePropertyOrder() {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(CompositeKeyEntity.class);

		expected = new LinkedList<CassandraPersistentProperty>();
		expected.add(entity.getPersistentProperty("key"));
		expected.add(entity.getPersistentProperty("attribute"));
		expected.add(entity.getPersistentProperty("text"));

		final List<CassandraPersistentProperty> actual = new LinkedList<CassandraPersistentProperty>();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty persistentProperty) {
				actual.add(persistentProperty);
			}
		});

		assertEquals(expected, actual);

	}

	@Table
	static class CompositeKeyEntity {

		@PrimaryKey
		private CompositePK key;

		private String attribute;

		private String text;

	}

	/**
	 * This is intentionally using dumb ordinals
	 */
	@PrimaryKeyClass
	static class CompositePK implements Serializable {

		@PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.PARTITIONED)
		private String key0;

		@PrimaryKeyColumn(ordinal = 0)
		private String key1;

		@PrimaryKeyColumn(ordinal = 1)
		private String key2;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((key0 == null) ? 0 : key0.hashCode());
			result = prime * result + ((key1 == null) ? 0 : key1.hashCode());
			result = prime * result + ((key2 == null) ? 0 : key2.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CompositePK other = (CompositePK) obj;
			if (key0 == null) {
				if (other.key0 != null)
					return false;
			} else if (!key0.equals(other.key0))
				return false;
			if (key1 == null) {
				if (other.key1 != null)
					return false;
			} else if (!key1.equals(other.key1))
				return false;
			if (key2 == null) {
				if (other.key2 != null)
					return false;
			} else if (!key2.equals(other.key2))
				return false;
			return true;
		}

	}

	@Table
	static class SimpleKeyEntity {

		@Id
		private String id;

	}

}
