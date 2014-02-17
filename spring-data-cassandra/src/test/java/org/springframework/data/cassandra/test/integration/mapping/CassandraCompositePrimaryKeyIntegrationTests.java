/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.core.keyspace.ColumnSpecification;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.mapping.BasicCassandraPersistentProperty;
import org.springframework.data.cassandra.mapping.CachingCassandraPersistentProperty;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.mapping.CassandraSimpleTypeHolder;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.DataType;

/**
 * Integration test for {@link BasicCassandraPersistentProperty} with a composite primary key class.
 * 
 * @author Matthew T. Adams
 */
public class CassandraCompositePrimaryKeyIntegrationTests {

	private static final CassandraSimpleTypeHolder SIMPLE_TYPE_HOLDER = new CassandraSimpleTypeHolder();

	@PrimaryKeyClass
	static class Key {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
		String z;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED)
		String a;
	}

	@Table
	static class Thing {

		@PrimaryKey
		Key id;

		Date time;

		@Column("message")
		String text;
	}

	CassandraMappingContext context;
	CassandraPersistentEntity<?> thing;
	CassandraPersistentEntity<?> key;

	@Before
	public void setup() {
		context = new DefaultCassandraMappingContext();
		thing = context.getPersistentEntity(ClassTypeInformation.from(Thing.class));
		key = context.getPersistentEntity(ClassTypeInformation.from(Key.class));
	}

	@Test
	public void validateMappingInfo() {

		Field field = ReflectionUtils.findField(Thing.class, "id");
		CassandraPersistentProperty property = new CachingCassandraPersistentProperty(field, null, thing,
				SIMPLE_TYPE_HOLDER);
		assertTrue(property.isIdProperty());
		assertTrue(property.isCompositePrimaryKey());

		List<String> expectedColumnNames = Arrays.asList(new String[] { "z", "a" });
		assertTrue(expectedColumnNames.equals(property.getColumnNames()));

		List<String> actualColumnNames = new ArrayList<String>();
		List<CassandraPersistentProperty> properties = property.getCompositePrimaryKeyProperties();
		for (CassandraPersistentProperty p : properties) {
			actualColumnNames.addAll(p.getColumnNames());
		}
		assertTrue(expectedColumnNames.equals(actualColumnNames));

		CreateTableSpecification spec = context.getCreateTableSpecificationFor(thing);

		List<ColumnSpecification> partitionKeyColumns = spec.getPartitionKeyColumns();
		assertEquals(1, partitionKeyColumns.size());
		ColumnSpecification partitionKeyColumn = partitionKeyColumns.get(0);
		assertEquals("z", partitionKeyColumn.getName().toCql());
		assertEquals(PrimaryKeyType.PARTITIONED, partitionKeyColumn.getKeyType());
		assertEquals(DataType.text(), partitionKeyColumn.getType());

		List<ColumnSpecification> clusteredKeyColumns = spec.getClusteredKeyColumns();
		assertEquals(1, clusteredKeyColumns.size());
		ColumnSpecification clusteredKeyColumn = clusteredKeyColumns.get(0);
		assertEquals("a", clusteredKeyColumn.getName().toCql());
		assertEquals(PrimaryKeyType.CLUSTERED, clusteredKeyColumn.getKeyType());
		assertEquals(DataType.text(), partitionKeyColumn.getType());
	}
}
