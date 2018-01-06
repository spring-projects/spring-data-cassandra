/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;

import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.cql.keyspace.ColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.DataType;

/**
 * Unit tests for {@link BasicCassandraPersistentProperty} with a composite primary key class.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraCompositePrimaryKeyUnitTests {

	CassandraMappingContext context;

	CassandraPersistentEntity<?> entity;

	CassandraPersistentEntity<?> key;

	@Before
	public void setup() {

		context = new CassandraMappingContext();
		entity = context.getRequiredPersistentEntity(ClassTypeInformation.from(TypeWithCompositeKey.class));
		key = context.getRequiredPersistentEntity(ClassTypeInformation.from(CompositeKey.class));
	}

	@Test // DATACASS-507
	public void validateMappingInfo() {

		Field field = ReflectionUtils.findField(TypeWithCompositeKey.class, "id");
		CassandraPersistentProperty property = new BasicCassandraPersistentProperty(
				Property.of(ClassTypeInformation.from(TypeWithCompositeKey.class), field), entity,
				CassandraSimpleTypeHolder.HOLDER);
		assertThat(property.isIdProperty()).isTrue();
		assertThat(property.isCompositePrimaryKey()).isTrue();

		CreateTableSpecification spec = context.getCreateTableSpecificationFor(entity);

		List<ColumnSpecification> partitionKeyColumns = spec.getPartitionKeyColumns();
		assertThat(partitionKeyColumns).hasSize(1);
		ColumnSpecification partitionKeyColumn = partitionKeyColumns.get(0);
		assertThat(partitionKeyColumn.getName().toCql()).isEqualTo("z");
		assertThat(partitionKeyColumn.getKeyType()).isEqualTo(PrimaryKeyType.PARTITIONED);
		assertThat(partitionKeyColumn.getType()).isEqualTo(DataType.text());

		List<ColumnSpecification> clusteredKeyColumns = spec.getClusteredKeyColumns();
		assertThat(clusteredKeyColumns).hasSize(1);
		ColumnSpecification clusteredKeyColumn = clusteredKeyColumns.get(0);
		assertThat(clusteredKeyColumn.getName().toCql()).isEqualTo("a");
		assertThat(clusteredKeyColumn.getKeyType()).isEqualTo(PrimaryKeyType.CLUSTERED);
		assertThat(partitionKeyColumn.getType()).isEqualTo(DataType.text());
	}

	@PrimaryKeyClass
	@EqualsAndHashCode
	static class CompositeKey implements Serializable {

		private static final long serialVersionUID = 1L;

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String z;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED) String a;
	}

	@Table
	static class TypeWithCompositeKey {

		@PrimaryKey CompositeKey id;

		Date time;

		@Column("message") String text;
	}
}
