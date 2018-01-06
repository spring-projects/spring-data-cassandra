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

import java.lang.reflect.Field;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link BasicCassandraPersistentProperty}.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CompoundPrimaryKeyUnitTests {

	@PrimaryKeyClass
	static class TimelineKey {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String string;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) Date datetime;
	}

	@Table
	static class Timeline {
		@PrimaryKey TimelineKey id;

		@Column("message") String text;
	}

	CassandraPersistentEntity<TimelineKey> cpk;
	CassandraPersistentEntity<Timeline> entity;

	@Before
	public void setup() {
		cpk = new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(TimelineKey.class));
		entity = new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(Timeline.class));
	}

	@Test // DATACASS-507
	public void checkIdProperty() {
		Field id = ReflectionUtils.findField(Timeline.class, "id");
		CassandraPersistentProperty property = getPropertyFor(Property.of(ClassTypeInformation.from(Timeline.class), id));
		assertThat(property.isIdProperty()).isTrue();
		assertThat(property.isCompositePrimaryKey()).isTrue();
	}

	private CassandraPersistentProperty getPropertyFor(Property property) {
		return new BasicCassandraPersistentProperty(property, entity, CassandraSimpleTypeHolder.HOLDER);
	}
}
