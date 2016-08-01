/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.mapping;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link BasicCassandraPersistentProperty}.
 *
 * @author Alex Shvid
 */
public class BasicCassandraPersistentPropertyUnitTests {

	static class Timeline {

		@PrimaryKey String id;

		Date time;

		@Column("message") String text;

	}

	CassandraPersistentEntity<Timeline> entity;

	@Before
	public void setup() {
		entity = new BasicCassandraPersistentEntity<Timeline>(ClassTypeInformation.from(Timeline.class));
	}

	@Test
	public void usesAnnotatedColumnName() {

		Field field = ReflectionUtils.findField(Timeline.class, "text");
		assertThat(getPropertyFor(field).getColumnName().toCql()).isEqualTo("message");
	}

	@Test
	public void checksIdProperty() {
		Field field = ReflectionUtils.findField(Timeline.class, "id");
		CassandraPersistentProperty property = getPropertyFor(field);
		assertThat(property.isIdProperty()).isTrue();
	}

	@Test
	public void returnsPropertyNameForUnannotatedProperty() {
		Field field = ReflectionUtils.findField(Timeline.class, "time");
		assertThat(getPropertyFor(field).getColumnName().toCql()).isEqualTo("time");
	}

	private CassandraPersistentProperty getPropertyFor(Field field) {
		return new BasicCassandraPersistentProperty(field, null, entity, new CassandraSimpleTypeHolder());
	}
}
