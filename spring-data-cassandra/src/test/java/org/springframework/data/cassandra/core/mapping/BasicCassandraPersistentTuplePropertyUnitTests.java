/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.mapping.model.Property;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link BasicCassandraPersistentTupleEntity}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicCassandraPersistentTuplePropertyUnitTests {

	@Test // DATACASS-523
	public void mappedTupleShouldNotReportColumnName() {

		CassandraPersistentProperty property = getPropertyFor(MappedTuple.class, "date");

		assertThat(property.getColumnName()).isNull();
	}

	@Test // DATACASS-523
	public void mappedTupleShouldReportOrdinal() {

		CassandraPersistentProperty property = getPropertyFor(MappedTuple.class, "time");

		assertThat(property.getOrdinal()).isEqualTo(1);
	}

	private CassandraPersistentProperty getPropertyFor(Class<?> type, String fieldName) {

		Field field = ReflectionUtils.findField(type, fieldName);

		return new BasicCassandraPersistentTupleProperty(Property.of(ClassTypeInformation.from(type), field),
				getEntity(type), CassandraSimpleTypeHolder.HOLDER);
	}

	private <T> BasicCassandraPersistentEntity<T> getEntity(Class<T> type) {
		return new BasicCassandraPersistentTupleEntity<>(ClassTypeInformation.from(type));
	}

	@Tuple
	static class MappedTuple {
		@Element(0) Date date;
		@Element(1) Date time;
	}
}
