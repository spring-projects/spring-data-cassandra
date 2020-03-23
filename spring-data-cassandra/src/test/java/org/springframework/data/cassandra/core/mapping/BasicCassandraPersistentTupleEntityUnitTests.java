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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.annotation.Transient;
import org.springframework.data.mapping.MappingException;

/**
 * Unit tests for {@link BasicCassandraPersistentTupleEntity}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicCassandraPersistentTupleEntityUnitTests {

	CassandraMappingContext mappingContext = new CassandraMappingContext();

	@Test // DATACASS-523
	public void shouldCreatePersistentTupleEntity() {

		BasicCassandraPersistentEntity<?> entity = this.mappingContext.getRequiredPersistentEntity(Address.class);

		assertThat(entity).isInstanceOf(BasicCassandraPersistentTupleEntity.class);

		entity.verify();
	}

	@Test // DATACASS-523
	public void shouldCreateElementsInOrder() {

		List<String> propertyNames = new ArrayList<>();

		BasicCassandraPersistentEntity<?> entity = this.mappingContext.getRequiredPersistentEntity(Address.class);

		entity.verify();

		entity.forEach(it -> propertyNames.add(it.getName()));

		assertThat(propertyNames).containsSequence("street", "city", "sortOrder");
	}

	@Test // DATACASS-523
	public void shouldReportDuplicateMappings() {

		assertThatThrownBy(() -> this.mappingContext.getRequiredPersistentEntity(DuplicateElement.class))
				.isInstanceOf(MappingException.class).hasMessageContaining("Duplicate ordinal [0]");
	}

	@Test // DATACASS-523
	public void shouldReportMissingOrdinalMappings() {

		assertThatThrownBy(() -> this.mappingContext.getRequiredPersistentEntity(MissingElementOrdinals.class))
				.isInstanceOf(MappingException.class).hasMessageContaining("Mapped tuple has no")
				.hasMessageContaining("for ordinal(s): 0");
	}

	@Test // DATACASS-523
	public void shouldReportNegativeOrdinalIndex() {

		assertThatThrownBy(() -> this.mappingContext.getRequiredPersistentEntity(NegativeIndex.class))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Element ordinal must be greater or equal to zero for property [street] in entity");
	}

	@Test // DATACASS-523
	public void shouldReportNoElements() {

		assertThatThrownBy(() -> this.mappingContext.getRequiredPersistentEntity(NoElements.class))
				.isInstanceOf(MappingException.class)
				.hasMessageContaining("Mapped tuple contains no persistent elements annotated");
	}

	@Test // DATACASS-523
	public void shouldReportMissingAnnotations() {

		assertThatThrownBy(() -> this.mappingContext.getRequiredPersistentEntity(MissingAnnotation.class))
				.isInstanceOf(MappingException.class)
				.hasMessageContaining("Missing @Element annotation in mapped tuple type for property [street]");
	}

	@Tuple
	static class Address {

		@Element(1) String city;
		@Element(0) String street;
		@Element(2) int sortOrder;
	}

	@Tuple
	static class DuplicateElement {

		@Element(0) String street;
		@Element(0) String city;
	}

	@Tuple
	static class NegativeIndex {
		@Element(-1) String street;
	}

	@Tuple
	static class MissingElementOrdinals {

		@Element(1) String street;
		@Element(3) String city;
	}

	@Tuple
	static class NoElements {
		@Transient String springDataTransient;
	}

	@Tuple
	static class MissingAnnotation {

		String street;
		String city;
	}
}
