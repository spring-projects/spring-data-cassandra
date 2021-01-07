/*
 *  Copyright 2013-2021 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.mapping.CassandraPrimaryKeyColumnAnnotationComparator.*;

import java.sql.Timestamp;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;

/**
 * The CassandraPrimaryKeyColumnAnnotationComparatorUnitTests class is a test suite of test cases testing the contract
 * and functionality of the {@link CassandraPrimaryKeyColumnAnnotationComparator} class.
 *
 * @author John Blum
 * @author Mark Paluch
 */
class CassandraPrimaryKeyColumnAnnotationComparatorUnitTests {

	private static PrimaryKeyColumn entityOne;
	private static PrimaryKeyColumn entityTwo;
	private static PrimaryKeyColumn entityThree;
	private static PrimaryKeyColumn entityFour;
	private static PrimaryKeyColumn entityFive;

	@BeforeAll
	static void setup() throws Exception {

		entityOne = EntityOne.class.getDeclaredField("id").getAnnotation(PrimaryKeyColumn.class);
		entityTwo = EntityTwo.class.getDeclaredField("id").getAnnotation(PrimaryKeyColumn.class);
		entityThree = EntityThree.class.getDeclaredField("id").getAnnotation(PrimaryKeyColumn.class);
		entityFour = EntityFour.class.getDeclaredField("id").getAnnotation(PrimaryKeyColumn.class);
		entityFive = EntityFive.class.getDeclaredField("id").getAnnotation(PrimaryKeyColumn.class);
	}

	@Test // DATACASS-248
	void compareTypes() {

		assertThat(INSTANCE.compare(entityOne, entityTwo)).isEqualTo(1);
		assertThat(INSTANCE.compare(entityTwo, entityTwo)).isEqualTo(0);
		assertThat(INSTANCE.compare(entityTwo, entityOne)).isEqualTo(-1);
	}

	@Test // DATACASS-248
	void compareOrdinals() {

		assertThat(INSTANCE.compare(entityOne, entityThree)).isEqualTo(-1);
		assertThat(INSTANCE.compare(entityThree, entityThree)).isEqualTo(0);
		assertThat(INSTANCE.compare(entityThree, entityOne)).isEqualTo(1);
	}

	@Test // DATACASS-248
	void compareName() {

		assertThat(INSTANCE.compare(entityOne, entityFour)).isEqualTo(-1);
		assertThat(INSTANCE.compare(entityFour, entityFour)).isEqualTo(0);
		assertThat(INSTANCE.compare(entityFour, entityOne)).isEqualTo(1);
	}

	@Test // DATACASS-248
	void compareOrdering() {

		assertThat(INSTANCE.compare(entityOne, entityFive)).isEqualTo(-1);
		assertThat(INSTANCE.compare(entityFive, entityFive)).isEqualTo(0);
		assertThat(INSTANCE.compare(entityFive, entityOne)).isEqualTo(1);
	}

	private static class EntityOne {
		@PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1, name = "A",
				ordering = Ordering.ASCENDING) private Integer id;
	}

	private static class EntityTwo {
		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1, name = "A",
				ordering = Ordering.ASCENDING) private Long id;
	}

	private static class EntityThree {
		@PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 2, name = "A",
				ordering = Ordering.ASCENDING) private String id;
	}

	private static class EntityFour {
		@PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1, name = "B",
				ordering = Ordering.ASCENDING) private Timestamp id;
	}

	private static class EntityFive {
		@PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1, name = "A",
				ordering = Ordering.DESCENDING) private UUID id;
	}
}
