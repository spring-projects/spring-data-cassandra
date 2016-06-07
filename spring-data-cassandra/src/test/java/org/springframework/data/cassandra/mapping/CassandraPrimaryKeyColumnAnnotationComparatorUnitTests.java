/*
 *  Copyright 2013-2016 the original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.data.cassandra.mapping;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;

/**
 * The CassandraPrimaryKeyColumnAnnotationComparatorUnitTests class is a test suite of test cases testing the contract
 * and functionality of the {@link CassandraPrimaryKeyColumnAnnotationComparator} class.
 *
 * @author John Blum
 * @see org.springframework.data.cassandra.mapping.CassandraPrimaryKeyColumnAnnotationComparator
 * @since 1.5.0
 */
public class CassandraPrimaryKeyColumnAnnotationComparatorUnitTests {

	private static PrimaryKeyColumn entityOne;
	private static PrimaryKeyColumn entityTwo;
	private static PrimaryKeyColumn entityThree;
	private static PrimaryKeyColumn entityFour;
	private static PrimaryKeyColumn entityFive;

	@BeforeClass
	public static void setup() throws Exception {
		entityOne = EntityOne.class.getDeclaredField("id").getAnnotation(PrimaryKeyColumn.class);
		entityTwo = EntityTwo.class.getDeclaredField("id").getAnnotation(PrimaryKeyColumn.class);
		entityThree = EntityThree.class.getDeclaredField("id").getAnnotation(PrimaryKeyColumn.class);
		entityFour = EntityFour.class.getDeclaredField("id").getAnnotation(PrimaryKeyColumn.class);
		entityFive = EntityFive.class.getDeclaredField("id").getAnnotation(PrimaryKeyColumn.class);
	}

	@Test
	public void compareTypes() {
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityOne, entityTwo), is(equalTo(1)));
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityTwo, entityTwo), is(equalTo(0)));
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityTwo, entityOne), is(equalTo(-1)));
	}

	@Test
	public void compareOrdinals() {
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityOne, entityThree), is(equalTo(-1)));
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityThree, entityThree), is(equalTo(0)));
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityThree, entityOne), is(equalTo(1)));
	}

	@Test
	public void compareName() {
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityOne, entityFour), is(equalTo(-1)));
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityFour, entityFour), is(equalTo(0)));
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityFour, entityOne), is(equalTo(1)));
	}

	@Test
	public void compareOrdering() {
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityOne, entityFive), is(equalTo(-1)));
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityFive, entityFive), is(equalTo(0)));
		assertThat(CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(entityFive, entityOne), is(equalTo(1)));
	}

	static class EntityOne {
		@PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1, name = "A", ordering = Ordering.ASCENDING)
		Integer id;
	}

	static class EntityTwo {
		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1, name = "A", ordering = Ordering.ASCENDING)
		Long id;
	}

	static class EntityThree {
		@PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 2, name = "A", ordering = Ordering.ASCENDING)
		String id;
	}

	static class EntityFour {
		@PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1, name = "B", ordering = Ordering.ASCENDING)
		Timestamp id;
	}

	static class EntityFive {
		@PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1, name = "A", ordering = Ordering.DESCENDING)
		UUID id;
	}
}
