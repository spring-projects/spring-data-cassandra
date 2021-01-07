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
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.core.mapping.CassandraPersistentPropertyComparator.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * The CassandraPersistentPropertyComparatorUnitTests class is a test suite of test cases testing the contract and
 * functionality of the {@link CassandraPersistentPropertyComparator} class.
 *
 * @author John Blum
 * @author Mark Paluch
 * @since 1.5.0
 */
@ExtendWith(MockitoExtension.class)
class CassandraPersistentPropertyComparatorUnitTests {

	@Mock CassandraPersistentProperty left;

	@Mock CassandraPersistentProperty right;

	@Test // DATACASS-248
	void leftAndRightAreNullReturnsZero() {

		assertThat(INSTANCE.compare(null, null)).isEqualTo(0);

		verifyZeroInteractions(left);
		verifyZeroInteractions(right);
	}

	@Test // DATACASS-248
	void leftIsNotNullAndRightIsNullReturnsOne() {

		assertThat(INSTANCE.compare(left, null)).isEqualTo(1);

		verifyZeroInteractions(left);
		verifyZeroInteractions(right);
	}

	@Test // DATACASS-248
	void leftIsNullAndRightIsNotNullReturnsMinusOne() {

		assertThat(INSTANCE.compare(null, right)).isEqualTo(-1);

		verifyZeroInteractions(left);
		verifyZeroInteractions(right);
	}

	@Test // DATACASS-248
	void leftAndRightAreEqualReturnsZero() {

		assertThat(INSTANCE.compare(left, left)).isEqualTo(0);
		assertThat(INSTANCE.compare(right, right)).isEqualTo(0);

		verifyZeroInteractions(left);
		verifyZeroInteractions(right);
	}

	@Test // DATACASS-248
	void leftAndRightAreCompositePrimaryKeysReturnsZero() {

		when(left.isCompositePrimaryKey()).thenReturn(true);
		when(right.isCompositePrimaryKey()).thenReturn(true);

		assertThat(INSTANCE.compare(left, right)).isEqualTo(0);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isCompositePrimaryKey();
	}

	@Test // DATACASS-248
	void leftIsCompositePrimaryKeyReturnsMinusOne() {

		when(left.isCompositePrimaryKey()).thenReturn(true);
		when(left.isPrimaryKeyColumn()).thenReturn(false);
		when(right.isCompositePrimaryKey()).thenReturn(false);
		when(right.isPrimaryKeyColumn()).thenReturn(false);

		assertThat(INSTANCE.compare(left, right)).isEqualTo(-1);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(left, times(1)).isPrimaryKeyColumn();
		verify(right, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isPrimaryKeyColumn();
	}

	@Test // DATACASS-248
	void leftIsPrimaryKeyColumnReturnsMinusOne() {

		when(left.isCompositePrimaryKey()).thenReturn(false);
		when(left.isPrimaryKeyColumn()).thenReturn(true);
		when(right.isCompositePrimaryKey()).thenReturn(false);
		when(right.isPrimaryKeyColumn()).thenReturn(false);

		assertThat(INSTANCE.compare(left, right)).isEqualTo(-1);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(left, times(1)).isPrimaryKeyColumn();
		verify(right, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isPrimaryKeyColumn();
	}

	@Test // DATACASS-248
	void rightIsCompositePrimaryKeyReturnsOne() {

		when(left.isCompositePrimaryKey()).thenReturn(false);
		when(left.isPrimaryKeyColumn()).thenReturn(false);
		when(right.isCompositePrimaryKey()).thenReturn(true);
		when(right.isPrimaryKeyColumn()).thenReturn(false);

		assertThat(INSTANCE.compare(left, right)).isEqualTo(1);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(left, times(1)).isPrimaryKeyColumn();
		verify(right, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isPrimaryKeyColumn();
	}

	@Test // DATACASS-248
	void rightIsPrimaryKeyColumnReturnsOne() {

		when(left.isCompositePrimaryKey()).thenReturn(false);
		when(left.isPrimaryKeyColumn()).thenReturn(false);
		when(right.isCompositePrimaryKey()).thenReturn(false);
		when(right.isPrimaryKeyColumn()).thenReturn(true);

		assertThat(INSTANCE.compare(left, right)).isEqualTo(1);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(left, times(1)).isPrimaryKeyColumn();
		verify(right, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isPrimaryKeyColumn();
	}

	@Test // DATACASS-248
	void compareLeftAndRightNamesReturnsNegativeValue() {

		when(left.isCompositePrimaryKey()).thenReturn(false);
		when(left.isPrimaryKeyColumn()).thenReturn(true);
		when(right.isCompositePrimaryKey()).thenReturn(true);
		when(right.isPrimaryKeyColumn()).thenReturn(false);
		when(left.getRequiredColumnName()).thenReturn(CqlIdentifier.fromCql("left"));
		when(right.getRequiredColumnName()).thenReturn(CqlIdentifier.fromCql("right"));

		assertThat(INSTANCE.compare(left, right)).isLessThan(0);

		verify(left, times(1)).isCompositePrimaryKey();
		verify(left, times(1)).isPrimaryKeyColumn();
		verify(left, times(1)).getRequiredColumnName();
		verify(right, times(1)).isCompositePrimaryKey();
		verify(right, times(1)).isPrimaryKeyColumn();
		verify(right, times(1)).getRequiredColumnName();
	}

	@Test // DATACASS-352
	void columnNameComparisonShouldHonorContract() throws Exception {

		CassandraMappingContext context = new CassandraMappingContext();
		CassandraPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(TwoColumns.class);

		CassandraPersistentProperty annotated = persistentEntity.getRequiredPersistentProperty("annotated");
		CassandraPersistentProperty another = persistentEntity.getRequiredPersistentProperty("anotherAnnotated");
		CassandraPersistentProperty plain = persistentEntity.getRequiredPersistentProperty("plain");

		assertThat(INSTANCE.compare(annotated, plain)).isLessThanOrEqualTo(-1);
		assertThat(INSTANCE.compare(plain, annotated)).isGreaterThanOrEqualTo(1);
		assertThat(INSTANCE.compare(another, another)).isEqualTo(0);
	}

	private static class TwoColumns {

		@Column("annotated") String annotated;

		@Column("another") String anotherAnnotated;

		String plain;
	}
}
