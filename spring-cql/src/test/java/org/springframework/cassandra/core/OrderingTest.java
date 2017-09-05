/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cassandra.core;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.junit.Test;

/**
 * Unit tests for class {@link Ordering org.springframework.cassandra.core.Ordering}.
 *
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class OrderingTest {


	@Test
	public void testCompareWithNonNullReturningPositive() throws Exception {

		Ordering primaryKeyType = Ordering.ASCENDING;
		int comparationResult = primaryKeyType.compare(primaryKeyType, Ordering.DESCENDING);

		assertThat(comparationResult).isEqualTo(1);
	}


	@Test
	public void testCompareWithNullReturningPositive() throws Exception {

		Ordering primaryKeyType = Ordering.DESCENDING;
		int comparationResult = primaryKeyType.compare(null, primaryKeyType);

		assertThat(comparationResult).isEqualTo(1);
	}


	@Test
	public void testValueOfWithClustered() throws Exception {

		Ordering primaryKeyType = Ordering.valueOf("ASCENDING");

		AssertionsForInterfaceTypes.assertThat(primaryKeyType).isEqualTo(Ordering.ASCENDING);
	}


	@Test
	public void testValueOfWithPartitioned() throws Exception {

		Ordering primaryKeyType = Ordering.valueOf("DESCENDING");

		AssertionsForInterfaceTypes.assertThat(primaryKeyType).isEqualTo(Ordering.DESCENDING);
	}


	@Test
	public void testCompareReturningNegative() throws Exception {

		Ordering primaryKeyType = Ordering.ASCENDING;
		int comparationResult = primaryKeyType.compare(Ordering.DESCENDING, primaryKeyType);

		assertThat(comparationResult).isEqualTo(-1);
	}


	@Test
	public void testCompareReturningNegativeWithNullParameter() throws Exception {

		Ordering ordering = Ordering.DESCENDING;
		int comparationResult = ordering.compare(ordering, null);

		assertThat(comparationResult).isEqualTo(-1);
	}


	@Test
	public void testCompareReturningZero() throws Exception {

		Ordering ordering = Ordering.DESCENDING;
		int comparationResult = ordering.compare(ordering, ordering);

		assertThat(comparationResult).isEqualTo(0);
	}
}