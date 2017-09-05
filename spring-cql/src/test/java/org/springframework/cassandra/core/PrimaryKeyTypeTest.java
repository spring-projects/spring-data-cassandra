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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.junit.Test;

/**
 * Unit tests for class {@link PrimaryKeyType org.springframework.cassandra.core.PrimaryKeyType}.
 *
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class PrimaryKeyTypeTest {


	@Test
	public void testCompareWithNonNullReturningPositive() throws Exception {

		PrimaryKeyType primaryKeyTypeOne = PrimaryKeyType.PARTITIONED;
		int comparationResult = primaryKeyTypeOne.compare(primaryKeyTypeOne, PrimaryKeyType.CLUSTERED);

		assertThat(comparationResult).isEqualTo(1);
	}


	@Test
	public void testCompareWithNullReturningPositive() throws Exception {

		PrimaryKeyType primaryKeyTypeOne = PrimaryKeyType.PARTITIONED;
		int comparationResult = primaryKeyTypeOne.compare(null, primaryKeyTypeOne);

		assertThat(comparationResult).isEqualTo(1);
	}


	@Test
	public void testValueOfWithClustered() throws Exception {

		PrimaryKeyType primaryKeyTypeOne = PrimaryKeyType.valueOf("CLUSTERED");

		assertThat(primaryKeyTypeOne).isEqualTo(PrimaryKeyType.CLUSTERED);
	}


	@Test
	public void testValueOfWithPartitioned() throws Exception {

		PrimaryKeyType primaryKeyTypeOne = PrimaryKeyType.valueOf("PARTITIONED");

		assertThat(primaryKeyTypeOne).isEqualTo(PrimaryKeyType.PARTITIONED);
	}


	@Test
	public void testCompareReturningNegative() throws Exception {

		PrimaryKeyType primaryKeyTypeOne = PrimaryKeyType.PARTITIONED;
		int comparationResult = primaryKeyTypeOne.compare(PrimaryKeyType.CLUSTERED, primaryKeyTypeOne);

		assertThat(comparationResult).isEqualTo(-1);
	}


	@Test
	public void testCompareReturningNegativeWithNullParameter() throws Exception {

		PrimaryKeyType primaryKeyTypeOne = PrimaryKeyType.PARTITIONED;
		int comparationResult = primaryKeyTypeOne.compare(primaryKeyTypeOne, null);

		assertThat(comparationResult).isEqualTo(-1);
	}


	@Test
	public void testCompareReturningZero() throws Exception {

		PrimaryKeyType primaryKeyTypeOne = PrimaryKeyType.PARTITIONED;
		int comparationResult = primaryKeyTypeOne.compare(PrimaryKeyType.PARTITIONED, PrimaryKeyType.PARTITIONED);

		assertThat(comparationResult).isEqualTo(0);
	}
}