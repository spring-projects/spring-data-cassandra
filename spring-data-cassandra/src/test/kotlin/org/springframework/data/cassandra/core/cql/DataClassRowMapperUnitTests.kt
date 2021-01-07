/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.cql.ColumnDefinition
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions
import com.datastax.oss.driver.api.core.cql.Row
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito

/**
 * Unit tests for [DataClassRowMapper].
 *
 * @author Mark Paluch
 */
class DataClassRowMapperUnitTests {

	val row = mockk<Row>(relaxed = true)

	@Test // DATACASS-810
	fun createBeanFromRow() {

		val definitions = forColumns("firstname", "age")

		every { row.columnDefinitions } returns definitions
		every { row.get(0, String::class.java) } returns "Walter"
		every { row.get(1, Int::class.javaPrimitiveType) } returns 42

		val rowMapper = DataClassRowMapper<Person>()
		val person = rowMapper.mapRow(row, 0)

		assertThat(person.firstname).isEqualTo("Walter")
		assertThat(person.age).isEqualTo(42)
	}

	data class Person(val firstname: String, val age: Int)

	private fun forColumns(vararg columns: String): ColumnDefinitions {

		val definitions = Mockito.mock(ColumnDefinitions::class.java)
		var index = 0
		for (column in columns) {
			val columnDefinition = Mockito.mock(ColumnDefinition::class.java)
			val columnIndex = index++
			Mockito.`when`(columnDefinition.name).thenReturn(CqlIdentifier.fromInternal(column))
			Mockito.`when`(definitions[columnIndex]).thenReturn(columnDefinition)
			Mockito.`when`(definitions.firstIndexOf(column)).thenReturn(columnIndex)
		}
		Mockito.`when`(definitions.size()).thenReturn(index)

		return definitions
	}
}
