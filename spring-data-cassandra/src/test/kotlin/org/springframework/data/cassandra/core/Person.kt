/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.cassandra.core

import org.springframework.data.cassandra.core.mapping.*
import java.time.LocalDate
import java.util.*

@Table("person")
data class Person(
    @PrimaryKey("person_id")
    val personId: UUID,
    @field:Column("first_name")
    val firstName: String,
    @field:Column("last_name")
    val lastName: String,
    @field:Column("date_of_birth")
    val dateOfBirth: LocalDate,
    @field:Column("last_updated_at")
    val lastUpdatedAt: LocalDate
)


fun main() {


    val cctx = CassandraMappingContext()

    val pe = cctx.getRequiredPersistentEntity(Person::class.java)

    pe.doWithProperties { persistentProperty: CassandraPersistentProperty -> println(persistentProperty.columnName) }


}
