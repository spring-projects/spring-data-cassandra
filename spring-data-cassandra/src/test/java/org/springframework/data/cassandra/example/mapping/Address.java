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
package org.springframework.data.cassandra.example.mapping;

import java.util.List;
import java.util.Set;

import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

// tag::class[]
@UserDefinedType("address")
public class Address {

  @CassandraType(type = CassandraType.Name.VARCHAR)
  private String street;

  private String city;

  private Set<String> zipcodes;

  @CassandraType(type = CassandraType.Name.SET, typeArguments = CassandraType.Name.BIGINT)
  private List<Long> timestamps;

  // other getters/setters omitted
}
// end::class[]
