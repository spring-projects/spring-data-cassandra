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

import java.util.Map;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.SASI;
import org.springframework.data.cassandra.core.mapping.SASI.StandardAnalyzed;
import org.springframework.data.cassandra.core.mapping.Table;

// tag::class[]
@Table
class PersonWithIndexes {

  @Id
  private String key;

  @SASI
  @StandardAnalyzed
  private String names;

  @Indexed("indexed_map")
  private Map<String, String> entries;

  private Map<@Indexed String, String> keys;

  private Map<String, @Indexed String> values;

  // …
}
// end::class[]
