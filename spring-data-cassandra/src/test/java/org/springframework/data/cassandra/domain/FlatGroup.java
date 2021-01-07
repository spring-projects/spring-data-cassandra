/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.domain;

import lombok.Data;

import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

/**
 * @author Mark Paluch
 * @see <a href=
 *      "https://www.datastax.com/dev/blog/basic-rules-of-cassandra-data-modeling">https://www.datastax.com/dev/blog/basic-rules-of-cassandra-data-modeling</a>
 */
@Table
@Data
public class FlatGroup {

	@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) private String groupname;
	@PrimaryKeyColumn(name = "hash_prefix", ordinal = 2, type = PrimaryKeyType.PARTITIONED) private String hashPrefix;
	@PrimaryKeyColumn(ordinal = 3, type = PrimaryKeyType.CLUSTERED) private String username;

	private String email;
	private int age;

	public FlatGroup(String groupname, String hashPrefix, String username) {
		this.groupname = groupname;
		this.hashPrefix = hashPrefix;
		this.username = username;
	}
}
