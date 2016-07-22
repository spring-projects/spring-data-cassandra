/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.domain;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Mark Paluch
 * @see http://www.datastax.com/dev/blog/basic-rules-of-cassandra-data-modeling
 */
@Table
@Data
@NoArgsConstructor
public class Group {

	@PrimaryKey private GroupKey id;

	private String email;
	private int age;

	public Group(GroupKey id) {
		this.id = id;
	}
}
