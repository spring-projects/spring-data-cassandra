/*
 * Copyright 2016-present the original author or authors.
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


import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.util.ObjectUtils;

/**
 * @author Mark Paluch
 * @see <a href=
 *      "https://www.datastax.com/dev/blog/basic-rules-of-cassandra-data-modeling">https://www.datastax.com/dev/blog/basic-rules-of-cassandra-data-modeling</a>
 */
@Table
public class Group {

	@PrimaryKey private GroupKey id;

	private String email;
	private int age;

	public Group(GroupKey id) {
		this.id = id;
	}

	public Group() {}

	public GroupKey getId() {
		return this.id;
	}

	public String getEmail() {
		return this.email;
	}

	public int getAge() {
		return this.age;
	}

	public void setId(GroupKey id) {
		this.id = id;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		Group group = (Group) o;

		if (age != group.age)
			return false;
		if (!ObjectUtils.nullSafeEquals(id, group.id)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(email, group.email);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(id);
		result = 31 * result + ObjectUtils.nullSafeHashCode(email);
		result = 31 * result + age;
		return result;
	}
}
