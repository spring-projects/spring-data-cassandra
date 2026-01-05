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

import java.io.Serializable;

import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.util.ObjectUtils;

/**
 * @author Mark Paluch
 * @see <a href=
 *      "https://www.datastax.com/dev/blog/basic-rules-of-cassandra-data-modeling">https://www.datastax.com/dev/blog/basic-rules-of-cassandra-data-modeling</a>
 */
@PrimaryKeyClass
public class GroupKey implements Serializable {

	@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) private String groupname;
	@PrimaryKeyColumn(name = "hash_prefix", ordinal = 2, type = PrimaryKeyType.PARTITIONED) private String hashPrefix;
	@PrimaryKeyColumn(ordinal = 3, type = PrimaryKeyType.CLUSTERED) private String username;

	public GroupKey(String groupname, String hashPrefix, String username) {
		this.groupname = groupname;
		this.hashPrefix = hashPrefix;
		this.username = username;
	}

	public GroupKey() {}

	public String getGroupname() {
		return this.groupname;
	}

	public String getHashPrefix() {
		return this.hashPrefix;
	}

	public String getUsername() {
		return this.username;
	}

	public void setGroupname(String groupname) {
		this.groupname = groupname;
	}

	public void setHashPrefix(String hashPrefix) {
		this.hashPrefix = hashPrefix;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		GroupKey groupKey = (GroupKey) o;

		if (!ObjectUtils.nullSafeEquals(groupname, groupKey.groupname)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(hashPrefix, groupKey.hashPrefix)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(username, groupKey.username);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(groupname);
		result = 31 * result + ObjectUtils.nullSafeHashCode(hashPrefix);
		result = 31 * result + ObjectUtils.nullSafeHashCode(username);
		return result;
	}
}
