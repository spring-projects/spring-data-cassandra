/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.cassandra.repository.mapid;

import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.util.ObjectUtils;

/**
 * @author Matthew T. Adams
 */
@Table
public class SinglePrimaryKeyColumn {

	@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String key;

	@Column String value;

	public SinglePrimaryKeyColumn(String key) {
		setKey(key);
	}

	public String getKey() {
		return key;
	}

	private void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		SinglePrimaryKeyColumn that = (SinglePrimaryKeyColumn) o;

		if (!ObjectUtils.nullSafeEquals(key, that.key)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(value, that.value);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(key);
		result = 31 * result + ObjectUtils.nullSafeHashCode(value);
		return result;
	}
}
