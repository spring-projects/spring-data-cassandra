/*
 * Copyright 2017-2021 the original author or authors.
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

/**
 * @author Matthew T. Adams
 */
@Table
class MultiPrimaryKeyColumns {

	@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) private String key0;

	@PrimaryKeyColumn(ordinal = 1) private String key1;

	@Column private String value;

	public MultiPrimaryKeyColumns(String key0, String key1) {
		setKey0(key0);
		setKey1(key1);
	}

	public String getKey0() {
		return key0;
	}

	private void setKey0(String key0) {
		this.key0 = key0;
	}

	public String getKey1() {
		return key1;
	}

	private void setKey1(String key1) {
		this.key1 = key1;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
