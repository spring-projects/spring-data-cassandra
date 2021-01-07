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

package org.springframework.data.cassandra.repository.forcequote.config;

import java.util.UUID;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

/**
 * @author Matthew T. Adams
 */
@Table
public class ImplicitProperties {

	@PrimaryKey(forceQuote = true) private String primaryKey;

	@Column(forceQuote = true) private String stringValue = UUID.randomUUID().toString();

	public ImplicitProperties() {
		this(UUID.randomUUID().toString());
	}

	private ImplicitProperties(String primaryKey) {
		setPrimaryKey(primaryKey);
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	private void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getStringValue() {
		return stringValue;
	}

	public void setStringValue(String stringy) {
		this.stringValue = stringy;
	}
}
