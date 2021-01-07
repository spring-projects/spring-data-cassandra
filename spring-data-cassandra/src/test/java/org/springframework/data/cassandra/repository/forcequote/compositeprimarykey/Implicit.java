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
package org.springframework.data.cassandra.repository.forcequote.compositeprimarykey;

import java.util.UUID;

import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

/**
 * @author Matthew T. Adams
 */
@Table(forceQuote = true)
public class Implicit {

	@PrimaryKey private ImplicitKey primaryKey;

	private String stringValue = UUID.randomUUID().toString();

	@SuppressWarnings("unused")
	private Implicit() {}

	public Implicit(ImplicitKey primaryKey) {
		setPrimaryKey(primaryKey);
	}

	public ImplicitKey getPrimaryKey() {
		return primaryKey;
	}

	private void setPrimaryKey(ImplicitKey primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getStringValue() {
		return stringValue;
	}

	public void setStringValue(String stringValue) {
		this.stringValue = stringValue;
	}
}
