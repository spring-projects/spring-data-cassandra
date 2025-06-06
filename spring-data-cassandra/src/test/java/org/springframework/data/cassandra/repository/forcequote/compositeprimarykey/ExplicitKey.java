/*
 * Copyright 2016-2025 the original author or authors.
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

import java.io.Serializable;

import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

/**
 * @author Matthew T. Adams
 */
@PrimaryKeyClass
public class ExplicitKey implements Serializable {

	public static final String EXPLICIT_KEY_ZERO = "JavaExplicitKeyZero";
	public static final String EXPLICIT_KEY_ONE = "JavaExplicitKeyOne";

	private static final long serialVersionUID = 4459456944472099332L;

	@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED, forceQuote = true,
			name = EXPLICIT_KEY_ZERO) private String keyZero;

	@PrimaryKeyColumn(ordinal = 1, forceQuote = true, name = EXPLICIT_KEY_ONE) private String keyOne;

	public ExplicitKey(String keyZero, String keyOne) {
		this.keyZero = keyZero;
		this.keyOne = keyOne;
	}

	public String getKeyZero() {
		return keyZero;
	}

	public String getKeyOne() {
		return keyOne;
	}
}
