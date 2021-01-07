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
package org.springframework.data.cassandra.support;

import java.util.UUID;

/**
 * Generates a random key space name starting with {@code ks}.
 *
 * @author Matthew T. Adams
 */
public abstract class RandomKeyspaceName {

	private RandomKeyspaceName() { }

	/**
	 * Creates a random {@link String keyspace name} starting with {@code ks} based on a random {@link UUID}.
	 *
	 * @return a random {@link String keyspace name}.
	 * @see java.lang.String
	 * @see java.util.UUID
	 */
	public static String create() {
		return "ks" + UUID.randomUUID().toString().replace("-", "");
	}
}
