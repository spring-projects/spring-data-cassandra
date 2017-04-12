/*
 * Copyright 2013-2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.query;

import java.io.Serializable;

import org.springframework.data.repository.core.EntityInformation;

/**
 * Cassandra specific {@link EntityInformation}.
 *
 * @author Alex Shvid
 * @author Mark Paluch
 */
public interface CassandraEntityInformation<T, ID extends Serializable>
		extends EntityInformation<T, ID>, CassandraEntityMetadata<T> {

	/**
	 * Return {@literal true} if the persistent entity consists entirely of primary key properties (a single Id property,
	 * composite primary key).
	 *
	 * @return {@literal true} if the persistent entity consists entirely of primary key properties (a single Id property,
	 *         composite primary key).
	 * @since 1.5.2
	 */
	boolean isPrimaryKeyEntity();
}
