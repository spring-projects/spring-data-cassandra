/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.cassandra.config;

/**
 * Enum identifying any schema actions to take at startup.
 *
 * @author Matthew T. Adams
 * @author John Blum
 */
public enum SchemaAction {

	/**
	 * Take no schema actions.
	 */
	NONE,

	/**
	 * Create each table as necessary. Fail if a table already exists.
	 */
	CREATE,

	/**
	 * Create each table as necessary. Avoid table creation if the table already exists.
	 */
	CREATE_IF_NOT_EXISTS,

	/**
	 * Create each table as necessary, dropping the table first if it exists.
	 */
	RECREATE,

	/**
	 * Drop <em>all</em> tables in the keyspace, then create each table as necessary.
	 */
	RECREATE_DROP_UNUSED
}
