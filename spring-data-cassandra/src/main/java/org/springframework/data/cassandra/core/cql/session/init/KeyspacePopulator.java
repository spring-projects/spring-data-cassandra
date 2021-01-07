/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session.init;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Strategy used to populate, initialize, or clean up a Cassandra keyspace.
 *
 * @author Mark Paluch
 * @since 3.0
 * @see ResourceKeyspacePopulator
 * @see SessionFactoryInitializer
 */
@FunctionalInterface
public interface KeyspacePopulator {

	/**
	 * Populate, initialize, or clean up the database using the provided CqlSession connection.
	 * <p>
	 * Concrete implementations <em>may</em> throw a {@link RuntimeException} if an error is encountered but are
	 * <em>strongly encouraged</em> to throw a specific {@link ScriptException} instead. For example, Spring's
	 * {@link ResourceKeyspacePopulator} wrap all exceptions in {@code ScriptExceptions}.
	 *
	 * @param session the CQL {@link CqlSession} to use to populate the keyspace; already configured and ready to use;
	 *          never {@literal null}
	 * @throws ScriptException in all other error cases
	 */
	void populate(CqlSession session) throws ScriptException;
}
