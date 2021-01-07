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
package org.springframework.data.cassandra.core.cql;

/**
 * Interface to be implemented by objects that can provide CQL strings.
 * <p>
 * Typically implemented by {@link PreparedStatementCreator}s and statement callbacks that want to expose the CQL they
 * use to create their statements, to allow for better contextual information in case of exceptions.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see PreparedStatementCreator
 * @see ReactivePreparedStatementCreator
 * @see ReactiveStatementCallback
 */
@FunctionalInterface
public interface CqlProvider {

	/**
	 * Return the CQL string for this object, i.e. typically the CQL used for creating statements.
	 *
	 * @return the CQL string.
	 */
	String getCql();
}
