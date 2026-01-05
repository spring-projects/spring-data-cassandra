/*
 * Copyright 2018-present the original author or authors.
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
package org.springframework.data.cassandra.core.mapping.event;

import java.io.Serial;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * {@link CassandraMappingEvent Mapping event} triggered before inserting or updating a row in the database. Before save
 * is invoked after {@link BeforeConvertCallback converting the entity} into a {@link Statement}. This is useful to let
 * the mapping layer derive values into the statement while the save callback can either update the domain object
 * without reflecting the changes in the statement. Another use is to inspect the {@link Statement}.
 *
 * @author Lukasz Antoniak
 * @author Mark Paluch
 * @since 2.1
 */
public class BeforeSaveEvent<E> extends AbstractStatementAwareMappingEvent<E> {

	@Serial private static final long serialVersionUID = 1L;

	/**
	 * Create a new {@link BeforeSaveEvent}.
	 *
	 * @param source must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param statement must not be {@literal null}.
	 */
	public BeforeSaveEvent(E source, CqlIdentifier table, Statement<?> statement) {
		super(source, statement, table);
	}
}
