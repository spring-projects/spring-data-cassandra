/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.QueryOptionsUtil;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.convert.EntityWriter;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Simple utility class for working with the QueryBuilder API.
 * <p>
 * Only intended for internal use.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class QueryUtils {

	/**
	 * Creates a Query Object for an insert.
	 *
	 * @param tableName the table name, must not be empty and not {@literal null}.
	 * @param objectToUpdate the object to save, must not be {@literal null}.
	 * @param options optional {@link WriteOptions} to apply to the {@link Insert} statement, may be {@literal null}.
	 * @param entityWriter the {@link EntityWriter} to write insert values.
	 * @return The Query object to run with session.execute();
	 */
	static Insert createInsertQuery(String tableName, Object objectToUpdate, WriteOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Assert.hasText(tableName, "TableName must not be empty");
		Assert.notNull(objectToUpdate, "Object to insert must not be null");
		Assert.notNull(entityWriter, "EntityWriter must not be null");

		Insert insert = QueryOptionsUtil.addWriteOptions(QueryBuilder.insertInto(tableName), options);

		if (options instanceof InsertOptions) {

			InsertOptions insertOptions = (InsertOptions) options;

			if (insertOptions.isIfNotExists()) {
				insert = insert.ifNotExists();
			}
		}

		entityWriter.write(objectToUpdate, insert);

		return insert;
	}

	/**
	 * Creates a Query Object for an Update. The {@link Update} uses the identity and values from the given
	 * {@code objectsToUpdate}.
	 *
	 * @param tableName the table name, must not be empty and not {@literal null}.
	 * @param objectToUpdate the object to update, must not be {@literal null}.
	 * @param options optional {@link WriteOptions} to apply to the {@link Update} statement.
	 * @param entityWriter the {@link EntityWriter} to write update assignments and where clauses.
	 * @return The Query object to run with session.execute();
	 */
	static Update createUpdateQuery(String tableName, Object objectToUpdate, WriteOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Assert.hasText(tableName, "TableName must not be empty");
		Assert.notNull(objectToUpdate, "Object to update must not be null");
		Assert.notNull(entityWriter, "EntityWriter must not be null");

		Update update = QueryOptionsUtil.addWriteOptions(QueryBuilder.update(tableName), options);

		if (options instanceof UpdateOptions) {

			UpdateOptions updateOptions = (UpdateOptions) options;

			if (updateOptions.isIfExists()) {
				update.where().ifExists();
			}
		}

		entityWriter.write(objectToUpdate, update);

		return update;
	}

	/**
	 * Creates a Delete Query Object from an annotated POJO. The {@link Delete} uses the identity from the given
	 * {@code objectToDelete}.
	 *
	 * @param tableName the table name, must not be empty and not {@literal null}.
	 * @param objectToDelete the object to delete, must not be {@literal null}.
	 * @param options optional {@link QueryOptions} to apply to the {@link Delete} statement.
	 * @param entityWriter the {@link EntityWriter} to write delete where clauses.
	 * @return The Query object to run with session.execute();
	 */
	static Delete createDeleteQuery(String tableName, Object objectToDelete, QueryOptions options,
			EntityWriter<Object, Object> entityWriter) {

		Assert.hasText(tableName, "TableName must not be empty");
		Assert.notNull(objectToDelete, "Object to delete must not be null");
		Assert.notNull(entityWriter, "EntityWriter must not be null");

		Delete.Selection deleteSelection = QueryBuilder.delete();
		Delete delete = deleteSelection.from(tableName);
		Where where = QueryOptionsUtil.addQueryOptions(delete.where(), options);

		entityWriter.write(objectToDelete, where);

		return delete;
	}
}
