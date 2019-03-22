/*
 * Copyright 2016-2019 the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.QueryOptionsUtil;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.convert.EntityWriter;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.Iterators;

/**
 * Simple utility class for working with the QueryBuilder API using mapped entities.
 * <p>
 * Only intended for internal use.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class EntityQueryUtils {

	private static final Pattern FROM_REGEX = Pattern.compile(" FROM ([\"]?[\\w]*[\\\\.]?[\\w]*[\"]?)[\\s]?",
			Pattern.CASE_INSENSITIVE);

	/**
	 * Creates a Query Object for an insert.
	 *
	 * @param tableName the table name, must not be empty and not {@literal null}.
	 * @param objectToInsert the object to save, must not be {@literal null}.
	 * @param options optional {@link WriteOptions} to apply to the {@link Insert} statement, may be {@literal null}.
	 * @param entityWriter the {@link EntityWriter} to write insert values.
	 * @param entity must not be {@literal null}.
	 * @return The Query object to run with session.execute();
	 */
	static Insert createInsertQuery(String tableName, Object objectToInsert, WriteOptions options,
			CassandraConverter entityWriter, CassandraPersistentEntity<?> entity) {

		Assert.hasText(tableName, "TableName must not be empty");
		Assert.notNull(objectToInsert, "Object to insert must not be null");
		Assert.notNull(entityWriter, "CassandraConverter must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		Insert insert = addWriteOptions(QueryBuilder.insertInto(tableName), options);

		boolean insertNulls = false;
		if (options instanceof InsertOptions) {

			InsertOptions insertOptions = (InsertOptions) options;

			insertNulls = insertOptions.isInsertNulls();
		}

		if (insertNulls) {

			Map<String, Object> toInsert = new LinkedHashMap<>();

			entityWriter.write(objectToInsert, toInsert, entity);

			for (Entry<String, Object> entry : toInsert.entrySet()) {
				insert.value(entry.getKey(), entry.getValue());
			}
		} else {
			entityWriter.write(objectToInsert, insert);
		}

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

		Update update = addWriteOptions(QueryBuilder.update(tableName), options);

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

		if (options instanceof WriteOptions) {
			addWriteOptions(delete, (WriteOptions) options);
		} else {
			QueryOptionsUtil.addQueryOptions(delete, options);
		}

		Where where = delete.where();

		entityWriter.write(objectToDelete, where);

		return delete;
	}

	/**
	 * Read a {@link Slice} of data from the {@link ResultSet} for a {@link Pageable}.
	 *
	 * @param resultSet must not be {@literal null}.
	 * @param mapper must not be {@literal null}.
	 * @param page
	 * @param pageSize
	 * @return the resulting {@link Slice}.
	 */
	static <T> Slice<T> readSlice(ResultSet resultSet, RowMapper<T> mapper, int page, int pageSize) {

		int toRead = resultSet.getAvailableWithoutFetching();

		return readSlice(() -> Iterators.limit(resultSet.iterator(), toRead), resultSet.getExecutionInfo().getPagingState(),
				mapper, page, pageSize);
	}

	/**
	 * Read a {@link Slice} of data from the {@link Iterable} of {@link Row}s for a {@link Pageable}.
	 *
	 * @param rows must not be {@literal null}.
	 * @param pagingState
	 * @param mapper must not be {@literal null}.
	 * @param page
	 * @param pageSize
	 * @return the resulting {@link Slice}.
	 * @since 2.1
	 */
	static <T> Slice<T> readSlice(Iterable<Row> rows, @Nullable PagingState pagingState, RowMapper<T> mapper, int page,
			int pageSize) {

		List<T> result = new ArrayList<>(pageSize);

		Iterator<Row> iterator = rows.iterator();
		int index = 0;

		while (iterator.hasNext()) {
			T element = mapper.mapRow(iterator.next(), index++);
			result.add(element);
		}

		CassandraPageRequest pageRequest = CassandraPageRequest.of(PageRequest.of(page, pageSize), pagingState);

		return new SliceImpl<>(result, pageRequest, pagingState != null);
	}

	/**
	 * Extract the table name from a {@link Statement}.
	 *
	 * @param statement
	 * @return
	 * @since 2.1
	 */
	static CqlIdentifier getTableName(Statement statement) {

		if (statement instanceof Select) {

			Select select = (Select) statement;

			DirectFieldAccessor accessor = new DirectFieldAccessor(select);
			String table = (String) accessor.getPropertyValue("table");

			if (table != null) {
				return CqlIdentifier.of(table);
			}
		}

		String cql = statement.toString();
		Matcher matcher = FROM_REGEX.matcher(cql);

		if (matcher.find()) {

			String cqlTableName = matcher.group(1);
			if (cqlTableName.startsWith("\"")) {
				return CqlIdentifier.quoted(cqlTableName.substring(1, cqlTableName.length() - 1));
			}

			int separator = cqlTableName.indexOf('.');

			if (separator != -1) {
				return CqlIdentifier.of(cqlTableName.substring(separator + 1));
			}

			return CqlIdentifier.of(cqlTableName);
		}

		return CqlIdentifier.of("unknown");
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Insert} CQL statements.
	 *
	 * @param insert {@link Insert} CQL statement, must not be {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Insert}.
	 * @see #addWriteOptions(Insert, WriteOptions)
	 * @since 2.1
	 */
	static Insert addWriteOptions(Insert insert, WriteOptions writeOptions) {

		Assert.notNull(insert, "Insert must not be null");

		if (writeOptions instanceof InsertOptions) {

			InsertOptions insertOptions = (InsertOptions) writeOptions;

			if (insertOptions.isIfNotExists()) {
				insert = insert.ifNotExists();
			}
		}

		QueryOptionsUtil.addWriteOptions(insert, writeOptions);

		return insert;
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Update} CQL statements.
	 *
	 * @param update {@link Update} CQL statement, must not be {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Update}.
	 * @see QueryOptionsUtil#addWriteOptions(Update, WriteOptions)
	 * @since 2.1
	 */
	static Update addWriteOptions(Update update, WriteOptions writeOptions) {

		Assert.notNull(update, "Update must not be null");

		QueryOptionsUtil.addWriteOptions(update, writeOptions);

		if (writeOptions instanceof UpdateOptions) {

			UpdateOptions updateOptions = (UpdateOptions) writeOptions;

			if (updateOptions.isIfExists()) {
				update.where().ifExists();
			}
		}

		return update;
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Delete} CQL statements.
	 *
	 * @param delete {@link Delete} CQL statement, must not be {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Delete}.
	 * @since 2.1
	 */
	static Delete addWriteOptions(Delete delete, WriteOptions writeOptions) {

		Assert.notNull(delete, "Delete must not be null");

		QueryOptionsUtil.addWriteOptions(delete, writeOptions);

		if (writeOptions instanceof DeleteOptions) {

			DeleteOptions deleteOptions = (DeleteOptions) writeOptions;

			if (deleteOptions.isIfExists()) {
				delete.where().ifExists();
			}
		}

		return delete;
	}
}
