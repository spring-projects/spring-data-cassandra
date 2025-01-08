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
package org.springframework.data.cassandra.repository.query;

import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.CassandraScrollPosition;
import org.springframework.data.convert.DtoInstantiatingConverter;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Query executions for Cassandra.
 *
 * @author Mark Paluch
 * @since 1.5
 */
@FunctionalInterface
interface CassandraQueryExecution {

	@Nullable
	Object execute(Statement<?> statement, Class<?> type);

	/**
	 * {@link CassandraQueryExecution} for a Stream.
	 *
	 * @author Mark Paluch
	 */
	final class StreamExecution implements CassandraQueryExecution {

		private final CassandraOperations operations;
		private final Converter<Object, Object> resultProcessing;

		StreamExecution(CassandraOperations operations, Converter<Object, Object> resultProcessing) {
			this.operations = operations;
			this.resultProcessing = resultProcessing;
		}

		@Override
		public Object execute(Statement<?> statement, Class<?> type) {
			return operations.stream(statement, type).map(resultProcessing::convert);
		}
	}

	/**
	 * {@link CassandraQueryExecution} for a {@link Slice}.
	 *
	 * @author Mark Paluch
	 */
	final class SlicedExecution implements CassandraQueryExecution {

		private final CassandraOperations operations;

		SlicedExecution(CassandraOperations operations, Pageable pageable) {
			this.operations = operations;
			this.pageable = pageable;
		}

		private final Pageable pageable;

		@Override
		public Object execute(Statement<?> statement, Class<?> type) {

			CassandraPageRequest.validatePageable(pageable);

			Statement<?> statementToUse = statement.setPageSize(pageable.getPageSize());

			if (pageable instanceof CassandraPageRequest) {
				statementToUse = statementToUse.setPagingState(((CassandraPageRequest) pageable).getPagingState());
			}

			Slice<?> slice = operations.slice(statementToUse, type);

			if (pageable.getSort().isUnsorted()) {
				return slice;
			}

			CassandraPageRequest cassandraPageRequest = (CassandraPageRequest) slice.getPageable();
			return new SliceImpl<>(slice.getContent(), cassandraPageRequest.withSort(pageable.getSort()), slice.hasNext());
		}
	}

	/**
	 * {@link CassandraQueryExecution} for a {@link org.springframework.data.domain.Window}.
	 *
	 * @author Mark Paluch
	 * @since 4.2
	 */
	final class WindowExecution implements CassandraQueryExecution {

		private final CassandraOperations operations;
		private final CassandraScrollPosition scrollPosition;
		private final Limit limit;

		public WindowExecution(CassandraOperations operations, CassandraScrollPosition scrollPosition, Limit limit) {
			this.operations = operations;
			this.scrollPosition = scrollPosition;
			this.limit = limit;
		}

		@Override
		public Object execute(Statement<?> statement, Class<?> type) {

			Statement<?> statementToUse = limit.isLimited() ? statement.setPageSize(limit.max()) : statement;

			if (!this.scrollPosition.isInitial()) {
				statementToUse = statementToUse.setPagingState(this.scrollPosition.getPagingState());
			}

			return WindowUtil.of(operations.slice(statementToUse, type));
		}
	}

	/**
	 * {@link CassandraQueryExecution} for collection returning queries.
	 *
	 * @author Mark Paluch
	 */
	final class CollectionExecution implements CassandraQueryExecution {

		private final CassandraOperations operations;

		CollectionExecution(CassandraOperations operations) {
			this.operations = operations;
		}

		@Override
		public Object execute(Statement<?> statement, Class<?> type) {
			return operations.select(statement, type);
		}
	}

	/**
	 * {@link CassandraQueryExecution} to return a single entity.
	 *
	 * @author Mark Paluch
	 */
	final class SingleEntityExecution implements CassandraQueryExecution {

		private final CassandraOperations operations;
		private final boolean limiting;

		SingleEntityExecution(CassandraOperations operations, boolean limiting) {
			this.operations = operations;
			this.limiting = limiting;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Object execute(Statement<?> statement, Class<?> type) {

			List<Object> objects = operations.select(statement, (Class) type);

			if (objects.isEmpty()) {
				return null;
			}

			if (objects.size() == 1 || limiting) {
				return objects.get(0);
			}

			throw new IncorrectResultSizeDataAccessException(1, objects.size());
		}
	}

	/**
	 * {@link CassandraQueryExecution} for an Exists query supporting count and regular row-data for exists calculation.
	 *
	 * @author Mark Paluch
	 * @since 2.1
	 */
	final class ExistsExecution implements CassandraQueryExecution {

		private final CassandraOperations operations;

		ExistsExecution(CassandraOperations operations) {
			this.operations = operations;
		}

		@Override
		public Object execute(Statement<?> statement, Class<?> type) {

			List<Row> resultSet = this.operations.select(statement, Row.class);

			if (resultSet.isEmpty()) {
				return false;
			}

			Row row = resultSet.get(0);

			if (resultSet.size() == 1 && ProjectionUtil.qualifiesAsCountProjection(row)) {

				Object object = row.getObject(0);

				return ((Number) object).longValue() > 0;
			}

			return true;
		}
	}

	/**
	 * {@link CassandraQueryExecution} to return a {@link com.datastax.oss.driver.api.core.cql.ResultSet}.
	 *
	 * @author Mark Paluch
	 */
	final class ResultSetQuery implements CassandraQueryExecution {

		private final CassandraOperations operations;

		ResultSetQuery(CassandraOperations operations) {
			this.operations = operations;
		}

		@Override
		public Object execute(Statement<?> statement, Class<?> type) {
			return operations.execute(statement);
		}
	}

	/**
	 * An {@link CassandraQueryExecution} that wraps the results of the given delegate with the given result processing.
	 *
	 * @author Mark Paluch
	 */
	final class ResultProcessingExecution implements CassandraQueryExecution {

		private final CassandraQueryExecution delegate;
		private final Converter<Object, Object> converter;

		ResultProcessingExecution(CassandraQueryExecution delegate, Converter<Object, Object> converter) {
			this.delegate = delegate;
			this.converter = converter;
		}

		@Nullable
		@Override
		public Object execute(Statement<?> statement, Class<?> type) {

			Object result = delegate.execute(statement, type);

			return result != null ? converter.convert(result) : null;
		}
	}

	/**
	 * A {@link Converter} to post-process all source objects using the given {@link ResultProcessor}.
	 *
	 * @author Mark Paluch
	 */
	final class ResultProcessingConverter implements Converter<Object, Object> {

		private final ResultProcessor processor;
		private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;
		private final EntityInstantiators instantiators;

		ResultProcessingConverter(ResultProcessor processor,
				MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext,
				EntityInstantiators instantiators) {
			this.processor = processor;
			this.mappingContext = mappingContext;
			this.instantiators = instantiators;
		}

		@Override
		public Object convert(@Nullable Object source) {

			ReturnedType returnedType = processor.getReturnedType();

			if (ClassUtils.isPrimitiveOrWrapper(returnedType.getReturnedType())) {
				return source;
			}

			if (source != null && returnedType.isInstance(source)) {
				return source;
			}

			Converter<Object, Object> converter = new DtoInstantiatingConverter(returnedType.getReturnedType(),
					mappingContext, instantiators);

			return processor.processResult(source, converter);
		}
	}
}
