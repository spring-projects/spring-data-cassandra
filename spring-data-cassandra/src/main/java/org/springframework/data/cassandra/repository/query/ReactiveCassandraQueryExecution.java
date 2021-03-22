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
package org.springframework.data.cassandra.repository.query;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import org.reactivestreams.Publisher;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.util.ClassUtils;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Reactive query executions for Cassandra.
 *
 * @author Mark Paluch
 * @since 2.0
 */
@FunctionalInterface
interface ReactiveCassandraQueryExecution {

	Publisher<? extends Object> execute(Statement<?> statement, Class<?> type);

	/**
	 * {@link ReactiveCassandraQueryExecution} for a {@link org.springframework.data.domain.Slice}.
	 *
	 * @author Hleb Albau
	 * @author Mark Paluch
	 * @since 2.1
	 */
	final class SlicedExecution implements ReactiveCassandraQueryExecution {

		private final ReactiveCassandraOperations operations;
		private final Pageable pageable;

		SlicedExecution(ReactiveCassandraOperations operations, Pageable pageable) {
			this.operations = operations;
			this.pageable = pageable;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.repository.query.CassandraQueryExecution#execute(java.lang.String, java.lang.Class)
		 */
		@Override
		public Publisher<? extends Object> execute(Statement<?> statement, Class<?> type) {

			CassandraPageRequest.validatePageable(pageable);

			Statement<?> statementToUse = statement.setPageSize(pageable.getPageSize());

			if (pageable instanceof CassandraPageRequest) {
				statementToUse = statementToUse.setPagingState(((CassandraPageRequest) pageable).getPagingState());
			}
			Mono<? extends Slice<?>> slice = operations.slice(statementToUse, type);

			if (pageable.getSort().isUnsorted()) {
				return slice;
			}

			return slice.map(it -> {

				CassandraPageRequest cassandraPageRequest = (CassandraPageRequest) it.getPageable();
				return new SliceImpl<>(it.getContent(), cassandraPageRequest.withSort(pageable.getSort()), it.hasNext());

			});
		}
	}

	/**
	 * {@link ReactiveCassandraQueryExecution} for collection returning queries.
	 *
	 * @author Mark Paluch
	 */
	final class CollectionExecution implements ReactiveCassandraQueryExecution {

		private final ReactiveCassandraOperations operations;

		CollectionExecution(ReactiveCassandraOperations operations) {
			this.operations = operations;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution#execute(java.lang.String, java.lang.Class)
		 */
		@Override
		public Publisher<? extends Object> execute(Statement<?> statement, Class<?> type) {
			return operations.select(statement, type);
		}
	}

	/**
	 * {@link ReactiveCassandraQueryExecution} to return a single entity.
	 *
	 * @author Mark Paluch
	 */
	final class SingleEntityExecution implements ReactiveCassandraQueryExecution {

		private final ReactiveCassandraOperations operations;
		private final boolean limiting;

		SingleEntityExecution(ReactiveCassandraOperations operations, boolean limiting) {
			this.operations = operations;
			this.limiting = limiting;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution#execute(java.lang.String, java.lang.Class)
		 */
		@Override
		public Publisher<? extends Object> execute(Statement<?> statement, Class<?> type) {

			return operations.select(statement, type).buffer(2).handle((objects, sink) -> {

				if (objects.isEmpty()) {
					return;
				}

				if (objects.size() == 1 || limiting) {
					sink.next(objects.get(0));
					return;
				}

				sink.error(new IncorrectResultSizeDataAccessException(1, objects.size()));
			});
		}
	}

	/**
	 * {@link ReactiveCassandraQueryExecution} for an Exists query supporting count and regular row-data for exists
	 * calculation.
	 *
	 * @author Mark Paluch
	 * @since 2.1
	 */
	final class ExistsExecution implements ReactiveCassandraQueryExecution {

		private final ReactiveCassandraOperations operations;

		ExistsExecution(ReactiveCassandraOperations operations) {
			this.operations = operations;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution#execute(com.datastax.oss.driver.api.core.cql.Statement, java.lang.Class)
		 */
		@Override
		public Publisher<? extends Object> execute(Statement<?> statement, Class<?> type) {

			Mono<List<Row>> rows = this.operations.select(statement, Row.class).buffer(2).next();

			return rows.map(it -> {

				if (it.isEmpty()) {
					return false;
				}

				if (it.size() == 1) {

					Row row = it.get(0);

					if (ProjectionUtil.qualifiesAsCountProjection(row)) {

						Object object = row.getObject(0);

						return ((Number) object).longValue() > 0;
					}
				}

				return true;
			}).switchIfEmpty(Mono.just(false));
		}
	}

	/**
	 * An {@link ReactiveCassandraQueryExecution} that wraps the results of the given delegate with the given result
	 * processing.
	 *
	 * @author Mark Paluch
	 */
	final class ResultProcessingExecution implements ReactiveCassandraQueryExecution {

		private final ReactiveCassandraQueryExecution delegate;
		private final Converter<Object, Object> converter;

		ResultProcessingExecution(ReactiveCassandraQueryExecution delegate, Converter<Object, Object> converter) {
			this.delegate = delegate;
			this.converter = converter;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.cassandra.repository.query.ReactiveCassandraQueryExecution#execute(java.lang.String, java.lang.Class)
		 */
		@Override
		public Publisher<? extends Object> execute(Statement<?> statement, Class<?> type) {
			return (Publisher) converter.convert(delegate.execute(statement, type));
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Object convert(Object source) {

			ReturnedType returnedType = processor.getReturnedType();

			if (ReflectionUtils.isVoid(returnedType.getReturnedType())) {

				if (source instanceof Mono) {
					return ((Mono<?>) source).then();
				}

				if (source instanceof Publisher) {
					return Flux.from((Publisher<?>) source).then();
				}
			}

			if (ClassUtils.isPrimitiveOrWrapper(returnedType.getReturnedType())) {
				return source;
			}

			if (returnedType.isInstance(source)) {
				return source;
			}

			Converter<Object, Object> converter = new DtoInstantiatingConverter(returnedType.getReturnedType(),
					mappingContext, instantiators);

			return processor.processResult(source, converter);
		}
	}
}
