/*
 * Copyright 2020-present the original author or authors.
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

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.springframework.data.cassandra.core.cql.QueryExtractorDelegate;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.function.SingletonSupplier;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Support class for Cassandra Template API implementation classes that want to make use of prepared statements.
 *
 * @author Mark Paluch
 * @since 3.2
 */
class PreparedStatementDelegate {

	/**
	 * Bind values held in {@link SimpleStatement} to the {@link PreparedStatement} and apply query options that are set
	 * or do not match the default value.
	 *
	 * @param source
	 * @param ps
	 * @return the bound statement.
	 */
	static BoundStatement bind(SimpleStatement source, PreparedStatement ps) {

		BoundStatementBuilder builder = ps.boundStatementBuilder(source.getPositionalValues().toArray());

		Mapper mapper = Mapper.INSTANCE;

		mapper.from(source.getExecutionProfileName()).whenHasText().to(builder::setExecutionProfileName);
		mapper.from(source.getExecutionProfile()).whenNonNull().to(builder::setExecutionProfile);
		mapper.from(source.getRoutingKeyspace()).whenNonNull().to(builder::setRoutingKeyspace);
		mapper.from(source.getRoutingKey()).whenNonNull().to(builder::setRoutingKey);
		mapper.from(source.getRoutingToken()).whenNonNull().to(builder::setRoutingToken);
		mapper.from(source.isIdempotent()).whenNonNull().to(builder::setIdempotence);
		mapper.from(source.isTracing()).whenNonNull().to(builder::setTracing);
		mapper.from(source.getQueryTimestamp()).whenNot(it -> it == Statement.NO_DEFAULT_TIMESTAMP)
				.to(builder::setQueryTimestamp);
		mapper.from(source.getPagingState()).whenNonNull().to(builder::setPagingState);
		mapper.from(source.getPageSize()).whenNot(it -> it == 0L).to(builder::setPageSize);
		mapper.from(source.getConsistencyLevel()).whenNonNull().to(builder::setConsistencyLevel);
		mapper.from(source.getSerialConsistencyLevel()).whenNonNull().to(builder::setSerialConsistencyLevel);
		mapper.from(source.getTimeout()).whenNonNull().to(builder::setTimeout);
		mapper.from(source.getNode()).whenNonNull().to(builder::setNode);
		mapper.from(source.getNowInSeconds()).whenNot(it -> it == Statement.NO_NOW_IN_SECONDS).to(builder::setNowInSeconds);

		Map<CqlIdentifier, Object> namedValues = source.getNamedValues();

		ColumnDefinitions variableDefinitions = ps.getVariableDefinitions();
		CodecRegistry codecRegistry = builder.codecRegistry();
		for (Map.Entry<CqlIdentifier, Object> entry : namedValues.entrySet()) {

			if (entry.getValue() == null) {
				builder = builder.setToNull(entry.getKey());
			} else {
				DataType type = variableDefinitions.get(entry.getKey()).getType();
				builder = builder.set(entry.getKey(), entry.getValue(), codecRegistry.codecFor(type));
			}
		}

		return builder.build();
	}

	/**
	 * Ensure the given {@link Statement} is a {@link SimpleStatement}. Throw a {@link IllegalArgumentException}
	 * otherwise.
	 *
	 * @param statement
	 * @return the {@link SimpleStatement}.
	 */
	static SimpleStatement getStatementForPrepare(Statement<?> statement) {

		if (statement instanceof SimpleStatement) {
			return (SimpleStatement) statement;
		}

		throw new IllegalArgumentException(getMessage(statement));
	}

	/**
	 * Check whether to use prepared statements. When {@code usePreparedStatements} is {@literal true}, then verifying
	 * additionally that the given {@link Statement} is a {@link SimpleStatement}, otherwise log the mismatch and fallback
	 * to non-prepared usage.
	 *
	 * @param usePreparedStatements
	 * @param statement
	 * @param logger
	 * @return
	 */
	static boolean canPrepare(boolean usePreparedStatements, Statement<?> statement, Log logger) {

		if (usePreparedStatements) {

			if (statement instanceof SimpleStatement) {
				return true;
			}

			logger.warn(getMessage(statement));
		}

		return false;
	}

	private static String getMessage(Statement<?> statement) {

		String cql = QueryExtractorDelegate.getCql(statement);

		if (StringUtils.hasText(cql)) {
			return String.format("Cannot prepare statement %s (%s); Statement must be a SimpleStatement", cql, statement);
		}

		return String.format("Cannot prepare statement %s; Statement must be a SimpleStatement", statement);
	}

	enum Mapper {

		INSTANCE;

		/**
		 * Return a new {@link Source} from the specified value supplier that can be used to perform the mapping.
		 *
		 * @param <T> the source type
		 * @param supplier the value supplier
		 * @return a {@link Source} that can be used to complete the mapping
		 * @see #from(Object)
		 */
		public <T> Source<T> from(Supplier<T> supplier) {

			Assert.notNull(supplier, "Supplier must not be null");
			return getSource(supplier);
		}

		/**
		 * Return a new {@link Source} from the specified value that can be used to perform the mapping.
		 *
		 * @param <T> the source type
		 * @param value the value
		 * @return a {@link Source} that can be used to complete the mapping
		 */
		public <T> Source<T> from(@Nullable T value) {
			return from(() -> value);
		}

		private <T> Source<T> getSource(Supplier<T> supplier) {
			return new Source<>(SingletonSupplier.of(supplier), t -> true);
		}
	}

	/**
	 * A source value/supplier that is in the process of being mapped.
	 *
	 * @param <T> the source type
	 */
	static class Source<T> {

		private final Supplier<T> supplier;

		private final Predicate<T> predicate;

		private Source(Supplier<T> supplier, Predicate<T> predicate) {

			Assert.notNull(predicate, "Predicate must not be null");

			this.supplier = supplier;
			this.predicate = predicate;
		}

		/**
		 * Return a filtered version of the source that won't map non-null values or suppliers that throw a
		 * {@link NullPointerException}.
		 *
		 * @return a new filtered source instance
		 */
		public Source<T> whenNonNull() {
			return new Source<>(this.supplier, Objects::nonNull);
		}

		/**
		 * Return a filtered version of the source that will only map values that are {@code true}.
		 *
		 * @return a new filtered source instance
		 */
		public Source<T> whenTrue() {
			return when(Boolean.TRUE::equals);
		}

		/**
		 * Return a filtered version of the source that will only map values that are {@code false}.
		 *
		 * @return a new filtered source instance
		 */
		public Source<T> whenFalse() {
			return when(Boolean.FALSE::equals);
		}

		/**
		 * Return a filtered version of the source that will only map values that have a {@code toString()} containing
		 * actual text.
		 *
		 * @return a new filtered source instance
		 */
		public Source<T> whenHasText() {
			return when((value) -> StringUtils.hasText(Objects.toString(value, null)));
		}

		/**
		 * Return a filtered version of the source that will only map values equal to the specified {@code object}.
		 *
		 * @param object the object to match
		 * @return a new filtered source instance
		 */
		public Source<T> whenEqualTo(Object object) {
			return when(object::equals);
		}

		/**
		 * Return a filtered version of the source that won't map values that match the given predicate.
		 *
		 * @param predicate the predicate used to filter values
		 * @return a new filtered source instance
		 */
		public Source<T> whenNot(Predicate<T> predicate) {

			Assert.notNull(predicate, "Predicate must not be null");
			return when(predicate.negate());
		}

		/**
		 * Return a filtered version of the source that won't map values that don't match the given predicate.
		 *
		 * @param predicate the predicate used to filter values
		 * @return a new filtered source instance
		 */
		public Source<T> when(Predicate<T> predicate) {

			Assert.notNull(predicate, "Predicate must not be null");
			return new Source<>(this.supplier, (this.predicate != null) ? this.predicate.and(predicate) : predicate);
		}

		/**
		 * Complete the mapping by passing any non-filtered value to the specified consumer.
		 *
		 * @param consumer the consumer that should accept the value if it's not been filtered
		 */
		public void to(Consumer<T> consumer) {

			Assert.notNull(consumer, "Consumer must not be null");

			T value = this.supplier.get();
			if (this.predicate.test(value)) {
				consumer.accept(value);
			}
		}

	}

}
