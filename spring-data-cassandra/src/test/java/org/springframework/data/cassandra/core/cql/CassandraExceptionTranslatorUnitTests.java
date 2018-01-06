/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.cassandra.*;
import org.springframework.data.cassandra.CassandraSchemaElementExistsException.ElementType;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.*;
import com.google.common.reflect.TypeToken;

/**
 * Unit tests for {@link CassandraExceptionTranslator}
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraExceptionTranslatorUnitTests {

	InetSocketAddress socketAddress = new InetSocketAddress("localhost", 42);
	CassandraExceptionTranslator sut = new CassandraExceptionTranslator();

	@Test // DATACASS-402
	public void shouldTranslateAuthenticationException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new AuthenticationException(socketAddress, "message"));

		assertThat(result).isInstanceOf(CassandraAuthenticationException.class)
				.hasMessageStartingWith("Authentication error on host").hasCauseInstanceOf(AuthenticationException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateCassandraInternalException() {

		DataAccessException result = sut.translateExceptionIfPossible(new DriverInternalError("message"));

		assertThat(result).isInstanceOf(CassandraInternalException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(DriverInternalError.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateTraceRetrievalException() {

		DataAccessException result = sut.translateExceptionIfPossible(new TraceRetrievalException("message"));

		assertThat(result).isInstanceOf(CassandraTraceRetrievalException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(TraceRetrievalException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateNoHostAvailableException() {

		DataAccessException result = sut.translateExceptionIfPossible(
				new NoHostAvailableException(Collections.singletonMap(socketAddress, new IllegalStateException())));

		assertThat(result).isInstanceOf(CassandraConnectionFailureException.class)
				.hasMessageStartingWith("All host(s) tried").hasCauseInstanceOf(NoHostAvailableException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateInvalidQueryException() {

		DataAccessException result = sut.translateExceptionIfPossible(new InvalidQueryException(socketAddress, "message"));

		assertThat(result).isInstanceOf(CassandraInvalidQueryException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(InvalidQueryException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateInvalidConfigurationInQueryException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new InvalidConfigurationInQueryException(socketAddress, "message"));

		assertThat(result).isInstanceOf(CassandraInvalidConfigurationInQueryException.class)
				.hasMessageStartingWith("message").hasCauseInstanceOf(InvalidConfigurationInQueryException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateUnauthorizedException() {

		DataAccessException result = sut.translateExceptionIfPossible(new UnauthorizedException(socketAddress, "message"));

		assertThat(result).isInstanceOf(CassandraUnauthorizedException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(UnauthorizedException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateSyntaxError() {

		DataAccessException result = sut.translateExceptionIfPossible(new SyntaxError(socketAddress, "message"));

		assertThat(result).isInstanceOf(CassandraQuerySyntaxException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(SyntaxError.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateKeyspaceExistsException() {

		AlreadyExistsException cx = new AlreadyExistsException("keyspace", "");
		DataAccessException result = sut.translateExceptionIfPossible(cx);

		assertThat(result).isInstanceOf(CassandraKeyspaceExistsException.class)
				.hasMessageStartingWith("Keyspace keyspace already exists").hasCauseInstanceOf(AlreadyExistsException.class);

		CassandraSchemaElementExistsException exception = (CassandraSchemaElementExistsException) result;

		assertThat(exception.getElementName()).isEqualTo("keyspace");
		assertThat(exception.getElementType()).isEqualTo(ElementType.KEYSPACE);
	}

	@Test // DATACASS-402
	public void shouldTranslateTableExistsException() {

		AlreadyExistsException cx = new AlreadyExistsException("keyspace", "table");
		DataAccessException result = sut.translateExceptionIfPossible(cx);

		assertThat(result).isInstanceOf(CassandraTableExistsException.class)
				.hasMessageStartingWith("Table keyspace.table already exists").hasCauseInstanceOf(AlreadyExistsException.class);

		CassandraSchemaElementExistsException exception = (CassandraSchemaElementExistsException) result;

		assertThat(exception.getElementName()).isEqualTo("table");
		assertThat(exception.getElementType()).isEqualTo(ElementType.TABLE);
	}

	@Test // DATACASS-402
	public void shouldTranslateInvalidTypeException() {

		DataAccessException result = sut.translateExceptionIfPossible(new InvalidTypeException("message"));

		assertThat(result).isInstanceOf(CassandraTypeMismatchException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(InvalidTypeException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateUnavailableException() {

		DataAccessException result = sut.translateExceptionIfPossible(new UnavailableException(ConsistencyLevel.ALL, 5, 1));

		assertThat(result).isInstanceOf(CassandraInsufficientReplicasAvailableException.class)
				.hasMessageStartingWith("Not enough replicas available").hasCauseInstanceOf(UnavailableException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateBootstrappingException() {

		DataAccessException result = sut.translateExceptionIfPossible(new BootstrappingException(socketAddress, "message"));

		assertThat(result).isInstanceOf(TransientDataAccessResourceException.class).hasMessageStartingWith("Queried host")
				.hasCauseInstanceOf(BootstrappingException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateOverloadedException() {

		DataAccessException result = sut.translateExceptionIfPossible(new OverloadedException(socketAddress, "message"));

		assertThat(result).isInstanceOf(TransientDataAccessResourceException.class).hasMessageStartingWith("Queried host")
				.hasCauseInstanceOf(OverloadedException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateTruncateException() {

		DataAccessException result = sut.translateExceptionIfPossible(new TruncateException(socketAddress, "message"));

		assertThat(result).isInstanceOf(CassandraTruncateException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(TruncateException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateWriteFailureException() {

		DataAccessException result = sut.translateExceptionIfPossible(
				new WriteFailureException(ConsistencyLevel.ALL, WriteType.BATCH, 1, 5, 1, Collections.emptyMap()));

		assertThat(result).isInstanceOf(DataAccessResourceFailureException.class)
				.hasMessageStartingWith("Cassandra failure during").hasCauseInstanceOf(WriteFailureException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateReadFailureException() {

		DataAccessException result = sut.translateExceptionIfPossible(
				new ReadFailureException(ConsistencyLevel.ALL, 1, 5, 1, Collections.emptyMap(), true));

		assertThat(result).isInstanceOf(DataAccessResourceFailureException.class)
				.hasMessageStartingWith("Cassandra failure during").hasCauseInstanceOf(ReadFailureException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateWriteTimeoutException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new WriteTimeoutException(ConsistencyLevel.ALL, WriteType.BATCH, 1, 5));

		assertThat(result).isInstanceOf(CassandraWriteTimeoutException.class)
				.hasMessageStartingWith("Cassandra timeout during").hasCauseInstanceOf(WriteTimeoutException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateReadTimeoutException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new ReadTimeoutException(ConsistencyLevel.ALL, 1, 5, true));

		assertThat(result).isInstanceOf(CassandraReadTimeoutException.class)
				.hasMessageStartingWith("Cassandra timeout during").hasCauseInstanceOf(ReadTimeoutException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateFunctionExecutionException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new FunctionExecutionException(socketAddress, "message"));

		assertThat(result).isInstanceOf(DataAccessResourceFailureException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(FunctionExecutionException.class);
	}

	@Test // DATACASS-402
	@SuppressWarnings("unchecked")
	public void shouldTranslateBusyPoolException() throws Exception {

		assumeTrue(
				ClassUtils.isPresent("com.datastax.driver.core.exceptions.BusyPoolException", getClass().getClassLoader()));

		DriverException exception = createInstance("com.datastax.driver.core.exceptions.BusyPoolException",
				new Class[] { InetSocketAddress.class, Integer.TYPE }, socketAddress, 5);

		DataAccessException result = sut.translateExceptionIfPossible(exception);

		assertThat(result).isInstanceOf(CassandraConnectionFailureException.class).hasMessageContaining("Pool is busy")
				.hasCauseInstanceOf(exception.getClass());
	}

	@Test // DATACASS-402
	public void shouldTranslateConnectionException() {

		DataAccessException result = sut.translateExceptionIfPossible(new ConnectionException(socketAddress, "message"));

		assertThat(result).isInstanceOf(CassandraConnectionFailureException.class).hasMessageContaining("] message")
				.hasCauseInstanceOf(ConnectionException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateBusyConnectionException() {

		DataAccessException result = sut.translateExceptionIfPossible(new BusyConnectionException(socketAddress));

		assertThat(result).isInstanceOf(CassandraConnectionFailureException.class)
				.hasMessageContaining("Connection has run out of stream").hasCauseInstanceOf(BusyConnectionException.class);
	}

	@Test // DATACASS-402
	@SuppressWarnings("unchecked")
	public void shouldTranslateFrameTooLongException() throws Exception {

		assumeTrue(
				ClassUtils.isPresent("com.datastax.driver.core.exceptions.FrameTooLongException", getClass().getClassLoader()));

		DriverException exception = createInstance("com.datastax.driver.core.exceptions.FrameTooLongException",
				new Class[] { Integer.TYPE }, 5);

		DataAccessException result = sut.translateExceptionIfPossible(exception);

		assertThat(result).isInstanceOf(CassandraUncategorizedException.class).hasCauseInstanceOf(exception.getClass());
	}

	@Test // DATACASS-402
	public void shouldTranslateToUncategorized() {

		assertThat(sut.translateExceptionIfPossible(
				new CodecNotFoundException("message", DataType.ascii(), TypeToken.of(Class.class))))
						.isInstanceOf(CassandraUncategorizedException.class);

		assertThat(sut.translateExceptionIfPossible(
				new UnsupportedProtocolVersionException(socketAddress, ProtocolVersion.NEWEST_SUPPORTED, ProtocolVersion.V1)))
						.isInstanceOf(CassandraUncategorizedException.class);

		assertThat(sut.translateExceptionIfPossible(new UnpreparedException(socketAddress, "message")))
				.isInstanceOf(CassandraUncategorizedException.class);

		assertThat(sut.translateExceptionIfPossible(new PagingStateException("message")))
				.isInstanceOf(CassandraUncategorizedException.class);

		assertThat(sut.translateExceptionIfPossible(new UnresolvedUserTypeException("keyspace", "message")))
				.isInstanceOf(CassandraUncategorizedException.class);

		assertThat(
				sut.translateExceptionIfPossible(new UnsupportedFeatureException(ProtocolVersion.NEWEST_SUPPORTED, "message")))
						.isInstanceOf(CassandraUncategorizedException.class);

		assertThat(sut.translateExceptionIfPossible(new UnresolvedUserTypeException("keyspace", "message")))
				.isInstanceOf(CassandraUncategorizedException.class);
	}

	@Test // DATACASS-335
	public void shouldTranslateWithCqlMessage() {

		InvalidQueryException cx = new InvalidConfigurationInQueryException(null, "err");
		DataAccessException dax = sut.translate("Query", "SELECT * FROM person", cx);

		assertThat(dax).hasRootCauseInstanceOf(InvalidQueryException.class).hasMessage(
				"Query; CQL [SELECT * FROM person]; err; nested exception is com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException: err");
	}

	@SuppressWarnings("unchecked")
	public <T> T createInstance(String className, Class<?> argTypes[], Object... args)
			throws ReflectiveOperationException {

		Class<T> exceptionClass = (Class) ClassUtils.forName(className, getClass().getClassLoader());
		Constructor<T> constructor = exceptionClass.getDeclaredConstructor(argTypes);

		return constructor.newInstance(args);
	}
}
