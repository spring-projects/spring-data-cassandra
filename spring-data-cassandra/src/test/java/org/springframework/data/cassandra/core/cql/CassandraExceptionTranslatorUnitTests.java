/*
 * Copyright 2013-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.cassandra.*;
import org.springframework.util.ClassUtils;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;

/**
 * Unit tests for {@link CassandraExceptionTranslator}
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraExceptionTranslatorUnitTests {

	InetSocketAddress socketAddress = new InetSocketAddress("localhost", 42);
	EndPoint endPoint = new DefaultEndPoint(socketAddress);
	Node node = mock(Node.class);
	CassandraExceptionTranslator sut = new CassandraExceptionTranslator();

	@Test // DATACASS-402
	public void shouldTranslateAuthenticationException() {

		DataAccessException result = sut.translateExceptionIfPossible(new AuthenticationException(endPoint, "message"));

		assertThat(result).isInstanceOf(CassandraAuthenticationException.class)
				.hasMessageStartingWith("Authentication error on host").hasCauseInstanceOf(AuthenticationException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateNoHostAvailableException() {

		DataAccessException result = sut.translateExceptionIfPossible(new NoNodeAvailableException());

		assertThat(result).isInstanceOf(CassandraConnectionFailureException.class)
				.hasMessageStartingWith("All host(s) tried").hasCauseInstanceOf(NoNodeAvailableException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateInvalidQueryException() {

		DataAccessException result = sut.translateExceptionIfPossible(new InvalidQueryException(node, "message"));

		assertThat(result).isInstanceOf(CassandraInvalidQueryException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(InvalidQueryException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateInvalidConfigurationInQueryException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new InvalidConfigurationInQueryException(node, "message"));

		assertThat(result).isInstanceOf(CassandraInvalidConfigurationInQueryException.class)
				.hasMessageStartingWith("message").hasCauseInstanceOf(InvalidConfigurationInQueryException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateUnauthorizedException() {

		DataAccessException result = sut.translateExceptionIfPossible(new UnauthorizedException(node, "message"));

		assertThat(result).isInstanceOf(CassandraUnauthorizedException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(UnauthorizedException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateSyntaxError() {

		DataAccessException result = sut.translateExceptionIfPossible(new SyntaxError(node, "message"));

		assertThat(result).isInstanceOf(CassandraQuerySyntaxException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(SyntaxError.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateKeyspaceExistsException() {

		AlreadyExistsException cx = new AlreadyExistsException(node, "keyspace", "");
		DataAccessException result = sut.translateExceptionIfPossible(cx);

		assertThat(result).isInstanceOf(CassandraSchemaElementExistsException.class)
				.hasMessageStartingWith("Keyspace keyspace already exists").hasCauseInstanceOf(AlreadyExistsException.class);

		CassandraSchemaElementExistsException exception = (CassandraSchemaElementExistsException) result;
	}

	@Test // DATACASS-402
	public void shouldTranslateUnavailableException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new UnavailableException(node, DefaultConsistencyLevel.ALL, 5, 1));

		assertThat(result).isInstanceOf(CassandraInsufficientReplicasAvailableException.class)
				.hasMessageStartingWith("Not enough replicas available").hasCauseInstanceOf(UnavailableException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateBootstrappingException() {

		DataAccessException result = sut.translateExceptionIfPossible(new BootstrappingException(node));

		assertThat(result).isInstanceOf(TransientDataAccessResourceException.class).hasMessageStartingWith("Queried host")
				.hasCauseInstanceOf(BootstrappingException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateOverloadedException() {

		DataAccessException result = sut.translateExceptionIfPossible(new OverloadedException(node));

		assertThat(result).isInstanceOf(TransientDataAccessResourceException.class).hasMessageStartingWith("Queried host")
				.hasCauseInstanceOf(OverloadedException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateTruncateException() {

		DataAccessException result = sut.translateExceptionIfPossible(new TruncateException(node, "message"));

		assertThat(result).isInstanceOf(CassandraTruncateException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(TruncateException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateWriteFailureException() {

		DataAccessException result = sut.translateExceptionIfPossible(
				new WriteFailureException(node, DefaultConsistencyLevel.ALL, 1, 5, WriteType.BATCH, 1, Collections.emptyMap()));

		assertThat(result).isInstanceOf(DataAccessResourceFailureException.class)
				.hasMessageStartingWith("Cassandra failure during").hasCauseInstanceOf(WriteFailureException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateReadFailureException() {

		DataAccessException result = sut.translateExceptionIfPossible(
				new ReadFailureException(node, DefaultConsistencyLevel.ALL, 1, 5, 1, true, Collections.emptyMap()));

		assertThat(result).isInstanceOf(DataAccessResourceFailureException.class)
				.hasMessageStartingWith("Cassandra failure during").hasCauseInstanceOf(ReadFailureException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateWriteTimeoutException() {

		DataAccessException result = sut.translateExceptionIfPossible(
				new WriteTimeoutException(node, DefaultConsistencyLevel.ALL, 1, 5, WriteType.BATCH));

		assertThat(result).isInstanceOf(CassandraWriteTimeoutException.class)
				.hasMessageStartingWith("Cassandra timeout during").hasCauseInstanceOf(WriteTimeoutException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateReadTimeoutException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new ReadTimeoutException(node, DefaultConsistencyLevel.ALL, 1, 5, true));

		assertThat(result).isInstanceOf(CassandraReadTimeoutException.class)
				.hasMessageStartingWith("Cassandra timeout during").hasCauseInstanceOf(ReadTimeoutException.class);
	}

	@Test // DATACASS-402
	public void shouldTranslateBusyConnectionException() {

		DataAccessException result = sut.translateExceptionIfPossible(new BusyConnectionException(2));

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

		assertThat(
				sut.translateExceptionIfPossible(new CodecNotFoundException(DataTypes.ASCII, GenericType.of(String.class))))
						.isInstanceOf(CassandraUncategorizedException.class);

		assertThat(sut.translateExceptionIfPossible(new UnsupportedProtocolVersionException(endPoint, "Foo",
				Arrays.asList(ProtocolVersion.V3, ProtocolVersion.V4)))).isInstanceOf(CassandraUncategorizedException.class);
	}

	@Test // DATACASS-335
	public void shouldTranslateWithCqlMessage() {

		InvalidConfigurationInQueryException cx = new InvalidConfigurationInQueryException(node, "err");
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
