/*
 * Copyright 2013-2023 the original author or authors.
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
import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.cassandra.CassandraAuthenticationException;
import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.data.cassandra.CassandraDriverTimeOutException;
import org.springframework.data.cassandra.CassandraInsufficientReplicasAvailableException;
import org.springframework.data.cassandra.CassandraInvalidConfigurationInQueryException;
import org.springframework.data.cassandra.CassandraInvalidQueryException;
import org.springframework.data.cassandra.CassandraQuerySyntaxException;
import org.springframework.data.cassandra.CassandraReadTimeoutException;
import org.springframework.data.cassandra.CassandraSchemaElementExistsException;
import org.springframework.data.cassandra.CassandraTruncateException;
import org.springframework.data.cassandra.CassandraUnauthorizedException;
import org.springframework.data.cassandra.CassandraUncategorizedException;
import org.springframework.data.cassandra.CassandraWriteTimeoutException;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.InvalidConfigurationInQueryException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;

/**
 * Unit tests for {@link CassandraExceptionTranslator}
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author Mikhail Polivakha
 */
class CassandraExceptionTranslatorUnitTests {

	private InetSocketAddress socketAddress = new InetSocketAddress("localhost", 42);
	private EndPoint endPoint = new DefaultEndPoint(socketAddress);
	private Node node = mock(Node.class);
	private CassandraExceptionTranslator sut = new CassandraExceptionTranslator();

	@Test // DATACASS-402
	void shouldTranslateAuthenticationException() {

		DataAccessException result = sut.translateExceptionIfPossible(new AuthenticationException(endPoint, "message"));

		assertThat(result).isInstanceOf(CassandraAuthenticationException.class)
				.hasMessageStartingWith("Authentication error").hasCauseInstanceOf(AuthenticationException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateNoHostAvailableException() {

		DataAccessException result = sut.translateExceptionIfPossible(new NoNodeAvailableException());

		assertThat(result).isInstanceOf(CassandraConnectionFailureException.class).hasMessageStartingWith("No node was")
				.hasCauseInstanceOf(NoNodeAvailableException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateInvalidQueryException() {

		DataAccessException result = sut.translateExceptionIfPossible(new InvalidQueryException(node, "message"));

		assertThat(result).isInstanceOf(CassandraInvalidQueryException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(InvalidQueryException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateInvalidConfigurationInQueryException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new InvalidConfigurationInQueryException(node, "message"));

		assertThat(result).isInstanceOf(CassandraInvalidConfigurationInQueryException.class)
				.hasMessageStartingWith("message").hasCauseInstanceOf(InvalidConfigurationInQueryException.class);
	}

	@Test
	void shouldRecognizeDriverTimeoutException() {
		DataAccessException dataAccessException = sut.translateExceptionIfPossible(new DriverTimeoutException("message"));

		assertThat(dataAccessException).isInstanceOf(CassandraDriverTimeOutException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(DriverTimeoutException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateUnauthorizedException() {

		DataAccessException result = sut.translateExceptionIfPossible(new UnauthorizedException(node, "message"));

		assertThat(result).isInstanceOf(CassandraUnauthorizedException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(UnauthorizedException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateSyntaxError() {

		DataAccessException result = sut.translateExceptionIfPossible(new SyntaxError(node, "message"));

		assertThat(result).isInstanceOf(CassandraQuerySyntaxException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(SyntaxError.class);
	}

	@Test // DATACASS-402
	void shouldTranslateKeyspaceExistsException() {

		AlreadyExistsException cx = new AlreadyExistsException(node, "keyspace", "");
		DataAccessException result = sut.translateExceptionIfPossible(cx);

		assertThat(result).isInstanceOf(CassandraSchemaElementExistsException.class)
				.hasMessageStartingWith("Keyspace keyspace already exists").hasCauseInstanceOf(AlreadyExistsException.class);

		CassandraSchemaElementExistsException exception = (CassandraSchemaElementExistsException) result;
	}

	@Test // DATACASS-402
	void shouldTranslateUnavailableException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new UnavailableException(node, DefaultConsistencyLevel.ALL, 5, 1));

		assertThat(result).isInstanceOf(CassandraInsufficientReplicasAvailableException.class)
				.hasMessageStartingWith("Not enough replicas available").hasCauseInstanceOf(UnavailableException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateBootstrappingException() {

		DataAccessException result = sut.translateExceptionIfPossible(new BootstrappingException(node));

		assertThat(result).isInstanceOf(TransientDataAccessResourceException.class).hasMessageContaining("bootstrapping")
				.hasCauseInstanceOf(BootstrappingException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateOverloadedException() {

		DataAccessException result = sut.translateExceptionIfPossible(new OverloadedException(node));

		assertThat(result).isInstanceOf(TransientDataAccessResourceException.class)
				.hasCauseInstanceOf(OverloadedException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateTruncateException() {

		DataAccessException result = sut.translateExceptionIfPossible(new TruncateException(node, "message"));

		assertThat(result).isInstanceOf(CassandraTruncateException.class).hasMessageStartingWith("message")
				.hasCauseInstanceOf(TruncateException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateWriteFailureException() {

		DataAccessException result = sut.translateExceptionIfPossible(
				new WriteFailureException(node, DefaultConsistencyLevel.ALL, 1, 5, WriteType.BATCH, 1, Collections.emptyMap()));

		assertThat(result).isInstanceOf(DataAccessResourceFailureException.class)
				.hasMessageStartingWith("Cassandra failure during").hasCauseInstanceOf(WriteFailureException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateReadFailureException() {

		DataAccessException result = sut.translateExceptionIfPossible(
				new ReadFailureException(node, DefaultConsistencyLevel.ALL, 1, 5, 1, true, Collections.emptyMap()));

		assertThat(result).isInstanceOf(DataAccessResourceFailureException.class)
				.hasMessageStartingWith("Cassandra failure during").hasCauseInstanceOf(ReadFailureException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateWriteTimeoutException() {

		DataAccessException result = sut.translateExceptionIfPossible(
				new WriteTimeoutException(node, DefaultConsistencyLevel.ALL, 1, 5, WriteType.BATCH));

		assertThat(result).isInstanceOf(CassandraWriteTimeoutException.class)
				.hasMessageStartingWith("Cassandra timeout during").hasCauseInstanceOf(WriteTimeoutException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateReadTimeoutException() {

		DataAccessException result = sut
				.translateExceptionIfPossible(new ReadTimeoutException(node, DefaultConsistencyLevel.ALL, 1, 5, true));

		assertThat(result).isInstanceOf(CassandraReadTimeoutException.class)
				.hasMessageStartingWith("Cassandra timeout during").hasCauseInstanceOf(ReadTimeoutException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateBusyConnectionException() {

		DataAccessException result = sut.translateExceptionIfPossible(new BusyConnectionException(2));

		assertThat(result).isInstanceOf(CassandraConnectionFailureException.class).hasMessageContaining("simultaneous")
				.hasCauseInstanceOf(BusyConnectionException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateFrameTooLongException() {

		DataAccessException result = sut.translateExceptionIfPossible(new FrameTooLongException(socketAddress, "foo"));

		assertThat(result).isInstanceOf(CassandraUncategorizedException.class)
				.hasCauseInstanceOf(FrameTooLongException.class);
	}

	@Test // DATACASS-402
	void shouldTranslateToUncategorized() {

		assertThat(
				sut.translateExceptionIfPossible(new CodecNotFoundException(DataTypes.ASCII, GenericType.of(String.class))))
						.isInstanceOf(CassandraUncategorizedException.class);

		assertThat(sut.translateExceptionIfPossible(new UnsupportedProtocolVersionException(endPoint, "Foo",
				Arrays.asList(ProtocolVersion.V3, ProtocolVersion.V4)))).isInstanceOf(CassandraUncategorizedException.class);
	}

	@Test // GH-1155
	void shouldNotTranslateUnknownExceptions() {
		assertThat(sut.translateExceptionIfPossible(new UnsupportedOperationException())).isNull();
	}

	@Test // DATACASS-335
	void shouldTranslateWithCqlMessage() {

		InvalidConfigurationInQueryException cx = new InvalidConfigurationInQueryException(node, "err");
		DataAccessException dax = sut.translate("Query", "SELECT * FROM person", cx);

		assertThat(dax).hasRootCauseInstanceOf(InvalidConfigurationInQueryException.class)
				.hasMessageContaining("Query; CQL [SELECT * FROM person]; err");
	}
}
