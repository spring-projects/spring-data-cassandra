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
package org.springframework.data.cassandra.repository.isolated;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.cassandra.repository.CountQuery;
import org.springframework.data.cassandra.repository.ExistsQuery;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;


/**
 * Integration tests for various return types on a Cassandra repository.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig
class RepositoryReturnTypesIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = RepositoryReturnTypesIntegrationTests.class,
			considerNestedRepositories = true)
	public static class Config extends IntegrationTestConfig {

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.singleton(AllPossibleTypes.class);
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}
	}

	@Autowired AllPossibleTypesRepository allPossibleTypesRepository;

	@BeforeEach
	void setUp() throws Exception {
		allPossibleTypesRepository.deleteAll();
	}

	@Test // DATACASS-271
	void shouldReturnOptional() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		allPossibleTypesRepository.save(entity);

		Optional<AllPossibleTypes> result = allPossibleTypesRepository.findOptionalById(entity.getId());
		assertThat(result).isPresent();
		assertThat(result.get()).isInstanceOf(AllPossibleTypes.class);
	}

	@Test // DATACASS-271
	void shouldReturnList() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		allPossibleTypesRepository.save(entity);

		List<AllPossibleTypes> result = allPossibleTypesRepository.findManyById(entity.getId());
		assertThat(result).isNotEmpty().contains(entity);
	}

	@Test // DATACASS-271
	void shouldReturnInetAddress() throws UnknownHostException {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setInet(InetAddress.getByName("localhost"));
		allPossibleTypesRepository.save(entity);

		InetAddress result = allPossibleTypesRepository.findInetAddressById(entity.getId());
		assertThat(result).isEqualTo(entity.getInet());
	}

	@Test // DATACASS-271
	void shouldReturnOptionalInetAddress() throws UnknownHostException {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setInet(InetAddress.getByName("localhost"));
		allPossibleTypesRepository.save(entity);

		Optional<InetAddress> result = allPossibleTypesRepository.findOptionalInetById(entity.getId());
		assertThat(result).isPresent().contains(entity.getInet());
	}

	@Test // DATACASS-271
	void shouldReturnBoxedByte() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setBoxedByte(Byte.valueOf("1"));
		allPossibleTypesRepository.save(entity);

		Byte result = allPossibleTypesRepository.findBoxedByteById(entity.getId());
		assertThat(result).isEqualTo(entity.getBoxedByte());
	}

	@Test // DATACASS-271
	void shouldReturnPrimitiveByte() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setPrimitiveByte(Byte.MAX_VALUE);
		allPossibleTypesRepository.save(entity);

		byte result = allPossibleTypesRepository.findPrimitiveByteById(entity.getId());
		assertThat(result).isEqualTo(entity.getPrimitiveByte());
	}

	@Test // DATACASS-271
	void shouldReturnBoxedShort() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setBoxedShort(Short.MAX_VALUE);
		allPossibleTypesRepository.save(entity);

		Short result = allPossibleTypesRepository.findBoxedShortById(entity.getId());
		assertThat(result).isEqualTo(entity.getBoxedShort());
	}

	@Test // DATACASS-271
	void shouldReturnBoxedLong() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setBoxedLong(Long.MAX_VALUE);
		allPossibleTypesRepository.save(entity);

		Long result = allPossibleTypesRepository.findBoxedLongById(entity.getId());
		assertThat(result).isEqualTo(entity.getBoxedLong());
	}

	@Test // DATACASS-271
	void shouldReturnBoxedInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setBoxedInteger(Integer.MAX_VALUE);
		allPossibleTypesRepository.save(entity);

		Integer result = allPossibleTypesRepository.findBoxedIntegerById(entity.getId());
		assertThat(result).isEqualTo(entity.getBoxedInteger());
	}

	@Test // DATACASS-271
	void shouldReturnBoxedDouble() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setBoxedDouble(Double.MAX_VALUE);
		allPossibleTypesRepository.save(entity);

		Double result = allPossibleTypesRepository.findBoxedDoubleById(entity.getId());
		assertThat(result).isEqualTo(entity.getBoxedDouble());
	}

	@Test // DATACASS-271
	void shouldReturnBoxedDoubleFromInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setBoxedInteger(Integer.MAX_VALUE);
		allPossibleTypesRepository.save(entity);

		Double result = allPossibleTypesRepository.findDoubleFromIntegerById(entity.getId());
		assertThat(result).isCloseTo(entity.getBoxedInteger(), offset(0.01d));
	}

	@Test // DATACASS-271
	void shouldReturnBoxedBoolean() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setBoxedBoolean(true);
		allPossibleTypesRepository.save(entity);

		Boolean result = allPossibleTypesRepository.findBoxedBooleanById(entity.getId());
		assertThat(result).isEqualTo(entity.getBoxedBoolean());
	}

	@Test // DATACASS-271
	void shouldReturnDate() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setDate(LocalDate.ofEpochDay(1));
		allPossibleTypesRepository.save(entity);

		LocalDate result = allPossibleTypesRepository.findLocalDateById(entity.getId());
		assertThat(result).isEqualTo(entity.getDate());
	}

	@Test // DATACASS-271
	void shouldReturnTimestamp() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setTimestamp(new Date(1));
		allPossibleTypesRepository.save(entity);

		Date result = allPossibleTypesRepository.findTimestampById(entity.getId());
		assertThat(result).isEqualTo(entity.getTimestamp());
	}

	@Test // DATACASS-271
	void shouldReturnBigDecimal() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setBigDecimal(BigDecimal.ONE);
		allPossibleTypesRepository.save(entity);

		BigDecimal result = allPossibleTypesRepository.findBigDecimalById(entity.getId());
		assertThat(result).isEqualTo(entity.getBigDecimal());
	}

	@Test // DATACASS-271
	void shouldReturnBigInteger() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		entity.setBigInteger(BigInteger.ONE);
		allPossibleTypesRepository.save(entity);

		BigInteger result = allPossibleTypesRepository.findBigIntegerById(entity.getId());
		assertThat(result).isEqualTo(entity.getBigInteger());
	}

	@Test // DATACASS-271
	void shouldReturnEntityAsMap() {

		AllPossibleTypes entity = new AllPossibleTypes("123");

		entity.setPrimitiveInteger(123);
		entity.setBigInteger(BigInteger.ONE);
		allPossibleTypesRepository.save(entity);

		Map<String, Object> result = allPossibleTypesRepository.findEntityAsMapById(entity.getId());

		assertThat(result).hasSizeGreaterThan(30);
		assertThat(result.get("primitiveinteger")).isEqualTo((Object) Integer.valueOf(123));
		assertThat(result.get("biginteger")).isEqualTo((Object) BigInteger.ONE);
	}

	@Test // DATACASS-512
	void shouldApplyCountProjection() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		allPossibleTypesRepository.save(entity);

		assertThat(allPossibleTypesRepository.countById(entity.getId())).isOne();
		assertThat(allPossibleTypesRepository.countById("foo")).isZero();
	}

	@Test // DATACASS-512
	void shouldApplyExistsProjection() {

		AllPossibleTypes entity = new AllPossibleTypes("123");
		allPossibleTypesRepository.save(entity);

		assertThat(allPossibleTypesRepository.existsUsingCountProjectionById(entity.getId())).isTrue();
		assertThat(allPossibleTypesRepository.existsUsingCountProjectionById("foo")).isFalse();

		assertThat(allPossibleTypesRepository.existsWithRowsById(entity.getId())).isTrue();
		assertThat(allPossibleTypesRepository.existsWithRowsById("foo")).isFalse();
	}

	public interface AllPossibleTypesRepository extends CrudRepository<AllPossibleTypes, String> {

		// blob/byte-buffer result do not work yet.

		// returning a map field does not work yet.

		@Query("select * from allpossibletypes where id = ?0")
		Optional<AllPossibleTypes> findOptionalById(String id);

		@Query("select * from allpossibletypes where id = ?0")
		List<AllPossibleTypes> findManyById(String id);

		@Query("select inet from allpossibletypes where id = ?0")
		InetAddress findInetAddressById(String id);

		@Query("select inet from allpossibletypes where id = ?0")
		Optional<InetAddress> findOptionalInetById(String id);

		@Query("select boxedByte from allpossibletypes where id = ?0")
		Byte findBoxedByteById(String id);

		@Query("select primitiveByte from allpossibletypes where id = ?0")
		byte findPrimitiveByteById(String id);

		@Query("select boxedShort from allpossibletypes where id = ?0")
		Short findBoxedShortById(String id);

		@Query("select boxedLong from allpossibletypes where id = ?0")
		Long findBoxedLongById(String id);

		@Query("select boxedInteger from allpossibletypes where id = ?0")
		Integer findBoxedIntegerById(String id);

		@Query("select boxedInteger from allpossibletypes where id = ?0")
		Double findDoubleFromIntegerById(String id);

		@Query("select boxedDouble from allpossibletypes where id = ?0")
		Double findBoxedDoubleById(String id);

		@Query("select boxedBoolean from allpossibletypes where id = ?0")
		Boolean findBoxedBooleanById(String id);

		@Query("select date from allpossibletypes where id = ?0")
		LocalDate findLocalDateById(String id);

		@Query("select timestamp from allpossibletypes where id = ?0")
		Date findTimestampById(String id);

		@Query("select bigDecimal from allpossibletypes where id = ?0")
		BigDecimal findBigDecimalById(String id);

		@Query("select bigInteger from allpossibletypes where id = ?0")
		BigInteger findBigIntegerById(String id);

		@Query("select * from allpossibletypes where id = ?0")
		Map<String, Object> findEntityAsMapById(String id);

		@CountQuery("select COUNT(*) from allpossibletypes where id = ?0")
		long countById(String id);

		@ExistsQuery("select COUNT(*) from allpossibletypes where id = ?0")
		boolean existsUsingCountProjectionById(String id);

		@ExistsQuery("select * from allpossibletypes where id = ?0")
		boolean existsWithRowsById(String id);
	}
}
