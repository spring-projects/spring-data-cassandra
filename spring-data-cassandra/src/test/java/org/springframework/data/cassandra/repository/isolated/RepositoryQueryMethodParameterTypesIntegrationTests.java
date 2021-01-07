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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.CassandraInvalidQueryException;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Integration tests for various query method parameter types.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig
@SuppressWarnings("Since15")
class RepositoryQueryMethodParameterTypesIntegrationTests
		extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = RepositoryQueryMethodParameterTypesIntegrationTests.class,
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

		@Override
		public CassandraAdminTemplate cassandraTemplate() {
			CassandraAdminTemplate template = super.cassandraTemplate();
			template.setUsePreparedStatements(false);
			return template;
		}
	}

	@Autowired AllPossibleTypesRepository allPossibleTypesRepository;
	@Autowired CqlSession session;
	@Autowired CassandraMappingContext mappingContext;
	@Autowired MappingCassandraConverter converter;

	@BeforeEach
	void setUp() throws Exception {
		allPossibleTypesRepository.deleteAll();
	}

	@Test // DATACASS-296
	void shouldFindByLocalDate() {

		session.execute("CREATE INDEX IF NOT EXISTS allpossibletypes_localdate ON allpossibletypes ( date )");

		AllPossibleTypes allPossibleTypes = new AllPossibleTypes();

		allPossibleTypes.setId("id");
		allPossibleTypes.setDate(LocalDate.now());

		allPossibleTypesRepository.save(allPossibleTypes);

		List<AllPossibleTypes> result = allPossibleTypesRepository.findWithCreatedDate(allPossibleTypes.getDate());

		assertThat(result).hasSize(1).contains(allPossibleTypes);
	}

	@Test // DATACASS-296
	void shouldFindByAnnotatedDateParameter() {

		CustomConversions customConversions = new CassandraCustomConversions(
				Collections.singletonList(new DateToLocalDateConverter()));

		mappingContext.setCustomConversions(customConversions);
		converter.setCustomConversions(customConversions);
		converter.afterPropertiesSet();

		session.execute("CREATE INDEX IF NOT EXISTS allpossibletypes_date ON allpossibletypes ( date )");

		AllPossibleTypes allPossibleTypes = new AllPossibleTypes();

		LocalDate localDate = LocalDate.now();
		Instant instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);

		allPossibleTypes.setId("id");
		allPossibleTypes.setDate(LocalDate.of(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()));

		allPossibleTypesRepository.save(allPossibleTypes);

		List<AllPossibleTypes> result = allPossibleTypesRepository.findWithAnnotatedDateParameter(Date.from(instant));

		assertThat(result).hasSize(1).contains(allPossibleTypes);
	}

	@Test // DATACASS-296, DATACASS-304
	void shouldThrowExceptionUsingWrongMethodParameter() {
		session.execute("CREATE INDEX IF NOT EXISTS allpossibletypes_date ON allpossibletypes ( date )");

		assertThatExceptionOfType(CassandraInvalidQueryException.class).isThrownBy(
				() -> allPossibleTypesRepository.findWithDateParameter(Date.from(Instant.ofEpochSecond(44234123421L))));
	}

	@Test // DATACASS-296
	void shouldFindByZoneId() {

		ZoneId zoneId = ZoneId.of("Europe/Paris");
		session.execute("CREATE INDEX IF NOT EXISTS allpossibletypes_zoneid ON allpossibletypes ( zoneid )");

		AllPossibleTypes allPossibleTypes = new AllPossibleTypes();

		allPossibleTypes.setId("id");
		allPossibleTypes.setZoneId(zoneId);

		allPossibleTypesRepository.save(allPossibleTypes);

		List<AllPossibleTypes> result = allPossibleTypesRepository.findWithZoneId(zoneId);

		assertThat(result).hasSize(1).contains(allPossibleTypes);
	}

	@Test // DATACASS-296
	void shouldFindByOptionalOfZoneId() {

		ZoneId zoneId = ZoneId.of("Europe/Paris");
		session.execute("CREATE INDEX IF NOT EXISTS allpossibletypes_zoneid ON allpossibletypes ( zoneid )");

		AllPossibleTypes allPossibleTypes = new AllPossibleTypes();

		allPossibleTypes.setId("id");
		allPossibleTypes.setZoneId(zoneId);

		allPossibleTypesRepository.save(allPossibleTypes);

		List<AllPossibleTypes> result = allPossibleTypesRepository.findWithZoneId(Optional.of(zoneId));

		assertThat(result).hasSize(1).contains(allPossibleTypes);
	}

	private interface AllPossibleTypesRepository extends CrudRepository<AllPossibleTypes, String> {

		@Query("select * from allpossibletypes where date = ?0")
		List<AllPossibleTypes> findWithCreatedDate(java.time.LocalDate createdDate);

		@Query("select * from allpossibletypes where zoneid = ?0")
		List<AllPossibleTypes> findWithZoneId(ZoneId zoneId);

		@Query("select * from allpossibletypes where date = ?0")
		List<AllPossibleTypes> findWithAnnotatedDateParameter(
				@CassandraType(type = CassandraType.Name.DATE) Date timestamp);

		@Query("select * from allpossibletypes where date = ?0")
		List<AllPossibleTypes> findWithDateParameter(Date timestamp);

		@Query("select * from allpossibletypes where zoneid = ?0")
		List<AllPossibleTypes> findWithZoneId(Optional<ZoneId> zoneId);
	}

	private static class DateToLocalDateConverter implements Converter<Date, LocalDate> {

		@Override
		public LocalDate convert(Date source) {

			LocalDate localDate = LocalDateTime.ofInstant(source.toInstant(), ZoneOffset.UTC.normalized()).toLocalDate();

			return LocalDate.of(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
		}
	}
}
