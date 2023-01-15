/*
 * Copyright 2013-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.convert;

import lombok.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.UserDefinedConversionServiceIntegrationTests.Review.Rating;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for user defined conversion service bean
 *
 * @author neshkeev
 */
@SpringJUnitConfig
public class UserDefinedConversionServiceIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired
	private CassandraOperations ops;

	@BeforeEach
	void setUp() {
		SchemaTestUtils.potentiallyCreateTableFor(Review.class, ops);
		SchemaTestUtils.truncate(Review.class, ops);
	}

	@Test // GH-1343
	void shouldConvertEnumToTinyint() {
		Review review = new Review();
		String id = "custom-conversion-service";
		review.setId(id);
		review.setRating(Rating.FIVE);
		ops.insert(review);

		assertThat(ops.exists(id, Review.class))
				.withFailMessage("Freshly inserted row with the [" + id + "] id should exist")
				.isTrue();
	}

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[]{AllPossibleTypes.class.getPackage().getName()};
		}

		@Bean(ConfigurableApplicationContext.CONVERSION_SERVICE_BEAN_NAME)
		public DefaultConversionService conversionService() {
			DefaultConversionService conversionService = new DefaultConversionService();
			conversionService.addConverter(new Rating.RatingToByteConverter());
			return conversionService;
		}
	}

	@Data
	@Table(value = "reviews")
	public static class Review {
		@Id
		@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED)
		@CassandraType(type = CassandraType.Name.TEXT)
		private String id;

		@CassandraType(type = CassandraType.Name.TINYINT)
		private Rating rating;

		enum Rating {
			ONE(1), TWO(2), THREE(3), FOUR(4), FIVE(5);

			private final int value;

			Rating(int value) {
				this.value = value;
			}

			public byte getValue() {
				return (byte) value;
			}

			private static class RatingToByteConverter implements Converter<Rating, Byte> {

				@Override
				public Byte convert(Rating source) {
					return source.getValue();
				}
			}
		}
	}
}
