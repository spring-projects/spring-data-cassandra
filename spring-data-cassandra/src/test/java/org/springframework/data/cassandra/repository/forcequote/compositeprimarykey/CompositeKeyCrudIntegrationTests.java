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
package org.springframework.data.cassandra.repository.forcequote.compositeprimarykey;

import static org.assertj.core.api.Assertions.*;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.repository.forcequote.compositeprimarykey.entity.CorrelationEntity;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;

/**
 * @author Mark Paluch
 */
class CompositeKeyCrudIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraOperations operations;

	private CorrelationEntity correlationEntity1, correlationEntity2;

	@BeforeEach
	void setUp() {

		operations = new CassandraTemplate(session);

		SchemaTestUtils.potentiallyCreateTableFor(CorrelationEntity.class, operations);
		SchemaTestUtils.truncate(CorrelationEntity.class, operations);

		Map<String, String> map1 = new HashMap<>(2);
		map1.put("v", "1");
		map1.put("labels", "1,2,3");
		Map<String, String> map2 = new HashMap<>(2);
		map2.put("v", "1");
		map2.put("labels", "4,5,6");

		correlationEntity1 = new CorrelationEntity("a", "b", "c", new Date(1), "d", map1);
		correlationEntity2 = new CorrelationEntity("a", "b", "c", new Date(2), "e", map2);
	}

	@Test
	void test() {

		operations.insert(correlationEntity1);
		operations.insert(correlationEntity2);

		Select select = QueryBuilder.selectFrom("identity_correlations").all().where(
				Relation.column("type").isEqualTo(QueryBuilder.literal("a")),
				Relation.column("value").isEqualTo(QueryBuilder.literal("b")));

		List<CorrelationEntity> correlationEntities = operations.select(select.build(), CorrelationEntity.class);

		assertThat(correlationEntities).hasSize(2);

		QueryOptions queryOptions = QueryOptions.builder().consistencyLevel(DefaultConsistencyLevel.ONE).build();

		operations.delete(correlationEntity1, queryOptions);
		operations.delete(correlationEntity2, queryOptions);

		correlationEntities = operations.select(select.build(), CorrelationEntity.class);

		assertThat(correlationEntities).isEmpty();
	}
}
