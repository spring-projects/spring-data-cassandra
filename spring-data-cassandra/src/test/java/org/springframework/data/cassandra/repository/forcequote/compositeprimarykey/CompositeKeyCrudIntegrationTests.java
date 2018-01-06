/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra.repository.forcequote.compositeprimarykey;

import static org.assertj.core.api.Assertions.*;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.repository.forcequote.compositeprimarykey.entity.CorrelationEntity;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * @author Mark Paluch
 */
public class CompositeKeyCrudIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraOperations operations;

	private CorrelationEntity correlationEntity1, correlationEntity2;

	@Before
	public void setUp() throws Throwable {

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
	public void test() {

		operations.insert(correlationEntity1);
		operations.insert(correlationEntity2);

		Select select = QueryBuilder.select().from("identity_correlations");
		select.where(QueryBuilder.eq("type", "a")).and(QueryBuilder.eq("value", "b"));
		select.setRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
		select.setConsistencyLevel(ConsistencyLevel.ONE);
		List<CorrelationEntity> correlationEntities = operations.select(select, CorrelationEntity.class);

		assertThat(correlationEntities).hasSize(2);

		QueryOptions queryOptions = QueryOptions.builder().consistencyLevel(ConsistencyLevel.ONE).build();

		operations.delete(correlationEntity1, queryOptions);
		operations.delete(correlationEntity2, queryOptions);

		correlationEntities = operations.select(select, CorrelationEntity.class);

		assertThat(correlationEntities).isEmpty();
	}
}
