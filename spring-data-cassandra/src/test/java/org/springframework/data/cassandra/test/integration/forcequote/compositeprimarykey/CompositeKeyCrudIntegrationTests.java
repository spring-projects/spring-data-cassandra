/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey.entity.CorrelationEntity;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CompositeKeyCrudIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = ImplicitRepository.class)
	public static class Config extends IntegrationTestConfig {}

	@Autowired private CassandraTemplate template1;

	private CorrelationEntity correlationEntity1, correlationEntity2;

	@Before
	public void setUp() throws Throwable {

		Map<String, String> map1 = new HashMap<String, String>(2);
		map1.put("v", "1");
		map1.put("labels", "1,2,3");
		Map<String, String> map2 = new HashMap<String, String>(2);
		map2.put("v", "1");
		map2.put("labels", "4,5,6");

		correlationEntity1 = new CorrelationEntity("a", "b", "c", new Date(1), "d", map1);
		correlationEntity2 = new CorrelationEntity("a", "b", "c", new Date(2), "e", map2);
	}

	@Test
	public void test() {
		template1.insert(correlationEntity1);
		template1.insert(correlationEntity2);

		Select select = QueryBuilder.select().from("identity_correlations");
		select.where(QueryBuilder.eq("type", "a")).and(QueryBuilder.eq("value", "b"));
		select.setRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
		select.setConsistencyLevel(ConsistencyLevel.ONE);
		List<CorrelationEntity> correlationEntities = template1.select(select, CorrelationEntity.class);

		assertEquals(2, correlationEntities.size());

		QueryOptions qo = new QueryOptions();
		qo.setConsistencyLevel(org.springframework.cassandra.core.ConsistencyLevel.ONE);
		ArrayList<CorrelationEntity> entities = new ArrayList<CorrelationEntity>();
		entities.add(correlationEntity1);
		entities.add(correlationEntity2);
		template1.delete(entities, qo);

		correlationEntities = template1.select(select, CorrelationEntity.class);

		assertEquals(0, correlationEntities.size());
	}

}
