package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import static org.junit.Assert.assertEquals;

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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CompositeKeyCrudTest extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = ImplicitRepository.class)
	public static class Config extends IntegrationTestConfig {}

	@Autowired
	CassandraTemplate t;

	CorrelationEntity c1, c2;

	@Before
	public void setUp() throws Throwable {

		Map<String, String> map1 = new HashMap<String, String>(2);
		map1.put("v", "1");
		map1.put("labels", "1,2,3");
		Map<String, String> map2 = new HashMap<String, String>(2);
		map2.put("v", "1");
		map2.put("labels", "4,5,6");

		c1 = new CorrelationEntity("a", "b", "c", new Date(1), "d", map1);
		c2 = new CorrelationEntity("a", "b", "c", new Date(2), "e", map2);
	}

	@Test
	public void test() {
		t.insert(c1);
		t.insert(c2);

		Select select = QueryBuilder.select().from("identity_correlations");
		select.where(QueryBuilder.eq("type", "a")).and(QueryBuilder.eq("value", "b"));
		select.setRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
		select.setConsistencyLevel(ConsistencyLevel.ONE);
		List<CorrelationEntity> correlationEntities = t.select(select, CorrelationEntity.class);

		assertEquals(2, correlationEntities.size());

		QueryOptions qo = new QueryOptions();
		qo.setConsistencyLevel(org.springframework.cassandra.core.ConsistencyLevel.ONE);
		ArrayList<CorrelationEntity> entities = new ArrayList<CorrelationEntity>();
		entities.add(c1);
		entities.add(c2);
		t.delete(entities, qo);

		correlationEntities = t.select(select, CorrelationEntity.class);

		assertEquals(0, correlationEntities.size());
	}

}
