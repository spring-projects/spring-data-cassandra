package org.springframework.data.cassandra.test.integration.minimal.config;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.test.integration.minimal.config.entities.AbsMin;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class AbsoluteMinimumXmlConfigIntegrationTest extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired
	CassandraMappingContext context;

	@Test
	public void test() {

		assertNotNull(context);

		context.getPersistentEntity(AbsMin.class);
	}
}
