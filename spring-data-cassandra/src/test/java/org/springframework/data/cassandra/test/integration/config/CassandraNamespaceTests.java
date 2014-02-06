package org.springframework.data.cassandra.test.integration.config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.test.integration.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CassandraNamespaceTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void test() {
		Assert.notNull(ctx);
	}
}
