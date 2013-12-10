package org.springframework.cassandra.test.integration.config.java;

import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = Config.class)
public class ConfigTest extends AbstractIntegrationTest {

	@Test
	public void test() {
		session
				.execute("CREATE KEYSPACE ConfigTest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
		session.execute("USE ConfigTest");
	}
}
