package org.springframework.data.cassandra.test.integration.forcequote.config;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
public abstract class ForceQuotedRepositoryIntegrationTestsDelegator extends
		AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired
	ImplicitRepository i;

	@Autowired
	ImplicitPropertiesRepository ip;

	@Autowired
	ExplicitRepository e;

	@Autowired
	ExplicitPropertiesRepository ep;

	@Autowired
	CassandraTemplate t;

	ForceQuotedRepositoryIntegrationTests tests;

	@Before
	public void before() {
		tests = new ForceQuotedRepositoryIntegrationTests();
		tests.i = i;
		tests.ip = ip;
		tests.e = e;
		tests.ep = ep;
		tests.t = t;

		tests.before();
	}

	@Test
	public void testImplicit() {
		tests.testImplicit();
	}

	/**
	 * Not a @Test -- used by subclasses!
	 */
	public void testExplicit(String tableName) {
		tests.testExplicit(tableName);
	}

	@Test
	public void testImplicitProperties() {
		tests.testImplicitProperties();
	}

	/**
	 * Not a @Test -- used by subclasses!
	 * 
	 * @see ForceQuotedRepositoryJavaConfigIntegrationTests#testExplicitPropertiesWithJavaValues()
	 * @see ForceQuotedRepositoryXmlConfigIntegrationTests#testExplicitPropertiesWithXmlValues()
	 */
	public void testExplicitProperties(String stringValueColumnName, String primaryKeyColumnName) {
		tests.testExplicitProperties(stringValueColumnName, primaryKeyColumnName);
	}
}
