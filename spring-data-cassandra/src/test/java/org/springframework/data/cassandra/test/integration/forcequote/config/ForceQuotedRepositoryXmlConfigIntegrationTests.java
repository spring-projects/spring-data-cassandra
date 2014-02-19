package org.springframework.data.cassandra.test.integration.forcequote.config;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ForceQuotedRepositoryXmlConfigIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired
	ImplicitRepository implicits;

	@Autowired
	ExplicitRepository explicits;

	@Autowired
	CassandraTemplate template;

	ForceQuotedRepositoryIntegrationTests tests;

	@Before
	public void before() {
		tests = new ForceQuotedRepositoryIntegrationTests(implicits, explicits, template);
		tests.before();
	}

	@Test
	public void testImplicit() {
		tests.testImplicit();
	}

	@Test
	public void testExplicit() {
		tests.testExplicit();
	}
}
