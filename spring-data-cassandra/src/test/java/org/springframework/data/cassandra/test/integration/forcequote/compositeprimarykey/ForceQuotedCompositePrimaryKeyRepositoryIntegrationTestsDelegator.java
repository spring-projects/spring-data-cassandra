package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
public abstract class ForceQuotedCompositePrimaryKeyRepositoryIntegrationTestsDelegator extends
		AbstractSpringDataEmbeddedCassandraIntegrationTest {

	ForceQuotedCompositePrimaryKeyRepositoryIntegrationTests tests = new ForceQuotedCompositePrimaryKeyRepositoryIntegrationTests();

	@Autowired
	ImplicitRepository i;

	@Autowired
	ExplicitRepository e;

	@Autowired
	CassandraTemplate t;

	@Before
	public void before() {

		tests.i = i;
		tests.e = e;
		tests.t = t;

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
