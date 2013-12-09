package org.springframework.cassandra.test.integration.config;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Session;

@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractIntegrationTest {

	@BeforeClass
	public static void startCassandra() throws ConfigurationException, TTransportException, IOException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("spring-cassandra.yaml");
	}

	@Inject
	public Session session;

	@Before
	public void assertSession() {
		IntegrationTestUtils.assertSession(session);
	}
}
