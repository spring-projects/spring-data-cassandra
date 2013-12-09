package org.springframework.cassandra.test.integration.config;

import static org.junit.Assert.*;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Session;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
public class ConfigTest {

	@BeforeClass
	public static void startCassandra() throws ConfigurationException, TTransportException, IOException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
	}

	@Inject
	Session session;

	@Test
	public void test() {
		assertNotNull(session);

		session
				.execute("CREATE KEYSPACE testy WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
		session.execute("USE testy");
	}
}
