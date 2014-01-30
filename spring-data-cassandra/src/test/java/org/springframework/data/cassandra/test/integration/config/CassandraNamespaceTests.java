package org.springframework.data.cassandra.test.integration.config;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CassandraNamespaceTests {

	@Autowired
	private ApplicationContext ctx;

	@BeforeClass
	public static void startCassandra() throws IOException, TTransportException, ConfigurationException,
			InterruptedException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("spring-cassandra.yaml");
	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	@Test
	public void test() {
		Assert.notNull(ctx);
	}
}
