package org.springframework.data.cassandra.test.integration.config;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.SpringDataKeyspace;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

import com.datastax.driver.core.Cluster;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CassandraNamespaceTests {

	@Autowired
	private ApplicationContext ctx;

	@BeforeClass
	public static void startCassandra() throws IOException, TTransportException, ConfigurationException,
			InterruptedException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
	}

	@Test
	public void testSingleton() throws Exception {
		Object cluster = ctx.getBean("cassandra-cluster");
		Assert.notNull(cluster);
		Assert.isInstanceOf(Cluster.class, cluster);
		Object ks = ctx.getBean("cassandra-keyspace");
		Assert.notNull(ks);
		Assert.isInstanceOf(SpringDataKeyspace.class, ks);

		Cluster c = (Cluster) cluster;
	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}

}
