package org.springframework.data.cassandra.config;

import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class DriverTests {

	@Test
	public void test() throws Exception {
		
		Cluster.Builder builder = Cluster.builder().addContactPoint("127.0.0.1");

		//builder.withCompression(ProtocolOptions.Compression.SNAPPY);
		
		Cluster cluster = builder.build();
		
		Session session = cluster.connect();
		
		session.shutdown();
		
		cluster.shutdown();
		
		
	}
	
}
