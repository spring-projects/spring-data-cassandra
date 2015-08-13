package org.springframework.cassandra.test.unit.config;

import com.datastax.driver.core.ProtocolVersion;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.cassandra.config.CassandraCqlClusterFactoryBean;

/**
 * JUnit test case for CassandraCqlClusterFactoryBean
 *
 * @author Kirk Clemens
 */
public class CassandraCqlClusterFactoryBeanTest{

	@Test
	public void testProtocolVersion(){
		final CassandraCqlClusterFactoryBean cassandraCqlClusterFactoryBean = new CassandraCqlClusterFactoryBean();
		cassandraCqlClusterFactoryBean.setProtocolVersion(ProtocolVersion.V2);
		try {
			cassandraCqlClusterFactoryBean.afterPropertiesSet();
			Assert.assertEquals(
				ProtocolVersion.V2,
				cassandraCqlClusterFactoryBean.getObject().getConfiguration().getProtocolOptions().getProtocolVersionEnum()
			);
		} catch (Exception e) {
			Assert.fail("Unable to create CQL cluster bean" + e.getMessage());
		}
	}

	@Test
	public void testDefaultProtocolVersion(){
		final CassandraCqlClusterFactoryBean cassandraCqlClusterFactoryBean = new CassandraCqlClusterFactoryBean();

		try {
			cassandraCqlClusterFactoryBean.afterPropertiesSet();
			Assert.assertNull(
				cassandraCqlClusterFactoryBean.getObject().getConfiguration().getProtocolOptions().getProtocolVersionEnum()
			);
		} catch (Exception e) {
			Assert.fail("Unable to create CQL cluster bean" + e.getMessage());
		}
	}
}
