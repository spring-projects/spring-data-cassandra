package org.springframework.data.cassandra.test.integration.querymethods.bigintparam;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.math.BigInteger;

import static org.junit.Assert.assertNotNull;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class BigIntParamIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = BigThingRepo.class)
	public static class Config extends IntegrationTestConfig {
		@Override
		public String[] getEntityBasePackages() {
			return new String[] { BigThing.class.getPackage().getName() };
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}
	}

	@Autowired
	BigThingRepo repo;

	@Test
	public void testQueryWithReference() {
		BigInteger number = new BigInteger("42");
		BigThing saved = new BigThing(number);
		repo.save(saved);

		BigThing found = repo.findThingByBigInteger(new BigInteger("42"));
		assertNotNull(found);
	}
}
