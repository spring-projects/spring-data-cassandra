package org.springframework.data.cassandra.test.integration.querymethods.intparam;

import static org.junit.Assert.assertNotNull;

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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class IntParamIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = ThingRepo.class)
	public static class Config extends IntegrationTestConfig {
		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Thing.class.getPackage().getName() };
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}
	}

	@Autowired
	ThingRepo repo;

	@Test
	public void testQueryWithIntPrimitiveAndReference() {
		int number = 42;
		Thing saved = new Thing(42);
		repo.save(saved);

		Thing found = repo.findThingByIntPrimitive(number);
		assertNotNull(found);

		found = repo.findThingByIntReference(new Integer(number));
		assertNotNull(found);
	}
}
