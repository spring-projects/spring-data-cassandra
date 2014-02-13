package org.springframework.data.cassandra.test.integration.multipackagescanning;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.config.CassandraEntityClassScanner;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;
import org.springframework.data.cassandra.test.integration.multipackagescanning.first.First;
import org.springframework.data.cassandra.test.integration.multipackagescanning.second.Second;
import org.springframework.data.cassandra.test.integration.multipackagescanning.third.Third;

public class MultipackageScanningIntegrationTests {

	DefaultCassandraMappingContext mapping;
	String pkg = getClass().getPackage().getName();

	@Before
	public void before() throws ClassNotFoundException {

		mapping = new DefaultCassandraMappingContext();
		mapping.setInitialEntitySet(CassandraEntityClassScanner.scan(pkg + ".first", pkg + ".second"));

		mapping.initialize();
	}

	@Test
	public void test() {

		Collection<CassandraPersistentEntity<?>> entities = mapping.getPersistentEntities();

		Collection<Class<?>> types = new HashSet<Class<?>>(entities.size());
		for (CassandraPersistentEntity<?> entity : entities) {
			types.add(entity.getType());
		}

		assertTrue(types.contains(First.class));
		assertTrue(types.contains(Second.class));
		assertFalse(types.contains(Third.class));
		assertFalse(types.contains(Top.class));
	}
}
