package org.springframework.data.cassandra.test.integration.mappingcontext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;

public class MappingContextIntegrationTests {

	public static class Transient {
	}

	@Test
	// TODO: (expected = MappingException.class)
	public void testGetPersistentEntityOfTransientType() {

		// TODO: when entity verification is added (DATACASS-85), this should throw a MappingException

		DefaultCassandraMappingContext ctx = new DefaultCassandraMappingContext();
		CassandraPersistentEntity<?> entity = ctx.getPersistentEntity(Transient.class);

		// TODO: remove following lines after DATACASS-85
		assertNotNull(entity);
		assertEquals(Transient.class.getSimpleName().toLowerCase(), entity.getTableName());
	}
}
