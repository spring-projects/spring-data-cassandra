package org.springframework.data.cassandra.test.integration.mapping.mapid.repo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.springframework.data.cassandra.repository.support.BasicMapId.id;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RepositoryMapIdIntegrationTest extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = RepositoryMapIdIntegrationTest.class)
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { SinglePkc.class.getPackage().getName() };
		}
	}

	@Autowired
	CassandraOperations t;

	@Autowired
	SinglePkcRepository sr;

	@Autowired
	MultiPkcRepository mr;

	@Before
	public void before() {
		assertNotNull(t);
		assertNotNull(sr);
		assertNotNull(mr);
	}

	@Test
	public void testSinglePkc() {

		// insert
		SinglePkc inserted = new SinglePkc(uuid());
		inserted.setValue(uuid());
		SinglePkc saved = sr.save(inserted);
		assertSame(saved, inserted);

		// select
		MapId id = id("key", saved.getKey());
		SinglePkc selected = sr.findOne(id);
		assertNotSame(selected, saved);
		assertEquals(saved.getKey(), selected.getKey());
		assertEquals(saved.getValue(), selected.getValue());

		// update
		selected.setValue(uuid());
		SinglePkc updated = sr.save(selected);
		assertSame(updated, selected);

		selected = sr.findOne(id);
		assertNotSame(selected, updated);
		assertEquals(updated.getValue(), selected.getValue());

		// delete
		sr.delete(selected);
		assertNull(sr.findOne(id));
	}

	@Test
	public void testMultiPkc() {

		// insert
		MultiPkc inserted = new MultiPkc(uuid(), uuid());
		inserted.setValue(uuid());
		MultiPkc saved = mr.save(inserted);
		assertSame(saved, inserted);

		// select
		MapId id = id("key0", saved.getKey0()).with("key1", saved.getKey1());
		MultiPkc selected = mr.findOne(id);
		assertNotSame(selected, saved);
		assertEquals(saved.getKey0(), selected.getKey0());
		assertEquals(saved.getKey1(), selected.getKey1());
		assertEquals(saved.getValue(), selected.getValue());

		// update
		selected.setValue(uuid());
		MultiPkc updated = mr.save(selected);
		assertSame(updated, selected);

		selected = mr.findOne(id);
		assertNotSame(selected, updated);
		assertEquals(updated.getValue(), selected.getValue());

		// delete
		t.delete(selected);
		assertNull(mr.findOne(id));
	}

}
