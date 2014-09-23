package org.springframework.data.cassandra.test.integration.mapping.mapid.template;

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
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CassandraTemplateMapIdIntegrationTest extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { SinglePkc.class.getPackage().getName() };
		}
	}

	@Autowired
	CassandraOperations t;

	@Before
	public void before() {
		assertNotNull(t);
	}

	@Test
	public void testSinglePkc() {

		// insert
		SinglePkc inserted = new SinglePkc(uuid());
		inserted.setValue(uuid());
		SinglePkc saved = t.insert(inserted);
		assertSame(saved, inserted);

		// select
		MapId id = id("key", saved.getKey());
		SinglePkc selected = t.selectOneById(SinglePkc.class, id);
		assertNotSame(selected, saved);
		assertEquals(saved.getKey(), selected.getKey());
		assertEquals(saved.getValue(), selected.getValue());

		// update
		selected.setValue(uuid());
		SinglePkc updated = t.update(selected);
		assertSame(updated, selected);

		selected = t.selectOneById(SinglePkc.class, id);
		assertNotSame(selected, updated);
		assertEquals(updated.getValue(), selected.getValue());

		// delete
		t.delete(selected);
		assertNull(t.selectOneById(SinglePkc.class, id));
	}

	@Table
	public static class SinglePkc {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
		String key;

		@Column
		String value;

		/**
		 * @deprecated for persistence use only
		 */
		@Deprecated
		@SuppressWarnings("unused")
		private SinglePkc() {}

		public SinglePkc(String key) {
			setKey(key);
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	@Test
	public void testMultiPkc() {

		// insert
		MultiPkc inserted = new MultiPkc(uuid(), uuid());
		inserted.setValue(uuid());
		MultiPkc saved = t.insert(inserted);
		assertSame(saved, inserted);

		// select
		MapId id = id("key0", saved.getKey0()).with("key1", saved.getKey1());
		MultiPkc selected = t.selectOneById(MultiPkc.class, id);
		assertNotSame(selected, saved);
		assertEquals(saved.getKey0(), selected.getKey0());
		assertEquals(saved.getKey1(), selected.getKey1());
		assertEquals(saved.getValue(), selected.getValue());

		// update
		selected.setValue(uuid());
		MultiPkc updated = t.update(selected);
		assertSame(updated, selected);

		selected = t.selectOneById(MultiPkc.class, id);
		assertNotSame(selected, updated);
		assertEquals(updated.getValue(), selected.getValue());

		// delete
		t.delete(selected);
		assertNull(t.selectOneById(MultiPkc.class, id));
	}

	@Table
	public static class MultiPkc {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
		String key0;

		@PrimaryKeyColumn(ordinal = 1)
		String key1;

		@Column
		String value;

		/**
		 * @deprecated for persistence use only
		 */
		@Deprecated
		@SuppressWarnings("unused")
		private MultiPkc() {}

		public MultiPkc(String key0, String key1) {
			setKey0(key0);
			setKey1(key1);
		}

		public String getKey0() {
			return key0;
		}

		public void setKey0(String key0) {
			this.key0 = key0;
		}

		public String getKey1() {
			return key1;
		}

		public void setKey1(String key1) {
			this.key1 = key1;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}
}
