package org.springframework.data.cassandra.test.integration.forcequote.config;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.springframework.data.cassandra.core.CassandraTemplate;

public class ForceQuotedRepositoryIntegrationTests {

	ImplicitRepository implicits;
	ExplicitRepository explicits;
	CassandraTemplate template;

	public ForceQuotedRepositoryIntegrationTests() {
	}

	public ForceQuotedRepositoryIntegrationTests(ImplicitRepository implicits, ExplicitRepository explicits,
			CassandraTemplate template) {
		this.implicits = implicits;
		this.explicits = explicits;
		this.template = template;
	}

	public void before() {
		template.deleteAll(Implicit.class);
	}

	public void testImplicit() {
		Implicit entity = new Implicit();
		String key = entity.getKey();

		Implicit si = implicits.save(entity);
		assertSame(si, entity);

		Implicit fi = implicits.findOne(key);
		assertNotSame(fi, entity);

		implicits.delete(key);

		assertNull(implicits.findOne(key));
	}

	public void testExplicit() {
		Explicit entity = new Explicit();
		String key = entity.getKey();

		Explicit si = explicits.save(entity);
		assertSame(si, entity);

		Explicit fi = explicits.findOne(key);
		assertNotSame(fi, entity);

		explicits.delete(key);

		assertNull(explicits.findOne(key));
	}
}
