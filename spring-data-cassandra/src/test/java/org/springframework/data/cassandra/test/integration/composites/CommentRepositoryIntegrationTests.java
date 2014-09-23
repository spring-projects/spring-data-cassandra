/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.composites;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import org.springframework.data.cassandra.core.CassandraOperations;

/**
 * Tests for {@link CommentRepository}.
 * 
 * @author Matthew T. Adams
 */
public class CommentRepositoryIntegrationTests {

	CommentRepository repository;
	CassandraOperations template;

	public CommentRepositoryIntegrationTests() {}

	public CommentRepositoryIntegrationTests(CommentRepository repository, CassandraOperations template) {
		this.repository = repository;
		this.template = template;
	}

	public void before() {
		repository.deleteAll();
	}

	public void testInsert() {

		String author = "testAuthorInsert";
		String company = "testCompanyInsert";

		Comment c = new Comment(author, company);
		c.setText("testTextInsert");

		CommentKey key = c.getId();

		repository.save(c);

		Comment retrieved = repository.findOne(key);

		assertNotSame(c, retrieved);
		assertEquals(c, retrieved);
		assertEquals(c.getText(), retrieved.getText());
	}

	public void testUpdateNonKeyField() {

		String author = "testAuthorUpdate";
		String company = "testCompanyUpdate";

		Comment c = new Comment(author, company);
		c.setText("testTextUpdate");

		CommentKey key = c.getId();

		repository.save(c);

		Comment retrieved = repository.findOne(key);

		assertNotSame(c, retrieved);
		assertEquals(c, retrieved);
		assertEquals(c.getText(), retrieved.getText());

		String newText = "x" + retrieved.getText();
		retrieved.setText(newText);

		repository.save(retrieved);

		Comment updated = repository.findOne(key);

		assertNotSame(retrieved, updated);
		assertEquals(newText, updated.getText());
	}

	public void testDelete() {

		String author = "testAuthorDelete";
		String company = "testCompanyDelete";

		Comment c = new Comment(author, company);
		c.setText("testTextDelete");

		CommentKey key = c.getId();

		repository.save(c);

		Comment retrieved = repository.findOne(key);

		assertNotSame(c, retrieved);
		assertEquals(c, retrieved);
		assertEquals(c.getText(), retrieved.getText());

		repository.delete(retrieved);

		assertNull(repository.findOne(key));
	}
}
