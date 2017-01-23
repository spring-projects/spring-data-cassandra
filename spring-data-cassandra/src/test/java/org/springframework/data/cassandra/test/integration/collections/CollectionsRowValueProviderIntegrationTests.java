/*
 * Copyright 2011-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.collections;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.test.integration.simpletons.Book;
import org.springframework.data.cassandra.test.integration.simpletons.BookHistory;
import org.springframework.data.cassandra.test.integration.simpletons.BookReference;
import org.springframework.data.cassandra.test.integration.support.SchemaTestUtils;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * @author dwebb
 * @author Mark Paluch
 */
public class CollectionsRowValueProviderIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraOperations operations;

	@Before
	public void before() throws IOException {

		operations = new CassandraTemplate(session);

		SchemaTestUtils.potentiallyCreateTableFor(Book.class, operations);
		SchemaTestUtils.potentiallyCreateTableFor(BookHistory.class, operations);
		SchemaTestUtils.potentiallyCreateTableFor(BookReference.class, operations);

		SchemaTestUtils.truncate(Book.class, operations);
		SchemaTestUtils.truncate(BookHistory.class, operations);
		SchemaTestUtils.truncate(BookReference.class, operations);
	}

	@Test
	public void mapTest() {

		BookHistory b1 = new BookHistory();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);
		b1.setSaleDate(new Date());
		b1.setInStock(true);

		Map<String, Integer> checkOutMap = new HashMap<>();
		checkOutMap.put("dwebb", 50);
		checkOutMap.put("madams", 100);
		checkOutMap.put("jmcpeek", 150);

		b1.setCheckOuts(checkOutMap);

		operations.insert(b1);

		Select select = QueryBuilder.select().all().from("bookHistory");
		select.where(QueryBuilder.eq("isbn", "123456-1"));

		BookHistory result = operations.selectOne(select, BookHistory.class);

		assertThat(result.getCheckOuts()).isNotNull();
		assertThat(result.getTitle()).isEqualTo("Spring Data Cassandra Guide");
		assertThat(result.getAuthor()).isEqualTo("Cassandra Guru");
	}

	@Test
	public void listSetTest() {

		BookReference b1 = new BookReference();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);
		b1.setSaleDate(new Date());
		b1.setInStock(true);

		Set<String> refs = new HashSet<>();
		refs.add("Spring Data by O'Reilly");
		refs.add("Spring by Example");
		refs.add("Spring Recipies");
		b1.setReferences(refs);

		List<Integer> marks = new LinkedList<>();
		marks.add(13);
		marks.add(52);
		marks.add(144);
		b1.setBookmarks(marks);

		operations.insert(b1);

		Select select = QueryBuilder.select().all().from("bookReference");
		select.where(QueryBuilder.eq("isbn", "123456-1"));

		BookReference result = operations.selectOne(select, BookReference.class);

		assertThat(result.getReferences()).isNotNull();
		assertThat(result.getBookmarks()).isNotNull();
		assertThat(result.getTitle()).isEqualTo("Spring Data Cassandra Guide");
		assertThat(result.getAuthor()).isEqualTo("Cassandra Guru");

	}
}
