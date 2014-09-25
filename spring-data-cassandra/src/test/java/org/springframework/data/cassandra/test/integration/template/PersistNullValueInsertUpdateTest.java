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
package org.springframework.data.cassandra.test.integration.template;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.test.integration.simpletons.Book;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;

/**
 * Integration Tests for including columns with null values in the Insert & Update statements
 *
 * @author Praveena Subrahmanyam
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class PersistNullValueInsertUpdateTest extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

    @Configuration
    public static class Config extends IntegrationTestConfig {

        @Override
        public String[] getEntityBasePackages() {
            return new String[]{Book.class.getPackage().getName()};
        }

        @Override
        public CassandraConverter cassandraConverter() throws Exception {
            return new MappingCassandraConverter(cassandraMapping(), true);
        }
    }

    @Before
    public void before() {
        deleteAllEntities();
    }

    @Test
    public void insertNullTest() {

        Book b1 = new Book();
        b1.setIsbn("123456-1");
        b1.setTitle("Spring Data Cassandra Guide");
        b1.setAuthor("Cassandra Guru");
        b1.setPages(521);
        b1.setSaleDate(null);
        b1.setInStock(true);

        template.insert(b1);

        Select select = QueryBuilder.select().all().from("book");
        select.where(QueryBuilder.eq("isbn", "123456-1"));

        Book insertedBook = null;
        insertedBook = template.selectOne(select, Book.class);

        Assert.assertNull(insertedBook.getSaleDate());

        //Update sale date to now and insert again
        Date saleDate = new Date();
        b1.setSaleDate(saleDate);

        template.insert(b1);

        insertedBook = template.selectOne(select, Book.class);
        Assert.assertEquals(saleDate, insertedBook.getSaleDate());

        //Update sale date to null and insert again
        b1.setSaleDate(null);

        template.insert(b1);

        insertedBook = template.selectOne(select, Book.class);
        Assert.assertNull(insertedBook.getSaleDate());
    }

    @Test
    public void updateTest() {

        Book b1 = new Book();
        b1.setIsbn("123456-1");
        b1.setTitle("Spring Data Cassandra Guide");
        b1.setAuthor("Cassandra Guru");
        b1.setPages(521);
        b1.setSaleDate(new Date());
        b1.setInStock(true);

        template.insert(b1);


        b1 = new Book();
        b1.setIsbn("123456-1");
        b1.setTitle("Spring Data Cassandra Book");
        b1.setAuthor(null);
        b1.setPages(521);

        template.update(b1);

        //check that author is null
        Select selectB1 = QueryBuilder.select().all().from("book");
        selectB1.where(QueryBuilder.eq("isbn", "123456-1"));
        Book updatedB1 = template.selectOne(selectB1, Book.class);
        Assert.assertNull(updatedB1.getAuthor());
    }

}
