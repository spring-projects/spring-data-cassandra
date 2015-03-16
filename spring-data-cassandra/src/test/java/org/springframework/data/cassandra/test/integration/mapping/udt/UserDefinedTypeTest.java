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
package org.springframework.data.cassandra.test.integration.mapping.udt;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Fabio Mendes <fabiojmendes@gmail.com> [Mar 14, 2015]
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class UserDefinedTypeTest {

	@Autowired
	CompanyRepository companyRepository;

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = CompanyRepository.class)
	public static class Config extends AbstractCassandraConfiguration {

		@Override
		protected String getKeyspaceName() {
			return UserDefinedTypeTest.class.getSimpleName();
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.NONE;
		}
	}

	@Test
	public void testSave() throws Exception {
		String id = "123";
		String streetName = "Street Name";

		Company companyToSave = new Company();
		companyToSave.id = id;
		companyToSave.address.street = streetName;
		companyToSave.address.number = 10;
		companyRepository.save(companyToSave);

		Company retrievedCompany = companyRepository.findOne(id);
		assertThat(retrievedCompany.id, is(id));
		assertThat(retrievedCompany.address, notNullValue());
		assertThat(retrievedCompany.address.street, is(streetName));
	}
}
