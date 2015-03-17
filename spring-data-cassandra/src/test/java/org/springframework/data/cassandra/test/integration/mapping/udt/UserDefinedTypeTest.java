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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableList;

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
	public static class Config extends IntegrationTestConfig {
		
		private final static String ks = UserDefinedTypeTest.class.getSimpleName();
		
		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.NONE;
		}
		
		@Override
		protected List<String> getStartupScripts() {
			return ImmutableList.of(
					String.format("use %s", ks),
					"drop table if exists company",
					"drop type if exists address",
					"drop type if exists phone",
					"create type phone(number text)",
					"create type address(street text, number int, active boolean, pbx frozen<phone>)",
					"create table company(id text primary key, addr frozen<address>)"
					);
		}
		
		@Override
		protected String getKeyspaceName() {
			return ks;
		}
		
		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Company.class.getPackage().getName() };
		}
	}

	@Test
	public void testSave() throws Exception {
		String id = "123";
		String streetName = "Street Name";
		Integer number = 10;
		Boolean active = true;
		String phoneNumber = "+1 800 555 0000";

		Company companyToSave = new Company();
		companyToSave.id = id;
		companyToSave.addr.street = streetName;
		companyToSave.addr.number = number;
		companyToSave.addr.active = active;
		companyToSave.addr.pbx.number = phoneNumber;
		companyRepository.save(companyToSave);

		Company retrievedCompany = companyRepository.findOne(id);
		assertThat(retrievedCompany.id, is(id));
		assertThat(retrievedCompany.addr, notNullValue());
		assertThat(retrievedCompany.addr.street, is(streetName));
		assertThat(retrievedCompany.addr.number, is(number));
		assertThat(retrievedCompany.addr.active, is(active));
		assertThat(retrievedCompany.addr.pbx.number, is(phoneNumber));
	}
}
