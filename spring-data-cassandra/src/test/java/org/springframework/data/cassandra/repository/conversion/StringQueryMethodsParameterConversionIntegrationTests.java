/*
 * Copyright 2016-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.conversion;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.repository.MapIdCassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;

/**
 * Integration tests for query argument conversion through {@link ContactStringQueryRepository}.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig(classes = ParameterConversionTestSupport.Config.class)
class StringQueryMethodsParameterConversionIntegrationTests extends ParameterConversionTestSupport {

	@Autowired ContactStringQueryRepository contactRepository;

	@Test // DATACASS-172
	void shouldFindByConvertedParameter() {

		List<Contact> contacts = contactRepository.findByAddress(walter.getAddress());

		assertThat(contacts).contains(walter, flynn);
	}

	@Test // DATACASS-172
	void shouldFindByStringParameter() {

		String parameter = AddressWriteConverter.INSTANCE.convert(walter.getAddress());
		List<Contact> contacts = contactRepository.findByAddress(parameter);

		assertThat(contacts).contains(walter, flynn);
	}

	@Test // DATACASS-172
	void findByAddressesIn() {

		assertThat(contactRepository.findByAddressesContains(flynn.address)).contains(flynn, walter);
		assertThat(contactRepository.findByAddressesContains(walter.addresses.get(1))).contains(walter);
	}

	@Test // DATACASS-172
	void findByMainPhone() {
		assertThat(contactRepository.findByMainPhone(walter.getMainPhone())).contains(walter);
	}

	@Test // DATACASS-172
	void findByMainPhoneUdtValue() {

		KeyspaceMetadata keyspace = adminOperations.getKeyspaceMetadata();
		UdtValue udtValue = keyspace.getUserDefinedType("phone").get().newValue();
		udtValue.setString("number", walter.getMainPhone().getNumber());

		assertThat(contactRepository.findByMainPhone(udtValue)).contains(walter);
	}

	@Test // DATACASS-172
	void findByAlternativePhones() {

		Phone phone = walter.getAlternativePhones().get(0);
		assertThat(contactRepository.findByAlternativePhonesContains(phone)).contains(walter);
	}

	@Test // DATACASS-172
	void findByAlternativePhonesUdtValue() {

		Phone phone = walter.getAlternativePhones().get(0);

		KeyspaceMetadata keyspace = adminOperations.getKeyspaceMetadata();
		UdtValue udtValue = keyspace.getUserDefinedType("phone").get().newValue();
		udtValue.setString("number", phone.getNumber());

		assertThat(contactRepository.findByAlternativePhonesContains(udtValue)).contains(walter);
	}

	interface ContactStringQueryRepository extends MapIdCassandraRepository<Contact> {

		@Query("SELECT * from contact where address = ?0;")
		List<Contact> findByAddress(Address address);

		@Query("SELECT * from contact where address = ?0;")
		List<Contact> findByAddress(String address);

		@Query("SELECT * from contact where addresses contains ?0;")
		List<Contact> findByAddressesContains(Address address);

		@Query("SELECT * from contact where mainphone = ?0;")
		List<Contact> findByMainPhone(Phone phone);

		@Query("SELECT * from contact where mainphone = ?0;")
		List<Contact> findByMainPhone(UdtValue udtValue);

		@Query("SELECT * from contact where alternativephones contains ?0;")
		List<Contact> findByAlternativePhonesContains(Phone phone);

		@Query("SELECT * from contact where alternativephones contains ?0;")
		List<Contact> findByAlternativePhonesContains(UdtValue udtValue);
	}
}
