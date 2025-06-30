/*
 * Copyright 2016-2025 the original author or authors.
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

import static org.springframework.data.cassandra.core.mapping.CassandraType.*;

import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.util.ObjectUtils;

/**
 * @author Mark Paluch
 */
@Table
public class Contact {

	@Id String id;

	Address address;
	List<Address> addresses;

	@CassandraType(type = Name.UDT, userTypeName = "phone") Phone mainPhone;

	@CassandraType(type = Name.LIST, typeArguments = Name.UDT,
			userTypeName = "phone") List<Phone> alternativePhones;

	public Contact(String id) {
		this.id = id;
	}

	public Contact() {}

	public String getId() {
		return this.id;
	}

	public Address getAddress() {
		return this.address;
	}

	public List<Address> getAddresses() {
		return this.addresses;
	}

	public Phone getMainPhone() {
		return this.mainPhone;
	}

	public List<Phone> getAlternativePhones() {
		return this.alternativePhones;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

	public void setAddresses(List<Address> addresses) {
		this.addresses = addresses;
	}

	public void setMainPhone(Phone mainPhone) {
		this.mainPhone = mainPhone;
	}

	public void setAlternativePhones(List<Phone> alternativePhones) {
		this.alternativePhones = alternativePhones;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		Contact contact = (Contact) o;

		if (!ObjectUtils.nullSafeEquals(id, contact.id)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(address, contact.address)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(addresses, contact.addresses)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(mainPhone, contact.mainPhone)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(alternativePhones, contact.alternativePhones);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(id);
		result = 31 * result + ObjectUtils.nullSafeHashCode(address);
		result = 31 * result + ObjectUtils.nullSafeHashCode(addresses);
		result = 31 * result + ObjectUtils.nullSafeHashCode(mainPhone);
		result = 31 * result + ObjectUtils.nullSafeHashCode(alternativePhones);
		return result;
	}

}
