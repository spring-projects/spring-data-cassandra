/*
 * Copyright 2017-present the original author or authors.
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
package org.springframework.data.cassandra.domain;

import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.util.ObjectUtils;

/**
 * @author Mark Paluch
 */
@UserDefinedType("address")
public class AddressType {

	String city;
	String country;

	public AddressType(String city, String country) {
		this.city = city;
		this.country = country;
	}

	public AddressType() {}

	public String getCity() {
		return this.city;
	}

	public String getCountry() {
		return this.country;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		AddressType that = (AddressType) o;

		if (!ObjectUtils.nullSafeEquals(city, that.city)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(country, that.country);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(city);
		result = 31 * result + ObjectUtils.nullSafeHashCode(country);
		return result;
	}
}
