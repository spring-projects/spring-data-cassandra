/*
 * Copyright 2016-present the original author or authors.
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

import org.springframework.util.ObjectUtils;

/**
 * @author Mark Paluch
 */
class Address {

	String city;
	String country;

	public Address(String city, String country) {
		this.city = city;
		this.country = country;
	}

	public Address() {}

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

		Address address = (Address) o;

		if (!ObjectUtils.nullSafeEquals(city, address.city)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(country, address.country);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(city);
		result = 31 * result + ObjectUtils.nullSafeHashCode(country);
		return result;
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer();
		sb.append(getClass().getSimpleName());
		sb.append(" [city='").append(city).append('\'');
		sb.append(", country='").append(country).append('\'');
		sb.append(']');
		return sb.toString();
	}
}
