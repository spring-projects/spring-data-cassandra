/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.repository.querymethods.conversion;

import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.mapping.Table;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Mark Paluch
 */
@Table
@Data
@NoArgsConstructor
class Contact {

	@Id String id;

	Address address;
	List<Address> addresses;

	public Contact(String id) {
		this.id = id;
	}
}
