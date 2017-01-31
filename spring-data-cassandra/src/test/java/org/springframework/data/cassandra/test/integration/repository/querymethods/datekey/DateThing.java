/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.repository.querymethods.datekey;

import java.util.Date;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.util.Assert;

/**
 * @author Matthew T. Adams
 */
@Table
public class DateThing {

	@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) private Date date;

	protected DateThing() {}

	public DateThing(Date date) {
		setDate(date);
	}

	public Date getDate() {
		return new Date(date.getTime());
	}

	public void setDate(Date date) {

		Assert.notNull(date, "Date must not be null");
		this.date = new Date(date.getTime());
	}
}
