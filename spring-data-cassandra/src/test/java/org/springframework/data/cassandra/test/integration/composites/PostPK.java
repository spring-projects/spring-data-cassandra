/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.composites;

import java.util.Date;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;

/**
 * This is an example of dynamic table that creates each time new column with Post timestamp.
 * 
 * It is possible to use a static table for posts and identify them by PostId(UUID), but in this case we need to use
 * MapReduce for Big Data to find posts for particular user, so it is better to have index (userId) -> index (post time)
 * architecture. It helps a lot to build eventually a search index for the particular user.
 * 
 * @author Alex Shvid
 */

@PrimaryKeyClass
public class PostPK {

	@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private String author;

	@PrimaryKeyColumn(ordinal = 1)
	private Date time;

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

}
