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
package org.springframework.data.cassandra.test.integration.composites;

import java.util.Date;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.mapping.Table;

/**
 * This is an example of the users timeline dynamic table, where all columns are dynamically created by @ColumnId field
 * value. The rest fields are places in Cassandra value. Timeline entity is used to store user's status updates that it
 * follows in the site. Timeline always ordered by @ColumnId field and we can retrieve last top status updates by using
 * limits.
 * 
 * @author Alex Shvid
 */
@Table("timeline")
public class Timeline {

	/*
	 * Row ID
	 */
	@Id
	private TimelinePK pk;

	/*
	 * Reference to the post by author and postUID
	 */
	private String author;
	private Date postTime;

	public TimelinePK getPk() {
		return pk;
	}

	public void setPk(TimelinePK pk) {
		this.pk = pk;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public Date getPostTime() {
		return postTime;
	}

	public void setPostTime(Date postTime) {
		this.postTime = postTime;
	}

}
