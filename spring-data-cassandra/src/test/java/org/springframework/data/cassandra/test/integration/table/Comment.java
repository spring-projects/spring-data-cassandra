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
package org.springframework.data.cassandra.test.integration.table;

import java.util.Date;
import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.mapping.ColumnId;
import org.springframework.data.cassandra.mapping.Qualify;
import org.springframework.data.cassandra.mapping.Table;

import com.datastax.driver.core.DataType;

/**
 * This is an example of dynamic table that creates each time new column with Post timestamp annotated by @ColumnId.
 * 
 * It is possible to use a static table for posts and identify them by PostId(UUID), but in this case we need to use
 * MapReduce for Big Data to find posts for particular user, so it is better to have index (userId) -> index (post time)
 * architecture. It helps a lot to build eventually a search index for the particular user.
 * 
 * @author Alex Shvid
 */
@Table(name = "comments")
public class Comment {

	/*
	 * Primary Row ID
	 */
	@Id
	private String author;

	/*
	 * Column ID
	 */
	@ColumnId
	@Qualify(type = DataType.Name.TIMESTAMP)
	private Date time;

	private String text;

	@Qualify(type = DataType.Name.SET, typeArguments = { DataType.Name.TEXT })
	private Set<String> likes;

	/*
	 * Reference to the Post
	 */
	private String postAuthor;
	private Date postTime;

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

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Set<String> getLikes() {
		return likes;
	}

	public void setLikes(Set<String> likes) {
		this.likes = likes;
	}

	public String getPostAuthor() {
		return postAuthor;
	}

	public void setPostAuthor(String postAuthor) {
		this.postAuthor = postAuthor;
	}

	public Date getPostTime() {
		return postTime;
	}

	public void setPostTime(Date postTime) {
		this.postTime = postTime;
	}

}
