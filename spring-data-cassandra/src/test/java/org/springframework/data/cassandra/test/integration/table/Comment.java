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

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.CassandraType;
import org.springframework.data.cassandra.mapping.Table;

import com.datastax.driver.core.DataType;

/**
 * This is an example of dynamic table (wide row). PartitionKey (former RowId) is pk.author. ClusteredColumn (former
 * Column Id) is pk.time
 * 
 * @author Alex Shvid
 */
@Table("comments")
public class Comment {

	/*
	 * Primary Key
	 */
	@PrimaryKey
	private CommentPK pk;

	private String text;

	@CassandraType(type = DataType.Name.SET, typeArguments = { DataType.Name.TEXT })
	private Set<String> likes;

	/*
	 * Reference to the Post
	 */
	private String postAuthor;
	private Date postTime;

	public CommentPK getPk() {
		return pk;
	}

	public void setPk(CommentPK pk) {
		this.pk = pk;
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
