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
import java.util.Map;
import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.mapping.Table;

/**
 * This is an example of dynamic table that creates each time new column with Post timestamp. It is possible to use a
 * static table for posts and identify them by PostId(UUID), but in this case we need to use MapReduce for Big Data to
 * find posts for particular user, so it is better to have index (userId) -> index (post time) architecture. It helps a
 * lot to build eventually a search index for the particular user.
 * 
 * @author Alex Shvid
 */
@Table("posts")
public class Post {

	/*
	 * Primary Key
	 */
	@Id
	private PostPK pk;

	private String type; // status, share

	private String text;
	private Set<String> resources;
	private Map<Date, String> comments;
	private Set<String> likes;
	private Set<String> followers;

	public PostPK getPk() {
		return pk;
	}

	public void setPk(PostPK pk) {
		this.pk = pk;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Set<String> getResources() {
		return resources;
	}

	public void setResources(Set<String> resources) {
		this.resources = resources;
	}

	public Map<Date, String> getComments() {
		return comments;
	}

	public void setComments(Map<Date, String> comments) {
		this.comments = comments;
	}

	public Set<String> getLikes() {
		return likes;
	}

	public void setLikes(Set<String> likes) {
		this.likes = likes;
	}

	public Set<String> getFollowers() {
		return followers;
	}

	public void setFollowers(Set<String> followers) {
		this.followers = followers;
	}

}
