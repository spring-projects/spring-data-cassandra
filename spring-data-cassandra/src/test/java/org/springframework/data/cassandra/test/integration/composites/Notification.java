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
import org.springframework.data.cassandra.mapping.Indexed;
import org.springframework.data.cassandra.mapping.Table;

/**
 * This is an example of dynamic table that creates each time new column with Notification timestamp. By default it is
 * active Notification until user deactivate it. This table uses index on the field active to access in WHERE cause only
 * for active notifications.
 * 
 * @author Alex Shvid
 */
@Table("notifications")
public class Notification {

	/*
	 * Primary Key
	 */
	@Id
	private NotificationPK pk;

	@Indexed
	private boolean active;

	/*
	 * Reference data
	 */

	private String type; // comment, post
	private String refAuthor;
	private Date refTime;

	public NotificationPK getPk() {
		return pk;
	}

	public void setPk(NotificationPK pk) {
		this.pk = pk;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getRefAuthor() {
		return refAuthor;
	}

	public void setRefAuthor(String refAuthor) {
		this.refAuthor = refAuthor;
	}

	public Date getRefTime() {
		return refTime;
	}

	public void setRefTime(Date refTime) {
		this.refTime = refTime;
	}

}
