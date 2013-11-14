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
package org.springframework.data.cassandra.table;

import java.util.Date;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.RowId;
import org.springframework.data.cassandra.mapping.Table;

/**
 * This is an example of the Users statis table, where all fields are columns in Cassandra row. Some fields can be
 * Set,List,Map like emails.
 * 
 * User contains base information related for separate user, like names, additional information, emails, following
 * users, friends.
 * 
 * @author Alex Shvid
 */
@Table(name = "log_entry")
public class LogEntry {

	/*
	 * Primary Row ID
	 */
	@RowId
	private Date logDate;

	private String hostname;

	private String logData;

	/**
	 * @return Returns the logDate.
	 */
	public Date getLogDate() {
		return logDate;
	}

	/**
	 * @param logDate The logDate to set.
	 */
	public void setLogDate(Date logDate) {
		this.logDate = logDate;
	}

	/**
	 * @return Returns the hostname.
	 */
	public String getHostname() {
		return hostname;
	}

	/**
	 * @param hostname The hostname to set.
	 */
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	/**
	 * @return Returns the logData.
	 */
	public String getLogData() {
		return logData;
	}

	/**
	 * @param logData The logData to set.
	 */
	public void setLogData(String logData) {
		this.logData = logData;
	}

}