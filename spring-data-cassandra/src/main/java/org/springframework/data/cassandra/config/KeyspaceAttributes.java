/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.cassandra.config;

import java.util.Collection;

/**
 * Keyspace attributes are used for manipulation around keyspace at the startup. Auto property defines the way how to do
 * this. Other attributes used to ensure or update keyspace settings.
 * 
 * @author Alex Shvid
 */
public class KeyspaceAttributes extends org.springframework.cassandra.config.KeyspaceAttributes {

	/*
	 * auto possible values:
	 * validate: validate the keyspace, makes no changes.
	   * update: update the keyspace.
	   * create: creates the keyspace, destroying previous data.
	   * create-drop: drop the keyspace at the end of the session.
	 */
	public static final String AUTO_VALIDATE = "validate";
	public static final String AUTO_UPDATE = "update";
	public static final String AUTO_CREATE = "create";
	public static final String AUTO_CREATE_DROP = "create-drop";

	private String auto = AUTO_VALIDATE;

	private Collection<TableAttributes> tables;

	public String getAuto() {
		return auto;
	}

	public void setAuto(String auto) {
		this.auto = auto;
	}

	public boolean isValidate() {
		return AUTO_VALIDATE.equals(auto);
	}

	public boolean isUpdate() {
		return AUTO_UPDATE.equals(auto);
	}

	public boolean isCreate() {
		return AUTO_CREATE.equals(auto);
	}

	public boolean isCreateDrop() {
		return AUTO_CREATE_DROP.equals(auto);
	}

	public Collection<TableAttributes> getTables() {
		return tables;
	}

	public void setTables(Collection<TableAttributes> tables) {
		this.tables = tables;
	}

}
