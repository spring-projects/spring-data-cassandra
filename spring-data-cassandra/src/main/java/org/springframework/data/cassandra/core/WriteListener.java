/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import java.util.Collection;

import com.datastax.driver.core.ResultSet;

/**
 * Listener for asynchronous repository insert or update methods.
 * 
 * @author Matthew T. Adams
 */
public interface WriteListener<T> {

	/**
	 * Called upon completion of the asynchronous insert or update.
	 * 
	 * @param entities The entities inserted or updated.
	 */
	void onWriteComplete(Collection<T> entities);

	/**
	 * Called if an exception is raised while getting or converting the {@link ResultSet}.
	 */
	void onException(Exception x);
}
