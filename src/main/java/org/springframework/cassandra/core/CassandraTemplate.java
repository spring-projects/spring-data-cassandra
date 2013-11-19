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
package org.springframework.cassandra.core;

import org.springframework.cassandra.support.CassandraAccessor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraDataTemplate;
import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

/**
 * The CassandraTemplate is a Spring convenience wrapper for low level and explicit operations on the Cassandra
 * Database. For working iwth POJOs, use the {@link CassandraDataTemplate}
 * 
 * @author Alex Shvid
 * @author David Webb
 */
public class CassandraTemplate extends CassandraAccessor implements CassandraOperations {

	/**
	 * Blank constructor. You must wire in the Session before use.
	 * 
	 */
	public CassandraTemplate() {
	}

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 */
	public CassandraTemplate(Session session) {
		setSession(session);
		afterPropertiesSet();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#execute(org.springframework.data.cassandra.core.SessionCallback)
	 */
	@Override
	public <T> T execute(SessionCallback<T> sessionCallback) {
		return doExecute(sessionCallback);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#execute(java.lang.String)
	 */
	@Override
	public void execute(final String cql) {

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {
				return s.execute(cql);
			}
		});

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#executeQuery(java.lang.String)
	 */
	@Override
	public ResultSet executeQuery(final String query) {

		return doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {

				return s.execute(query);

			}

		});

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#executeQueryAsync(java.lang.String)
	 */
	@Override
	public ResultSetFuture executeQueryAsynchronously(final String query) {

		return doExecute(new SessionCallback<ResultSetFuture>() {

			@Override
			public ResultSetFuture doInSession(Session s) throws DataAccessException {

				return s.executeAsync(query);

			}

		});

	}

	/**
	 * Attempt to translate a Runtime Exception to a Spring Data Exception
	 * 
	 * @param ex
	 * @return
	 */
	public RuntimeException potentiallyConvertRuntimeException(RuntimeException ex) {
		RuntimeException resolved = getExceptionTranslator().translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected <T> T doExecute(SessionCallback<T> callback) {

		Assert.notNull(callback);

		try {

			return callback.doInSession(getSession());

		} catch (DataAccessException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

}
