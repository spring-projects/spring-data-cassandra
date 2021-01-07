/*
 * Copyright 2016-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SettableListenableFuture;
import org.springframework.util.concurrent.SuccessCallback;

/**
 * Adapter class to {@link ListenableFuture} {@link ExecutionException} by applying a
 * {@link PersistenceExceptionTranslator}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class ExceptionTranslatingListenableFutureAdapter<T> implements ListenableFuture<T> {

	private final ListenableFuture<T> adaptee;

	private final ListenableFuture<T> future;

	/**
	 * Create a new {@link ExceptionTranslatingListenableFutureAdapter} given a {@link ListenableFuture} and a
	 * {@link PersistenceExceptionTranslator}.
	 *
	 * @param adaptee must not be {@literal null}.
	 * @param persistenceExceptionTranslator must not be {@literal null}.
	 */
	ExceptionTranslatingListenableFutureAdapter(ListenableFuture<T> adaptee,
			PersistenceExceptionTranslator persistenceExceptionTranslator) {

		Assert.notNull(adaptee, "ListenableFuture must not be null");
		Assert.notNull(persistenceExceptionTranslator, "PersistenceExceptionTranslator must not be null");

		this.adaptee = adaptee;
		this.future = adaptListenableFuture(adaptee, persistenceExceptionTranslator);
	}

	private static <T> ListenableFuture<T> adaptListenableFuture(ListenableFuture<T> listenableFuture,
			PersistenceExceptionTranslator exceptionTranslator) {

		SettableListenableFuture<T> settableFuture = new SettableListenableFuture<>();

		listenableFuture.addCallback(new ListenableFutureCallback<T>() {

			@Override
			public void onSuccess(@Nullable T result) {
				settableFuture.set(result);
			}

			@Override
			public void onFailure(Throwable ex) {
				if (ex instanceof RuntimeException) {
					DataAccessException dataAccessException = exceptionTranslator
							.translateExceptionIfPossible((RuntimeException) ex);

					if (dataAccessException != null) {
						settableFuture.setException(dataAccessException);
						return;
					}
				}

				settableFuture.setException(ex);
			}
		});

		return settableFuture;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.util.concurrent.ListenableFuture#addCallback(org.springframework.util.concurrent.ListenableFutureCallback)
	 */
	@Override
	public void addCallback(ListenableFutureCallback<? super T> callback) {
		future.addCallback(callback);
	}

	/* (non-Javadoc)
	 * @see org.springframework.util.concurrent.ListenableFuture#addCallback(org.springframework.util.concurrent.SuccessCallback, org.springframework.util.concurrent.FailureCallback)
	 */
	@Override
	public void addCallback(SuccessCallback<? super T> successCallback, FailureCallback failureCallback) {
		future.addCallback(successCallback, failureCallback);
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Future#cancel(boolean)
	 */
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return adaptee.cancel(mayInterruptIfRunning);
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Future#isCancelled()
	 */
	@Override
	public boolean isCancelled() {
		return adaptee.isCancelled();
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Future#isDone()
	 */
	@Override
	public boolean isDone() {
		return future.isDone();
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Future#get()
	 */
	@Override
	public T get() throws InterruptedException, ExecutionException {
		return future.get();
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return future.get(timeout, unit);
	}
}
