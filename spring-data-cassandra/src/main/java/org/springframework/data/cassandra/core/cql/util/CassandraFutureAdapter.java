/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.ListenableFutureCallbackRegistry;
import org.springframework.util.concurrent.SuccessCallback;

/**
 * Adapts a {@link CompletableFuture} or {@link CompletionStage} into a Spring {@link ListenableFuture} applying
 * {@link PersistenceExceptionTranslator}.
 *
 * @since 3.0
 * @param <T> the result type returned by this Future's {@code get} method
 */
public class CassandraFutureAdapter<T> implements ListenableFuture<T> {

	private final CompletableFuture<T> completableFuture;
	private final CompletableFuture<T> translated;

	private final ListenableFutureCallbackRegistry<T> callbacks = new ListenableFutureCallbackRegistry<>();

	/**
	 * Create a new adapter for the given {@link CompletionStage}.
	 */
	public CassandraFutureAdapter(CompletionStage<T> completionStage, PersistenceExceptionTranslator exceptionMapper) {
		this(completionStage.toCompletableFuture(), exceptionMapper);
	}

	/**
	 * Create a new adapter for the given {@link CompletableFuture}.
	 */
	public CassandraFutureAdapter(CompletableFuture<T> completableFuture,
			PersistenceExceptionTranslator exceptionMapper) {
		this.completableFuture = completableFuture;
		this.translated = new CompletableFuture<>();
		this.completableFuture.whenComplete((result, ex) -> {
			if (ex != null) {

				Throwable exceptionToUse = ex;
				if (exceptionToUse instanceof CompletionException) {
					exceptionToUse = exceptionToUse.getCause();
				}

				if (exceptionToUse instanceof RuntimeException) {
					RuntimeException translated = exceptionMapper.translateExceptionIfPossible((RuntimeException) exceptionToUse);
					this.callbacks.failure(translated != null ? translated : ex);
					this.translated.completeExceptionally(translated != null ? translated : ex);
				} else {
					this.callbacks.failure(ex);
					this.translated.completeExceptionally(ex);
				}
			} else {
				this.callbacks.success(result);
				this.translated.complete(result);
			}
		});
	}

	@Override
	public void addCallback(ListenableFutureCallback<? super T> callback) {
		this.callbacks.addCallback(callback);
	}

	@Override
	public void addCallback(SuccessCallback<? super T> successCallback, FailureCallback failureCallback) {
		this.callbacks.addSuccessCallback(successCallback);
		this.callbacks.addFailureCallback(failureCallback);
	}

	@Override
	public CompletableFuture<T> completable() {
		return this.completableFuture;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return this.completableFuture.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return this.completableFuture.isCancelled();
	}

	@Override
	public boolean isDone() {
		return this.completableFuture.isDone();
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		return this.translated.get();
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return this.translated.get(timeout, unit);
	}

}
