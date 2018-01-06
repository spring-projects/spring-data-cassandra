/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.support;

import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;

import java.util.concurrent.ThreadFactory;

import com.datastax.driver.core.NettyOptions;

/**
 * {@link NettyOptions} to for Integration tests a {@link com.datastax.driver.core.Cluster}. This class caches and
 * reuses (on a best-effort basis) {@link EventLoopGroup} and {@link Timer} instances during the tests. Caching reduces
 * thread disposal that leads to a overall improved resource reusage during tests.
 *
 * @author Mark Paluch
 * @since 1.5
 */
public class IntegrationTestNettyOptions extends NettyOptions {

	public static final IntegrationTestNettyOptions INSTANCE = new IntegrationTestNettyOptions();
	private volatile static EventLoopGroup eventLoopGroup;
	private volatile static Timer timer;

	@Override
	public EventLoopGroup eventLoopGroup(ThreadFactory threadFactory) {

		if (eventLoopGroup != null) {
			return eventLoopGroup;
		}

		EventLoopGroup eventLoopGroup = super.eventLoopGroup(r -> {

			Thread thread = threadFactory.newThread(r);
			thread.setDaemon(true);
			return thread;
		});

		IntegrationTestNettyOptions.eventLoopGroup = eventLoopGroup;
		return eventLoopGroup;
	}

	@Override
	public Timer timer(ThreadFactory threadFactory) {

		if (timer != null) {
			return timer;
		}

		Timer timer = super.timer(threadFactory);
		Runtime.getRuntime().addShutdownHook(new Thread(timer::stop));

		IntegrationTestNettyOptions.timer = timer;
		return timer;

	}

	@Override
	public void onClusterClose(EventLoopGroup eventLoopGroup) {}

	@Override
	public void onClusterClose(Timer timer) {}
}
