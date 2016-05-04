/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.cassandra.test.integration.support;

import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.NettyOptions;

import io.netty.channel.EventLoopGroup;

/**
 * {@link NettyOptions} to shutdown a {@link com.datastax.driver.core.Cluster} instance without wait time.
 *
 * @author Mark Paluch
 * @since 1.5
 */
public class FastShutdownNettyOptions extends NettyOptions {

	public final static FastShutdownNettyOptions INSTANCE = new FastShutdownNettyOptions();

	@Override
	public void onClusterClose(EventLoopGroup eventLoopGroup) {
		eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
	}
}
