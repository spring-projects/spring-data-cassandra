/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.core.GenericTypeResolver;

/**
 * Base class to implement domain specific {@link ApplicationListener}s for {@link CassandraMappingEvent}.
 *
 * @author Lukasz Antoniak
 * @author Mark Paluch
 * @since 2.1
 */
public abstract class AbstractCassandraEventListener<E> implements ApplicationListener<CassandraMappingEvent<?>> {

	protected static final Logger log = LoggerFactory.getLogger(AbstractCassandraEventListener.class);

	private final Class<?> domainClass;

	/**
	 * Creates a new {@link AbstractCassandraEventListener}.
	 */
	public AbstractCassandraEventListener() {

		Class<?> typeArgument = GenericTypeResolver.resolveTypeArgument(getClass(), AbstractCassandraEventListener.class);

		this.domainClass = typeArgument == null ? Object.class : typeArgument;
	}

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	@SuppressWarnings({ "unchecked" })
	@Override
	public void onApplicationEvent(CassandraMappingEvent<?> event) {

		Object source = event.getSource();

		if (event instanceof AfterLoadEvent) {

			AfterLoadEvent<?> afterLoadEvent = (AfterLoadEvent<?>) event;

			if (domainClass.isAssignableFrom(afterLoadEvent.getType())) {
				onAfterLoad((AfterLoadEvent<E>) event);
			}

			return;
		}

		if (event instanceof AbstractDeleteEvent) {

			Class<?> eventDomainType = ((AbstractDeleteEvent<?>) event).getType();

			if (eventDomainType != null && domainClass.isAssignableFrom(eventDomainType)) {

				if (event instanceof BeforeDeleteEvent) {
					onBeforeDelete((BeforeDeleteEvent<E>) event);
				}
				if (event instanceof AfterDeleteEvent) {
					onAfterDelete((AfterDeleteEvent<E>) event);
				}
			}

			return;
		}

		// Check for matching domain type and invoke callbacks.
		if (!domainClass.isAssignableFrom(source.getClass())) {
			return;
		}

		if (event instanceof BeforeSaveEvent) {
			onBeforeSave((BeforeSaveEvent<E>) event);
		} else if (event instanceof AfterSaveEvent) {
			onAfterSave((AfterSaveEvent<E>) event);
		} else if (event instanceof AfterConvertEvent) {
			onAfterConvert((AfterConvertEvent<E>) event);
		}
	}

	/**
	 * Captures {@link BeforeSaveEvent}.
	 *
	 * @param event will never be {@literal null}.
	 */
	public void onBeforeSave(BeforeSaveEvent<E> event) {
		if (log.isDebugEnabled()) {
			log.debug("onBeforeSave({})", event.getSource());
		}
	}

	/**
	 * Captures {@link AfterSaveEvent}.
	 *
	 * @param event will never be {@literal null}.
	 */
	public void onAfterSave(AfterSaveEvent<E> event) {
		if (log.isDebugEnabled()) {
			log.debug("onAfterSave({})", event.getSource());
		}
	}

	/**
	 * Captures {@link BeforeDeleteEvent}.
	 *
	 * @param event will never be {@literal null}.
	 */
	public void onBeforeDelete(BeforeDeleteEvent<E> event) {
		if (log.isDebugEnabled()) {
			log.debug("onBeforeDelete({})", event.getSource());
		}
	}

	/**
	 * Captures {@link AfterDeleteEvent}.
	 *
	 * @param event will never be {@literal null}.
	 */
	public void onAfterDelete(AfterDeleteEvent<E> event) {
		if (log.isDebugEnabled()) {
			log.debug("onAfterDelete({})", event.getSource());
		}
	}

	/**
	 * Captures {@link AfterLoadEvent}.
	 *
	 * @param event will never be {@literal null}.
	 */
	public void onAfterLoad(AfterLoadEvent<E> event) {
		if (log.isDebugEnabled()) {
			log.debug("onAfterLoad({})", event.getSource());
		}
	}

	/**
	 * Captures {@link AfterConvertEvent}.
	 *
	 * @param event will never be {@literal null}.
	 */
	public void onAfterConvert(AfterConvertEvent<E> event) {
		if (log.isDebugEnabled()) {
			log.debug("onAfterConvert({})", event.getSource());
		}
	}
}
