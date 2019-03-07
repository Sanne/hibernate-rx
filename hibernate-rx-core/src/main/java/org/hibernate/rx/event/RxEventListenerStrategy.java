package org.hibernate.rx.event;

import org.hibernate.Metamodel;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;

public class RxEventListenerStrategy implements EventListenerRegistry {

	private final EventListenerRegistry delegate;

	public RxEventListenerStrategy(EventListenerRegistry delegate) {
		this.delegate = delegate;
	}

	@Override
	public void prepare(Metamodel metamodel) {
		delegate.prepare( metamodel );
	}

	@Override
	public <T> EventListenerGroup<T> getEventListenerGroup(EventType<T> eventType) {
		if ( eventType == EventType.PERSIST ) {
			return (EventListenerGroup<T>) new DefaultRxPersistEventListener();
		}
		return delegate.getEventListenerGroup( eventType );
	}

	@Override
	public void addDuplicationStrategy(DuplicationStrategy strategy) {
		delegate.addDuplicationStrategy( strategy );
	}

	@Override
	public <T> void setListeners(EventType<T> type, Class<? extends T>... listeners) {
		delegate.setListeners( type, listeners );
	}

	@Override
	public <T> void setListeners(EventType<T> type, T... listeners) {
		delegate.setListeners( type, listeners );
	}

	@Override
	public <T> void appendListeners(EventType<T> type, Class<? extends T>... listeners) {
		delegate.appendListeners( type, listeners );
	}

	@Override
	public <T> void appendListeners(EventType<T> type, T... listeners) {
		delegate.appendListeners( type, listeners );
	}

	@Override
	public <T> void prependListeners(EventType<T> type, Class<? extends T>... listeners) {
		delegate.prependListeners( type, listeners );
	}

	@Override
	public <T> void prependListeners(EventType<T> type, T... listeners) {
		delegate.prependListeners( type, listeners );
	}
}
