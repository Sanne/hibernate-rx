package org.hibernate.rx.event;

import org.hibernate.event.spi.AbstractEvent;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PersistEvent;

/**
 * An event class for persist()
 */
public class RxPersistEvent extends PersistEvent {

	public RxPersistEvent(String entityName, Object original, EventSource source) {
		super( entityName, original, source );
	}
}