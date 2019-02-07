package org.hibernate.rx.impl;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.Session;
import org.hibernate.event.spi.EventSource;
import org.hibernate.rx.RxQuery;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.RxHibernateSessionFactory;
import org.hibernate.rx.StateControl;

public class RxSessionImpl implements RxSession {
	private Session delegate;

	public RxSessionImpl(RxHibernateSessionFactory factory, EventSource session) {
	}

	public RxSessionImpl(Session session) {
		this.delegate = session;
	}

	@Override
	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object id) {
		return null;
	}

	@Override
	public CompletionStage<Void> persist(Object entity) {
		return CompletableFuture.runAsync( () -> {
			delegate.beginTransaction();
			delegate.persist( entity );
			delegate.getTransaction().commit();
		} );
	}

	@Override
	public CompletionStage<Void> remove(Object entity) {
		return null;
	}

	@Override
	public <R> RxQuery<R> createQuery(Class<R> resultType, String jpql) {
		return null;
	}

	@Override
	public StateControl sessionState() {
		return null;
	}
}
