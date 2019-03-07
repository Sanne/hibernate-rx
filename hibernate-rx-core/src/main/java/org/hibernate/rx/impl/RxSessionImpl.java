package org.hibernate.rx.impl;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import javax.persistence.EntityTransaction;

import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.rx.RxHibernateSessionFactory;
import org.hibernate.rx.RxQuery;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.StateControl;
import org.hibernate.service.ServiceRegistry;

public class RxSessionImpl implements RxSession {
	private final RxHibernateSessionFactory factory;
	private final SessionImplementor delegate;

	public RxSessionImpl(RxHibernateSessionFactory factory, SessionImplementor session) {
		this.factory = factory;
		this.delegate = session;
	}

	@Override
	public CompletionStage<Void> inTransaction(BiConsumer<RxSession, EntityTransaction> consumer) {
//		RxConnectionPoolProvider poolProvider = serviceRegistry().getService( RxConnectionPoolProvider.class );
//		RxConnection connection = poolProvider.getConnection();
//		return connection.inTransaction( consumer, this );
		return CompletableFuture.runAsync( () -> {
												   System.out.println( "Begin Transaction" );
												   delegate.getTransaction().begin();
												   consumer.accept( this, delegate.getTransaction() );
										   }
		);
	}

	@Override
	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object id) {
		return CompletableFuture.supplyAsync( () -> {
			System.out.println( "Start find" );
			T result = delegate.find( entityClass, id );
			System.out.println( "Return result: " + result );
			return Optional.ofNullable( result );
		} );
	}

	@Override
	public CompletionStage<Void> persist(Object entity) {
		return CompletableFuture.runAsync( () -> {
			System.out.println( "Start persist" );
			delegate.persist( entity );
			System.out.println( "End persist" );
		} );
	}

//
//	private void firePersist(RxPersistEvent event) {
//		try {
//			// checkTransactionSynchStatus();
//			// checkNoUnresolvedActionsBeforeOperation();
//
//			delegate.persist(  );
//			for ( RxPersistEventListener listener : listeners( EventType.PERSIST ) ) {
//				listener.onPersist( event );
//			}
//		}
//		catch (MappingException e) {
//			throw exceptionConverter().convert( new IllegalArgumentException( e.getMessage() ) );
//		}
//		catch (RuntimeException e) {
//			throw exceptionConverter().convert( e );
//		}
//		finally {
////			try {
////				checkNoUnresolvedActionsAfterOperation();
////			}
////			catch (RuntimeException e) {
////				throw exceptionConverter.convert( e );
////			}
//		}
//	}

	private ExceptionConverter exceptionConverter() {
		return delegate.unwrap( EventSource.class ).getExceptionConverter();
	}

	private <T> Iterable<T> listeners(EventType<T> type) {
		return eventListenerGroup( type ).listeners();
	}

	private <T> EventListenerGroup<T> eventListenerGroup(EventType<T> type) {
		return factory.unwrap( SessionFactoryImplementor.class )
				.getServiceRegistry().getService( EventListenerRegistry.class )
				.getEventListenerGroup( type );
	}

	@Override
	public CompletionStage<Void> remove(Object entity) {
		return CompletableFuture.runAsync( () -> {
			delegate.remove( entity );
		} );
	}

	@Override
	public <R> RxQuery<R> createQuery(Class<R> resultType, String jpql) {
		return null;
	}

	@Override
	public StateControl sessionState() {
		return null;
	}

	private ServiceRegistry serviceRegistry() {
		return factory.unwrap( SessionFactoryImplementor.class ).getServiceRegistry();
	}
}
