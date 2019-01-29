package org.hibernate.rx;

import java.util.concurrent.CompletableFuture;

import io.reactivex.Maybe;

/**
 * Right now we are playing around, but this is going to be the core
 * interface of the project.
 */
public interface RxSession {

	<T> Maybe<T> find(Class<T> entityClass, Object id);

	CompletableFuture<Void> persist(Object entity);

	CompletableFuture<Void> remove(Object entity);

	<R> RxQuery<R> createQuery(Class<R> resultType, String jpql);

	StateControl sessionState();

}
