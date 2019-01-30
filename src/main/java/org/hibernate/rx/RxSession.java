package org.hibernate.rx;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import io.reactivex.Single;

/**
 * Right now we are playing around, but this is going to be the core
 * interface of the project.
 */
public interface RxSession {

	<T> Single<Optional<T>> find(Class<T> entityClass, Object id);

	CompletionStage<Void> persist(Object entity);

	void remove(Object entity);

	<R> RxQuery<R> createQuery(Class<R> resultType, String jpql);

	StateControl sessionState();

}
