package org.hibernate.rx;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import javax.persistence.EntityTransaction;

/**
 * Right now we are playing around, but this is going to be the core
 * interface of the project.
 */
public interface RxSession {

	CompletionStage<Void> inTransaction(BiConsumer<RxSession, EntityTransaction> consumer);

	<T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object id);

	CompletionStage<Void> persist(Object entity);

	CompletionStage<Void> remove(Object entity);

	<R> RxQuery<R> createQuery(Class<R> resultType, String jpql);

	StateControl sessionState();

}
