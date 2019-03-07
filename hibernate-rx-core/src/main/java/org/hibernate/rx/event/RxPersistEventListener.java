package org.hibernate.rx.event;

import java.io.Serializable;
import java.util.Map;

import org.hibernate.HibernateException;

public interface RxPersistEventListener extends Serializable {

    /** 
     * Handle the given create event.
     *
     * @param event The create event to be handled.
     * @throws HibernateException
     */
	void onPersist(RxPersistEvent event) throws HibernateException;

    /** 
     * Handle the given create event.
     *
     * @param event The create event to be handled.
     * @throws HibernateException
     */
	void onPersist(RxPersistEvent event, Map<?,?> createdAlready) throws HibernateException;

}