package org.hibernate.rx;

import java.util.function.Consumer;

import org.hibernate.Session;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.internal.SessionImpl;

public interface RxHibernateSession extends Session {

	@Override
	RxHibernateSessionFactory getSessionFactory();

	RxSession getRxSession();
}
