package org.hibernate.rx;

import org.hibernate.Session;

public interface RxHibernateSession extends Session {

	@Override
	RxHibernateSessionFactory getSessionFactory();

	RxSession getRxSession();
}
