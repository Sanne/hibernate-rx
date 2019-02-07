package org.hibernate.rx.impl;

import org.hibernate.engine.spi.SessionDelegatorBaseImpl;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.internal.SessionImpl;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxHibernateSessionFactory;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.engine.spi.RxHibernateSessionFactoryImplementor;

public class RxHibernateSessionImpl extends SessionDelegatorBaseImpl implements RxHibernateSession {

	public RxHibernateSessionImpl(RxHibernateSessionFactory factory, SessionImplementor delegate) {
		super( delegate );
	}

	@Override
	public RxHibernateSessionFactoryImplementor getSessionFactory() {
		return delegate.getSessionFactory().unwrap( RxHibernateSessionFactoryImplementor.class );
	}

	@Override
	public RxSession getRxSession() {
		return new RxSessionImpl( this );
	}
}
