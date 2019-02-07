/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.SessionFactoryServiceRegistryFactory;

public class RxSessionFactoryServiceRegistryFactoryInitiator implements StandardServiceInitiator<SessionFactoryServiceRegistryFactory> {

	public static final RxSessionFactoryServiceRegistryFactoryInitiator INSTANCE = new RxSessionFactoryServiceRegistryFactoryInitiator();

	@Override
	public Class<SessionFactoryServiceRegistryFactory> getServiceInitiated() {
		return SessionFactoryServiceRegistryFactory.class;
	}

	@Override
	public SessionFactoryServiceRegistryFactory initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new RxSessionFactoryServiceRegistryFactoryImpl( registry );
	}
}
