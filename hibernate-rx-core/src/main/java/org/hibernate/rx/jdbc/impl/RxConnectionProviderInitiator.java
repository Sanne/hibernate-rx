package org.hibernate.rx.jdbc.impl;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class RxConnectionProviderInitiator implements StandardServiceInitiator<ConnectionProvider> {

	public static RxConnectionProviderInitiator INSTANCE = new RxConnectionProviderInitiator();

	@Override
	public Class<ConnectionProvider> getServiceInitiated() {
		return ConnectionProvider.class;
	}

	@Override
	public ConnectionProvider initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new NoopConnectionProvider();
	}
}
