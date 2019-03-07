package org.hibernate.rx.boot.impl;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.metamodel.model.creation.spi.RuntimeModelDescriptorClassResolver;
import org.hibernate.rx.service.RxRuntimeModelDescriptorResolver;
import org.hibernate.service.spi.ServiceRegistryImplementor;

final class RxRuntimeModelDescriptorClassResolver implements StandardServiceInitiator<RuntimeModelDescriptorClassResolver> {

    public static final org.hibernate.metamodel.model.creation.internal.PersisterClassResolverInitiator INSTANCE = new org.hibernate.metamodel.model.creation.internal.PersisterClassResolverInitiator();

    @Override
    public Class<RuntimeModelDescriptorClassResolver> getServiceInitiated() {
        return RuntimeModelDescriptorClassResolver.class;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public RuntimeModelDescriptorClassResolver initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
        return new RxRuntimeModelDescriptorResolver();
    }

}
