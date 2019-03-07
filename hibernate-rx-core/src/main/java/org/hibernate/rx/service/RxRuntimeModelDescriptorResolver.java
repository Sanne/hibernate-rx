package org.hibernate.rx.service;

import org.hibernate.metamodel.model.creation.internal.StandardRuntimeModelDescriptorClassResolver;
import org.hibernate.metamodel.model.domain.spi.EntityTypeDescriptor;
import org.hibernate.rx.impl.RxSingleTableEntityTypeDescriptor;

public class RxRuntimeModelDescriptorResolver extends StandardRuntimeModelDescriptorClassResolver {

	@Override
	public Class<? extends EntityTypeDescriptor> singleTableEntityDescriptor() {
		return RxSingleTableEntityTypeDescriptor.class;
	}
}
