package com.ddougher.remoting;

import java.io.Serializable;
import java.util.UUID;

public class GridProtocol {

	static class RemoteInvocationRequest implements Serializable {
		private static final long serialVersionUID = 1L;
		UUID requestId;
		UUID objectId;
		String methodName;
		Class<?> [] parameterTypes;
		Object [] args;
		public RemoteInvocationRequest(UUID requestId, UUID objectId, String methodName, Class<?> [] parameterTypes, Object[] args) {
			super();
			this.methodName = methodName;
			this.requestId = requestId;
			this.objectId = objectId;
			this.parameterTypes = parameterTypes;
			this.args = args;
		}
		
		
	}
	
	static class RemoteInvocationResponse implements Serializable {
		private static final long serialVersionUID = 1L;
		UUID requestId;
		Object result;
		public RemoteInvocationResponse(UUID requestId, Object result) {
			super();
			this.requestId = requestId;
			this.result = result;
		}
		
	}

	static class CreateObjectRequest implements Serializable {
		private static final long serialVersionUID = 1L;
		Object [] args;
		Class<?> [] parameters;
		Class<?> objectClass;
		UUID requestId;
		public CreateObjectRequest(UUID requestId, Class<?> objectClass, Class<?> [] parameters, Object[] args) {
			this.requestId = requestId;
			this.objectClass = objectClass;
			this.parameters = parameters;
			this.args = args;
		}
	}
	
	static class CreateObjectResponse implements Serializable {
		private static final long serialVersionUID = 1L;
		UUID objectId;
		UUID requestId;
		public CreateObjectResponse(UUID requestId, UUID objectId) {
			this.requestId = requestId;
			this.objectId = objectId;
		}
	}

	static class FindClassRequest implements Serializable {
		private static final long serialVersionUID = 1L;
		String className;
		public FindClassRequest(String className) {
			super();
			this.className = className;
		}
	}

	static class FindClassResponse implements Serializable {
		private static final long serialVersionUID = 1L;
		String className;
		byte [] bytes;
		public FindClassResponse(String className, byte[] bytes) {
			super();
			this.className = className;
			this.bytes = bytes;
		}
	}
	
}
