package com.ddougher.remoting;

import java.io.Serializable;
import java.util.UUID;

public class GridProtocol {

	static class RemoteInvocationRequest implements Serializable {
		private static final long serialVersionUID = 1L;
		UUID requestId;
		UUID objectId;
		String methodName;
		Class<? extends Serializable> [] parameters;
		Serializable [] args;
		public RemoteInvocationRequest(UUID requestId, UUID objectId, String methodName, Class<? extends Serializable>[] parameters, Serializable[] args) {
			super();
			this.methodName = methodName;
			this.requestId = requestId;
			this.objectId = objectId;
			this.parameters = parameters;
			this.args = args;
		}
		
		
	}
	
	static class RemoteInvocationResponse implements Serializable {
		private static final long serialVersionUID = 1L;
		UUID requestId;
		UUID objectId;
		Serializable result;
		public RemoteInvocationResponse(UUID requestId, UUID objectId, Serializable result) {
			super();
			this.requestId = requestId;
			this.objectId = objectId;
			this.result = result;
		}
		
	}

	static class CreateObjectRequest implements Serializable {
		private static final long serialVersionUID = 1L;
		Serializable [] args;
		Class<? extends Serializable> [] parameters;
		String className;
		UUID requestId;
		public CreateObjectRequest(UUID requestId, String className, Class<? extends Serializable> [] parameters, Serializable[] args) {
			this.requestId = requestId;
			this.className = className;
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
