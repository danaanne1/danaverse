package com.ddougher.remoting;

import java.io.Serializable;
import java.util.UUID;

public class GridProtocol {

	static class CreateObjectRequest implements Serializable {
		private static final long serialVersionUID = 1L;
		Object [] args;
		Class<?> [] parameters;
		String className;
		UUID requestId;
		public CreateObjectRequest(UUID requestId, String className, Class<?> [] parameters, Object[] args) {
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

}
