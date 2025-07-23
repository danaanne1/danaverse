package com.ddougher.proxamic;

import com.ddougher.buff.serialization.Array;
import com.ddougher.buff.serialization.Struct;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * A place where Dynamic Documents based on byte buffers come to life.
 * 
 * @author Dana
 *
 */
public class BuffDocument implements Document, DocumentStoreAware {

	private static final long serialVersionUID = 1L;

	private static final DocumentStore defaultDocStore = new LocalDocumentStore();

	public transient Struct root;
	
	private transient DocumentStore docStore = defaultDocStore;
	
	private interface Reference extends DocumentView {
		@Getter("__REF__") String ID();
		@Setter("__REF__") void ID(String value);
	}
	
	/**
	 * If the document store is network aware (see network aware doc stores) then this will export the doc store
	 * endpoint information
	 * followed by the document id. It is the responsibility of the caller to save documents before serialization.
	 * <p>
	 * If the document store is not serializable, will serialize the document as a byte stream
	 * <p>
	 * @param out
	 * @throws IOException
	 */
	private void writeObject(ObjectOutputStream out)
			throws IOException 
	{
		out.defaultWriteObject();
		if (Proxy.isProxyClass(docStore.getClass())) {
			for (Class<?> c: docStore.getClass().getInterfaces()) {
				if (Serializable.class.isAssignableFrom(c)) {
					out.writeObject(((NetworkAware)docStore).getEndpoint());
					out.writeObject(docStore.getID(this));
					return;
				}
			}
		} else if (docStore instanceof NetworkAware) {
			out.writeObject(((NetworkAware)docStore).getEndpoint());
			out.writeObject(docStore.getID(this));
			return;
		}
		out.writeObject(null);
		out.writeObject(toBytes());
	}

	public String dump() {
		return root.toString();
	}
	
	public String toString() {
		return dump();
	}
	
	/** only used when restoring from a doc store style serialization */
	private transient String resolveKey = null;

	private void readObject(ObjectInputStream in)
		     throws IOException, ClassNotFoundException
	{
		in.defaultReadObject();
		DocumentStore store = (DocumentStore)in.readObject();
		if (store != null) {
			docStore = store;
			resolveKey = (String)in.readObject();
			return;
		}
		docStore = defaultDocStore;
		root = new Struct(ByteBuffer.wrap((byte [])in.readObject()));
	}

	private Object readResolve() throws ObjectStreamException
	{
		if (resolveKey!=null)
			return docStore.get(resolveKey);
		return this;
	}
	
	private BuffDocument(Struct root) {
		this.root = root;
	}

	protected BuffDocument(ByteBuffer buffer) {
		this(new Struct(buffer));
	}

	protected BuffDocument() {
		this(new Struct());
	}

	public Document newInstance() {
		return docStore.newInstance();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public <T extends DocumentView> T as(final Class<T> documentClass) {
		return (T)Proxy.newProxyInstance(
				getClass().getClassLoader(), 
				new Class [] { documentClass }, 
				new BuffHandler(documentClass));
	}

	@Override
	public void setDocumentStore(DocumentStore docStore) {
		this.docStore = docStore;
	}
	
	@Override
	public DocumentStore getDocumentStore() {
		return this.docStore;
	}
	
	@Override
	public void remove(String field) {
		root.keySet().remove(field);
	}
	
	private class BuffHandler implements InvocationHandler, Serializable {
		private static final long serialVersionUID = 1L;
		Class<? extends DocumentView> documentClass;
		
		// visible for serialization
		@SuppressWarnings("unused")
		protected BuffHandler() {
			
		}

		public BuffHandler(Class<? extends DocumentView> documentClass) {
			this.documentClass = documentClass;
		}
		@Override
		public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
			if (method.isAnnotationPresent(Getter.class)) {
				return convertFromStructValue(
								method.getGenericReturnType(), 
								createIfRequired(method.getAnnotation(Getter.class).value(), method.getReturnType()),
								method.isAnnotationPresent(Indirect.class)
						);
			}
			if (method.isAnnotationPresent(Setter.class)) {
				return fluently
						(
								proxy,
								method,
								root.put 
								(
										method.getAnnotation(Setter.class).value(),
										convertToStructValue
										(
												method.getGenericParameterTypes()[0],
												args[0],
												method.isAnnotationPresent(Indirect.class)
										)
								)
							);
			}
			if (method.isDefault()) {
				return handleDefaultMethod(proxy,method,args,documentClass);
			}
			String methodName = method.getName();
			if (methodName.equals("document")) { 
				return BuffDocument.this;
			}
			if (methodName.equals("equals")) {
				if (args[0] instanceof DocumentView)
					return BuffDocument.this.equals(((DocumentView)args[0]).document());
				return false;
			}
			if (methodName.equals("hashCode")) {
				return BuffDocument.this.hashCode();
			}
			if (DocumentStoreAware.class.isAssignableFrom(documentClass) && methodName.equals("getDocumentStore")) {
				return BuffDocument.this.getDocumentStore();
			}
			if (DocumentStoreAware.class.isAssignableFrom(documentClass) && methodName.equals("setDocumentStore")) {
				BuffDocument.this.setDocumentStore((DocumentStore)args[0]);
				return null;
			}
			if (methodName.startsWith("get")) { 
				return convertFromStructValue
						(
								method.getGenericReturnType(), 
								createIfRequired(methodName.substring(3),method.getReturnType()),
								method.isAnnotationPresent(Indirect.class)
						);
			}
			if (methodName.startsWith("set")) {
				return fluently
						(
								proxy,
								method,
								root.put
								(
										methodName.substring(3), 
										convertToStructValue
										(
												method.getGenericParameterTypes()[0], 
												args[0],
												method.isAnnotationPresent(Indirect.class)
										)
								)
						);
			}
			if (methodName.startsWith("with")) {
				root.put(methodName.substring(4), convertToStructValue(method.getGenericParameterTypes()[0], args[0], method.isAnnotationPresent(Indirect.class)));
				return proxy;
			}
			if (methodName.equals("toString")) {
				return BuffDocument.this.toString();
			}
			throw new RuntimeException("No path to invoke for " + method.getName());
		}
		
		private Object fluently(Object proxy, Method method, Object structValue) {
			if ( (!method.getName().startsWith("with")) &&  method.getReturnType()==method.getParameterTypes()[0]) {
				return convertFromStructValue( method.getGenericReturnType(), structValue, method.isAnnotationPresent(Indirect.class) );
			} else {
				return proxy;
			}
		}
	
		private Object createIfRequired(String fieldName, Class<?> returnType) {
			if (!root.has(fieldName)) {
				if (List.class.isAssignableFrom(returnType)) {
					root.put(fieldName, new Array());
				}
				if (Map.class.isAssignableFrom(returnType) || DocumentView.class.isAssignableFrom(returnType)) {
					root.put(fieldName, new Struct());
				}
			}
			return root.get(fieldName);
		}
	}


	@SuppressWarnings("unchecked")
	private Object convertFromStructValue(Type methodReturnType, Object structValue, boolean indirect) {

		// pass nulls through
		if (structValue==null)
			return null;
		
		// get the parameterized conversion type
		Class<?> returnType = Object.class;
		if (methodReturnType instanceof Class) 
			returnType = (Class<?>)methodReturnType;
		if (methodReturnType instanceof ParameterizedType) 
			returnType = (Class<?>)((ParameterizedType)methodReturnType).getRawType();

		// if its a know collection type, we wrap:
		if (returnType.isArray()) 
			return mapToArray(returnType.getComponentType(),(Array)structValue, indirect);

		if (List.class.isAssignableFrom(returnType)) {
			if (methodReturnType instanceof ParameterizedType) 
				return mapToList(((ParameterizedType)methodReturnType).getActualTypeArguments()[0], (Array)structValue, indirect);
			return mapToList(Object.class,(Array)structValue, false);
		}

		if (Map.class.isAssignableFrom(returnType)) {
			if (methodReturnType instanceof ParameterizedType) 
				return mapToMap(((ParameterizedType)methodReturnType).getActualTypeArguments()[1],(Struct)structValue, indirect);
			return mapToMap(Object.class,(Struct)structValue, false);
		}

		// if its a document type, we unwrap (and possibly unpack)
		if (DocumentView.class.isAssignableFrom(returnType)) {
			DocumentView ob = new BuffDocument((Struct)structValue).as((Class<? extends DocumentView>) returnType);
			if (ob.document() instanceof DocumentStoreAware)
				((DocumentStoreAware)ob.document()).setDocumentStore(getDocumentStore());
			if (indirect) {
				String id = ob.document().as(Reference.class).ID();
				if (id == null)
					ob.document().as(Reference.class).ID(id=docStore.getID(docStore.newInstance()));
				ob = docStore.get((Class<? extends DocumentView>) returnType, id);
			}
			return ob;
		}
			
		return returnType.cast(structValue);
	}
	
	@SuppressWarnings("unchecked")
	private <T> T[] mapToArray(Class<T> arrayType, Array array, boolean indirect) {
		if (array==null) {
			// return (T[])java.lang.reflect.Array.newInstance(arrayType,0);
			return null;
		}
		T [] result = (T[])java.lang.reflect.Array.newInstance(arrayType, array.size());
		for (int i = 0; i < array.size(); i++) {
			result[i] = arrayType.cast(convertFromStructValue(arrayType, array.get(i), indirect));
		}
		return result;
	}

	private <T> Array mapFromArray(Class<?> arrayType, Object [] value, boolean indirect) {
		Array result = new Array();
		for (int i = 0; i < value.length; i++) {
			result.add(convertToStructValue(arrayType, value[i], indirect));
		}
		return result;
	}
	
	private <B> List<B> mapToList(final Type elementClass, final Array array, boolean indirect) {
		return new AbstractList<B>() {
			@SuppressWarnings("unchecked")
			@Override
			public B set(int index, B element) {
				return (B)convertFromStructValue(elementClass, array.set(index, convertToStructValue(elementClass, element, indirect)), indirect);
			}
			@Override
			public void add(int index, B element) {
				array.add(index, convertToStructValue(elementClass, element, indirect));
			}
			@SuppressWarnings("unchecked")
			@Override
			public B remove(int index) {
				return (B)convertFromStructValue(elementClass,array.remove(index), indirect);
			}
			@SuppressWarnings("unchecked")
			@Override
			public B get(int index) {
				return (B)convertFromStructValue(elementClass, array.get(index), indirect);
			}
			@Override
			public int size() {
				return array.size();
			}
		};
	}

	private <B> Map<String,B> mapToMap(final Type valueType, final Struct struct, boolean indirect) {
		return new AbstractMap<String, B>() {

			@Override
			public Set<Entry<String, B>> entrySet() {
				return new AbstractSet<Entry<String,B>>() {

					@Override
					public Iterator<Entry<String, B>> iterator() {
						return new Iterator<Entry<String, B>>() {
							final Iterator<String> delegate = struct.keySet().iterator();

							@Override
							public boolean hasNext() {
								return delegate.hasNext();
							}

							@SuppressWarnings("unchecked")
							@Override
							public Entry<String, B> next() {
								String key = delegate.next();
								return new SimpleImmutableEntry<String, B>(key,(B) convertFromStructValue(valueType, struct.get(key), indirect));
							}
							
							@Override
							public void remove() {
								delegate.remove();
							}
							
						};
					}

					@Override
					public void clear() {
						struct.keySet().clear();
					}
					
					@Override
					public int size() {
						return struct.keySet().size();
					}
				};
			}
			
			@Override
			public boolean containsKey(Object key) {
				return struct.keySet().contains(key);
			}
			
			@SuppressWarnings("unchecked")
			@Override
			public B get(Object key) {
				return (B) convertFromStructValue(valueType, struct.get((String)key), indirect);
			}
			
			@Override
			public B remove(Object key) {
				B result = get(key);
				struct.keySet().remove(key);
				return result;
			}
			
			@SuppressWarnings("unchecked")
			@Override
			public B put(String key, B value) {
				return (B) convertFromStructValue(valueType, struct.put(key, convertToStructValue(valueType, value, indirect)), indirect);
			}
		
		};
	}
	
	/**
	 * 
	 * @param declaredType The declared type of value
	 * @param value
	 * @return 
	 */
	private Object convertToStructValue(Type declaredType, Object value, boolean indirect) {
		
		if (value == null) 
			return null;
		
		Class<?> returnType = Object.class;
		if (declaredType instanceof Class) 
			returnType = (Class<?>)declaredType;
		if (declaredType instanceof ParameterizedType) 
			returnType = (Class<?>)((ParameterizedType)declaredType).getRawType();

		if (returnType.isArray()) {
			return mapFromArray(returnType.getComponentType(),(Object [])value, indirect);
		}

		if (List.class.isAssignableFrom(returnType)) {
			throw new RuntimeException("Cannot replace a synthetic collection. Please operate through the collections members.");
		}
		
		
		if (Map.class.isAssignableFrom(returnType)) {
			throw new RuntimeException("Cannot replace a synthetic collection. Please operate through the collections members.");
		}
		
		if (DocumentView.class.isAssignableFrom(returnType)) {
			Object ob = ((BuffDocument)((DocumentView)value).document()).root;
			if (indirect) {
				Reference rDoc = new BuffDocument().as(Reference.class);
				rDoc.ID(docStore.getID((DocumentView)value));
				ob = ((BuffDocument)(rDoc.document())).root;
			}
			return ob;
		}

		return returnType.cast(value);
	}
	
	private static final Object handleDefaultMethod(Object proxy, Method method, Object [] args, Class<?> documentClass) throws Throwable {
		// this can get very ugly between Java8 and Java 9+
		// for more detail see this
		// https://blog.jooq.org/2018/03/28/correct-reflective-access-to-interface-default-methods-in-java-8-9-10/
		Lookup lookup;
		try {
			Constructor<Lookup> constructor = Lookup.class.getDeclaredConstructor(Class.class);
			constructor.setAccessible(true);
			lookup = constructor.newInstance(documentClass);
		} catch (Exception e) {
			lookup = MethodHandles.lookup();
		}
		return 
			lookup
				.findSpecial(documentClass, method.getName(), MethodType.methodType(method.getReturnType(), method.getParameterTypes()), documentClass)
				.bindTo(proxy)
				.invokeWithArguments(args);
	}
	
	/**
	 * A document only equals another when the underlying root object is the same. If canonicalization is a goal, use Execute or CachingDocumentStore.
	 */
	@Override
	public boolean equals(Object obj) {
		return 
			super.equals(obj) ||
			( 
				BuffDocument.class.isAssignableFrom(obj.getClass()) && 
				root == ((BuffDocument)obj).root
			);
	}
	
	@Override
	public int hashCode() {
		return root.hashCode();
	}

	@Override
	public ByteBuffer toByteBuffer() {
		return root.toByteBuffer();
	}

	@Override
	public byte[] toBytes() {
		ByteBuffer buf = toByteBuffer();
		byte [] bytes = new byte[buf.limit()];
		buf.get(bytes,0,buf.limit());
		return bytes;
	}
}
