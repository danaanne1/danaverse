package com.ddougher.remoting;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.security.ProtectionDomain;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * MT Safe version of a byte code snooper with opportunistic caching
 * 
 * @author Dana
 */
@SuppressWarnings("rawtypes")
public class ByteCodeExtractor implements ClassFileTransformer {

	private static class ByteCodeExtractorReference<T> extends WeakReference<T> {
		public Class cls;
		
		public ByteCodeExtractorReference(Class cls, T referent) {
			super(referent);
			this.cls = cls;
		}

		public ByteCodeExtractorReference(Class cls, T referent, ReferenceQueue<? super T> q) {
			super(referent, q);
			this.cls = cls;
		}
	}

	private static ReferenceQueue<CompletableFuture<byte[]>> queue = new ReferenceQueue<>();
	private static HashMap<Class, ByteCodeExtractorReference<CompletableFuture<byte[]>>> interest = new HashMap<>();
	private static Instrumentation instrumentation;
	private static boolean added = false;

	public static synchronized void agentmain(String args, Instrumentation inst) {
		instrumentation = inst;
		addSelf();
	}

	public static synchronized void premain(String args, Instrumentation inst) {
		instrumentation = inst;
		addSelf();
	}

	private static void addSelf() {
		if (!added) {
			instrumentation.addTransformer(new ByteCodeExtractor(), true);
			added = true;
		}
	}

	@SuppressWarnings("unchecked")
	public static byte[] getClassBytes(Class cls) throws UnmodifiableClassException, InterruptedException, ExecutionException {
		Instrumentation instrumentation = ByteCodeExtractor.instrumentation;
		if (instrumentation == null) {
			throw new IllegalStateException("Agent has not been loaded");
		}

		ByteCodeExtractorReference<CompletableFuture<byte[]>> ref, old;
		CompletableFuture<byte[]> result;
		synchronized (interest) {

			ref = interest.get(cls);
			if (ref == null || null == (result = ref.get())) {
				interest.put(cls, ref = new ByteCodeExtractorReference(cls, result = new CompletableFuture<byte[]>(), queue));
				instrumentation.retransformClasses(cls);
			}

			// parasitically clear anything expired that is still resident
			while (null!=(old=(ByteCodeExtractorReference<CompletableFuture<byte[]>>) queue.poll()))
				if (interest.get(old.cls)==old)
					interest.remove(old.cls);
		}

		return result.get();
	}

	@Override
	public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain pd,
			byte[] classFile) throws IllegalClassFormatException {

		if (classBeingRedefined == null)
			return null;

		CompletableFuture<byte[]> result = null;

		synchronized (interest) {
			ByteCodeExtractorReference<CompletableFuture<byte[]>> ref = interest.get(classBeingRedefined);
			if (ref != null)
				result = ref.get();
		}

		if (result != null)
			result.complete(classFile);
		
		return null;
	}
}
