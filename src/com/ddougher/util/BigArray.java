package com.ddougher.util;

import java.lang.ref.WeakReference;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.theunknowablebits.proxamic.TimeBasedUUIDGenerator;

/**
 * A Dynamically Resizeable 'Write Once Read Many' Unlimited Size Array
 * <p>
 * This structure meets the following tenets:
 * <ul>
 *  <li>Handles randomly sized blocks of bytes organized sequentially and accessable as a large array</li>
 * 	<li>Can handle mid insertion at a max cost of O*Log(n), where n is the block count</li>
 *  <li>Can handle O(n) linear iteration in either direction</li>
 *  <li>Can handle concurrent modification</li>
 *  <li>Can front a memory, memory mapped, disk, or cloud based storage subsystem</li>
 *  <li>Has an overhead cost of 100*2^(Log2(blockcount)+1))</li>
 *  <li>Is capable of transparent defragmentation</li>
 *  <li>Is capable of transparent consolidation to a desired block size</li>
 *  <li>Can self maintain to the optimal overhead size</li>
 *  </li>Can provide point in time traversals</li>
 * </ul>
 * <p>
 * Based on these tenets, a system could memory map a 2 tb array using 500mb blocks with 1mb of overhead.
 * @author Dana
 *
 */
/*
 * This works by maintaining a constant, cross linked tree structure with all nodes represented. 
 * This is slightly more expensive than a skip list, but much better balanced, capable of concurrent
 * modification (like: defragmentation, growth, and redistribution), and time travel  
 * 
 * Operations are structured as moves, swaps, splits, consolidations, and other mutations of the 
 * Addressable and size values of nodes, but the structure of the tree remains constant.
 * 
 * Transaction identifiers are represented by monoatomically increasing TimeStamp UUIDs, with the oldest (lowest)
 * number always taking precedence in the case of conflicts. (a committment ordering strategy) 
 * 
 * The Addressable provider is encouraged but not required to store object history... doing so is the basis
 * of the time travel feature. This also enables the transactional behavior, as the system can watermark 
 * for outstanding transactions.
 * 
 * This structure is a precursor to "engrams". (yes, i just borrowed from a "science fiction" novel)
 * 
 */
public class BigArray {

	public ConcurrentHashMap<String, AtomicLong> metrics = new ConcurrentHashMap<String, AtomicLong>();
	
	private <T> T withMetric(String baseName, Supplier<T> action) {
		long timestamp = System.nanoTime();
		T result = action.get();
		getMetric(baseName+".Time").addAndGet(System.nanoTime()-timestamp);
		getMetric(baseName+".Count").addAndGet(1);
		return result;
	};
	
	private void withMetric(String baseName, Runnable action) {
		withMetric(baseName, () -> {
			action.run();
			return null;
		});
	}
	
	public void dumpMetrics() {
		ArrayList<String> al = new ArrayList<String>();
		metrics.keySet().forEach(al::add);
		Collections.sort(al);
		al.forEach(k->{
			if (k.endsWith(".Time")) {
				System.out.println(k + " : " + ((double)metrics.get(k).get())/1000000);
				System.out.println(k.replace(".Time", ".Avg") + " : " + ((double)(metrics.get(k).get()/metrics.get(k.replace(".Time", ".Count")).get()))/1000000);
			} else {
				System.out.println(k + " : " + metrics.get(k).get());
			}
		});
	}
	
	private AtomicLong getMetric(String metricName) {
		AtomicLong l = metrics.get(metricName);
		if (l == null) {
			metrics.putIfAbsent(metricName, new AtomicLong());
			l = metrics.get(metricName);
		}
		return l;
	}
	
	public interface Optimizer {
		void addParticipant(BigArray participant);
	}

	public interface AssetFactory {
		Addressable createAddressable();
		Sizeable createSizeable();
	}

	public interface Sizeable {
		void set(BigInteger size, UUID mark);
		BigInteger get(UUID mark);
		Sizeable duplicate();
	}
	
	public interface Addressable {
		void set(ByteBuffer data, UUID mark);
		void set(Addressable src, UUID mark);
		void append(Addressable a, UUID mark);
		long size(UUID mark);
		ByteBuffer get(UUID mark);

		/** swaps a and b */
		default void swap(Addressable b, UUID mark) {
			ByteBuffer tmp = b.get(mark);
			b.set(get(mark), mark);
			set(tmp,mark);
		}
		
		/** changes the effective size of this addressable. Essentially the same as set(get(mark).duplicate().position(length).flip(),mark) */
		default void resize(int length, UUID mark) {
			set((ByteBuffer)get(mark).duplicate().position(length).flip(),mark);
		}
		/** Returns a new Addressable with a blank history that represents a subset of this addressable */
		Addressable slice(int offset, UUID mark);
	}

	private static final HashSet<WeakReference<BigArray>> optimizerParticipants = new HashSet<WeakReference<BigArray>>();
	private static final Object spaceLock = new Object();
	private static final Thread optimizerThread = new Thread(BigArray::optimizeSpaceLoop);
	private static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate(0);

	public static final int defaultFragmentLimit = 0;
	public static final Optimizer defaultOptimizer = p -> { synchronized(optimizerParticipants) { optimizerParticipants.add(new WeakReference<BigArray>(p)); } };

	static {
		optimizerThread.setDaemon(true);
		optimizerThread.start();
	}

	
	private final ReentrantReadWriteLock structureLock = new ReentrantReadWriteLock(true);
	private final Object transactionLock = new Object();
	private final Object pushLock = new Object();
	private final int fragmentLimit;
	
	private AssetFactory assetFactory;

	private Node root; // After the first push, the root never changes
	private Node head; // The head never changes
	private BigInteger nodeIdCounter = BigInteger.ZERO;
	
	private volatile UUID highWatermark;
	private volatile BigInteger depth; // is the number of layers from root to leaf, inclusive. The number of leafs is 2^(depth-1)

	public BigArray(AssetFactory assetFactory, Optimizer optimizer, int fragmentLimit) {
		super();
		this.assetFactory = assetFactory;
		this.fragmentLimit = fragmentLimit;
		highWatermark = TimeBasedUUIDGenerator.instance().nextUUID();
		depth = BigInteger.valueOf(2);
		root = new Node(null, assetFactory.createSizeable(), BigInteger.valueOf(2), null, null, null, null, null);
		head = new Node(assetFactory.createAddressable(), assetFactory.createSizeable(), BigInteger.ONE, root, null, null, null, null);
		head = new Node(assetFactory.createAddressable(), assetFactory.createSizeable(), BigInteger.ONE, root, null, null, null, head);
		root.left = head;
		root.right = head.next;
		push();
		optimizer.addParticipant(this); 
	}
	
	public BigArray(AssetFactory assetFactory) {
		this(assetFactory, defaultOptimizer, defaultFragmentLimit);
	}

	
	
	class Location {
		Node node;
		BigInteger offset;
		public Location(Node node, BigInteger offset) {
			super();
			this.node = node;
			this.offset = offset;
		}
	}

	class Node {
		final Addressable object;
		final Sizeable size;
		volatile BigInteger free;
		Node parent;
		Node left;
		Node right;
		Node previous;
		Node next;
		BigInteger id;
		public Node
		(
				Addressable object, 
				Sizeable size, 
				BigInteger free,
				Node parent,
				Node left, 
				Node right,
				Node previous,
				Node next
		) {
			super();
			this.object = object;
			this.size = size;
			this.parent = parent;
			this.left = left;
			this.right = right;
			this.previous = previous;
			this.next = next;
			this.free = free;
			this.id = (nodeIdCounter = nodeIdCounter.add(BigInteger.ONE));
		}

		/** Adds a new layer */
		public void push(UUID tid) {
			withMetric( "Node.Push", () -> {
				if (object == null) 
					throw new IllegalStateException("Push can only be called on leafs");
				Node newParent = new Node(null, size.duplicate(), free, parent, this, null, null, null );
				Node newSibling = new Node(assetFactory.createAddressable(), assetFactory.createSizeable(), BigInteger.ZERO ,newParent, null, null, this, next );
				if (parent.left == this) {
					parent.left = newParent;
				} else {
					parent.right = newParent;
				}
				parent = newParent;
				parent.right = newSibling;
				next = newSibling;
				if (newSibling.next!=null)
					newSibling.next.previous = newSibling;
				freeToParents(newSibling, BigInteger.ONE);
			});
		}
		
		/** Defragment's by appending and zeroing the next (if it would be under sizeLimit) */
		public boolean defragment(int sizeLimit) {
				if (object == null) 
					throw new IllegalStateException("Defrag can only be called on leafs");
				if (next == null)
					throw new IllegalStateException("Should not be called on tail node");
				return transactionally(tid->{
					return withMetric( "Node.Defragment", () -> {
						if (size.get(tid).equals(BigInteger.ZERO)||next.size.get(tid).equals(BigInteger.ZERO)) 
							return false;
						if (next.size.get(tid).longValue() + size.get(tid).longValue() <= sizeLimit ) {
							BigInteger difference = next.size.get(tid);
							object.append(next.object, tid);
							next.object.set(ZERO_BUFFER, tid);
							propagateToCommon(this, next, difference, difference.negate(), tid);
							freeToParents(next, BigInteger.ONE);
							return true;
						}
						return false;
					});
				});
		}
		
		public void swapPrevious(UUID tid) {
			withMetric( "Node.SwapPrevious", () -> {
				previous.object.swap(object, tid);
	
				if (previous.size.get(tid).equals(BigInteger.ZERO)) 
					freeToCommon(previous,this,BigInteger.ONE.negate(), BigInteger.ONE);
				if (size.get(tid).equals(BigInteger.ZERO))
					freeToCommon(previous,this,BigInteger.ONE, BigInteger.ONE.negate());
	
				BigInteger difference = previous.size.get(tid).subtract(size.get(tid));
				propagateToCommon(previous, this, difference.negate(), difference, tid);
			});
		}
		
		public void swapNext(UUID tid) {
			withMetric( "Node.SwapNext", () -> {
				next.object.swap(object, tid);
	
				if (next.size.get(tid).equals(BigInteger.ZERO)) 
					freeToCommon(next,this,BigInteger.ONE.negate(), BigInteger.ONE);
				if (size.get(tid).equals(BigInteger.ZERO))
					freeToCommon(next,this,BigInteger.ONE, BigInteger.ONE.negate());
	
				BigInteger difference = next.size.get(tid).subtract(size.get(tid));
				propagateToCommon(next, this, difference.negate(), difference, tid);
			});
		}
		
		public String dump(String prefix, int depth) {
			return withMetric( "Node.Dump", () -> {
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < depth; i++)
					sb.append("    ");
				String space = sb.toString();
				sb = new StringBuilder();
				sb
					.append(space+prefix+id)
					.append(" { ")
					.append("s: "+size.get(highWatermark))
					.append(", ")
					.append("f: "+free)
					.append(", ")
					.append("p: "+(previous!=null?previous.id:"-"))
					.append(", ")
					.append("n: "+(next!=null?next.id:"-"))
					.append(", ")
					.append("u: "+(parent!=null?parent.id:"-"))
					.append(", ")
					.append("o: "+object)
					.append("}\n");
				
				if (left != null)
					sb.append(left.dump("L: ",depth+1));
				if (right != null)
					sb.append(right.dump("R: ",depth+1));
				
				return sb.toString();
			});
		}	
	}

	public String dump() {
		return transactionally(()->{
			return withMetric("BigArray.Dump", () -> {
				StringBuilder sb = new StringBuilder();
				sb
					.append("HWM: " + highWatermark + "\n")
					.append("FRE: " + root.free + "\n")
					.append("DEP: " + depth + "\n")
					.append("ROOT\n" +root.dump("",1))
					.append("HEAD\n" + head.dump("",1));
				return sb.toString();
			});
		});
	}
	
	private LinkedHashMap<String,Runnable> postTransactionOperations = new LinkedHashMap<>();
	
	private <T> T transactionally(Supplier<T> transaction) {
		return withMetric("BigArray.Transaction.Outter", () -> {
			LinkedList<Runnable> pops = new LinkedList<Runnable>();
			structureLock.readLock().lock();
			AtomicReference<T> ref = new AtomicReference<T>();
			try {
				synchronized(transactionLock) {
					withMetric("BigArray.Transaction.Inner", () -> {
						ref.set(transaction.get());
					});
					pops.addAll(postTransactionOperations.values());
					postTransactionOperations.clear();
				}
			} finally {
				structureLock.readLock().unlock();
			}
			pops.forEach(a->a.run());
			return ref.get();
		});
	}
	
	@SuppressWarnings("unused")
	private void transactionally(Runnable transaction) {
		transactionally(()->{
			transaction.run();
			return null;
		});
	}
	
	/**
	 * invoked anytime content structure is subject to change
	 * @param transaction
	 */
	private <T> T transactionally(Function<UUID, T> transaction) {
		return transactionally(() -> {
			UUID transactionId = TimeBasedUUIDGenerator.instance().nextUUID();
			T result = transaction.apply(transactionId);
			highWatermark = transactionId;
			return result;
		});
	}			
	
	private void transactionally(Consumer<UUID> transaction) {
		transactionally(tid->{
			transaction.accept(tid);
			return null;
		});
	}
	
	/**
	 * invoked anytime tree structure is subject to change
	 * @param transaction
	 */
	private <T> T structurally(Function<UUID, T> transaction) {
		return withMetric("BigArray.Structurally.Outter", () -> {
			structureLock.writeLock().lock();
			try {
				return withMetric("BigArray.Structurally.Inner", () -> {
					return transactionally(transaction);
				});
			} finally {
				structureLock.writeLock().unlock();
			}
		});
	}
	
	private void structurally(Consumer<UUID> transaction) {
		structurally(tid->{
			transaction.accept(tid);
			return null;
		});
	}
	
	protected void push() {
		synchronized(pushLock) {
			structurally(tid->{
				withMetric("BigArray.Push", () -> {
					for (Node active = head; active!=null; active = active.next.next) 
						active.push(tid);
					depth = depth.add(BigInteger.ONE);
				});
			});
		}
	}
	
	protected void defragment() {
		withMetric("BigArray.Defragment", () -> {
			if (fragmentLimit == 0)
				return;
			for (Node active = head; active != null; active = active.next.next) 
				active.defragment(fragmentLimit);
		});
	}
	
	private Location locate(BigInteger off, UUID tid) {
		return withMetric("BigArray.Locate", () -> {
			Node active = root;
			BigInteger offset = off;
			while (active.object == null) {
				if (active.left.size.get(tid).compareTo(offset) >= 0) {
					active = active.left;
				} else {
					offset = offset.subtract(active.left.size.get(tid));
					active = active.right;
				}
			}
			return new Location(active,offset);
		});
	}
	
	/**
	 * Inserts a block of data at the given offset.
	 * 
	 * @param data
	 * .
	 * @param offset
	 * @param length
	 */
	public void insert(final ByteBuffer data, BigInteger offset) {
		int length = data.limit();
		
		// handle emergency push:
		if (root.free.compareTo(BigInteger.valueOf(2))<=0)
			push();

		transactionally(tid->{
			withMetric("BigArray.insert", () -> {
				Location l = locate(offset,tid);
				Node target = null;
				if (l.offset.equals(BigInteger.ZERO)) {
					// left insert
					target = makeSpace(l.node, tid);
				} else if (l.offset.equals(l.node.size.get(tid))) {
					// tail insert steals space 
					target = makeSpace(l.node, tid);
					target.swapNext(tid);
					target = target.next;
				} else  {
					// split insert
					target = makeSpace(split(l.node, l.offset.intValue() ,tid), tid);
				}
	
				// target is now a 0 in the appropriate position:
				target.object.set((ByteBuffer)data.duplicate().position(length).flip(), tid);
	
				propagateToParents(target, BigInteger.valueOf(length).subtract(target.size.get(tid)), tid);
	
				freeToParents(target, BigInteger.ONE.negate());
	
				spaceCheck(target, tid);
			});
		});
	}

	/** While busy, keep the space optimizer awake */
	private void spaceCheck(Node target, UUID tid) {
		withMetric("BigArray.SpaceCheck", () -> {
			synchronized (spaceLock) {
				spaceLock.notify();
			}
		});
	}

	@SuppressWarnings({ "unchecked" })
	private static final void optimizeSpaceLoop() {
 		while (true) {
 			try {
	 			Set<WeakReference<BigArray>> candidates;
	 			synchronized(optimizerParticipants) { candidates = (Set<WeakReference<BigArray>>)optimizerParticipants.clone(); }
	 			boolean sleep = true;
				for(WeakReference<BigArray> ref: candidates) {
					BigArray b = ref.get();
					if (b==null) {
						synchronized(optimizerParticipants) { optimizerParticipants.remove(ref); }
						continue;
					}
					sleep &= b.optimizeSpace();

				}
				if (sleep) {
					synchronized(spaceLock) {
						spaceLock.wait(1000);
					}
				}
			} catch (Exception ie) {
				
				ie.printStackTrace();
			}
 		}
	}
	
	private volatile boolean forceOptimizeFlag = false;
	
	private void forceOptimize() {
		if (forceOptimizeFlag) return;
		forceOptimizeFlag = true;
		postTransactionOperations.put("forceOptimize", () -> { optimizeSpace(); });
	}

	/**
	 * Runs full space optimization: Defragment, followed by push, followed by a spacewalk.
	 * 
	 * @return true if the spacewalk queue is empty
	 */
	protected boolean optimizeSpace() {
		//  ( 2^(depth-1) ) / freespace > 10 when there is less than 10% freespace
		return withMetric("BigArray.OptimizeSpace", () -> {
			BigInteger freeSpace = root.free;
			if (forceOptimizeFlag || freeSpace.compareTo(BigInteger.ONE) <= 0 || BigInteger.valueOf(2).pow(depth.intValue()-1).divide(freeSpace).compareTo(BigInteger.valueOf(10))>0) {
				defragment();
			}
			if (forceOptimizeFlag || freeSpace.compareTo(BigInteger.ONE) <= 0 || BigInteger.valueOf(2).pow(depth.intValue()-1).divide(freeSpace).compareTo(BigInteger.valueOf(10))>0) {
				push();
			}
			forceOptimizeFlag = false;
			
			return spaceWalk();
		});
	};
	

	/**
	 * Never has there been a more appropriate method name. This literally finds the high density spaces
	 * and walks them over to the low space density area.
	 * 
	 * @return true if the spacewalk queue is empty
	 */
	protected boolean spaceWalk() {
		return transactionally(tid->{
			return withMetric("BigArray.SpaceWalk", () -> {
				return false;
			});
		});
	}

	private void propagateToCommon(Node a, Node b, BigInteger diffa, BigInteger diffb, UUID tid) {
		while (a!=b) {
			if (a!=null) {
				a.size.set(a.size.get(tid).add(diffa), tid);
				a = a.parent;
			}
			if (b!=null) {
				b.size.set(b.size.get(tid).add(diffb), tid);
				b = b.parent;
			}
		}
	}
	
	private void freeToCommon(Node a, Node b, BigInteger diffa, BigInteger diffb) {
		while (a!=b) {
			if (a!=null) {
				a.free = a.free.add(diffa);
				a = a.parent;
			}
			if (b!=null) {
				b.free = b.free.add(diffb);
				b = b.parent;
			}
		}
	}

	private void propagateToParents(Node target, BigInteger difference, UUID tid) {
		for (; target!= null; target = target.parent) 
			target.size.set(target.size.get(tid).add(difference), tid);
	}

	private void freeToParents(Node target, BigInteger difference) {
		for (; target != null; target = target.parent)
			target.free = target.free.add(difference);
	}
	
	private Node makeSpace( Node active, UUID tid) {
		// find the closest space
		Node forwards = active;
		Node backwards = active;
		while (!forwards.size.get(tid).equals(BigInteger.ZERO) && !backwards.size.get(tid).equals(BigInteger.ZERO)) {
			if (forwards.next!=null)
				forwards = forwards.next;
			if (backwards.previous!=null)
				backwards = backwards.previous;
		}
		
		// this actually bubbles backwards from the found node until the current node (or its previous) is a zero 
		long distance = 0;
		Node result;
		if (forwards.size.get(tid).equals(BigInteger.ZERO)) {
			while (forwards != active) {
				forwards.swapPrevious(tid);
				forwards = forwards.previous;
				distance += 1;
			}
			result = forwards;
		} else {
			while (backwards.next != active) {
				backwards.swapNext(tid);
				backwards = backwards.next;
				distance += 1;
			}
			result =  backwards;
		}
		if (distance > 20) 
			forceOptimize();
		return result;
	}

	private Node split(Node node, int offset, UUID tid) {
		Addressable slice = node.object.slice(offset, tid);
		node.object.resize(offset, tid);
		BigInteger difference = BigInteger.valueOf(offset).subtract(node.size.get(tid));
		propagateToParents(node, difference, tid);
		
		
		Node target = makeSpace(node, tid);
		target.swapNext(tid); //move the space to the right
		target = target.next;
		target.object.set(slice, tid);
		propagateToParents(target, difference.negate(), tid);
		freeToParents(target, BigInteger.ONE.negate());
		return target;
	}
	
	public BigInteger size() {
		return size(null);
	}

	public BigInteger size(UUID tid) {
		return transactionally(()-> { return root.size.get(tid!=null?tid:highWatermark); }  );
	}
	
	public void remove(BigInteger offset, BigInteger length ) {
		// handle emergency push:
		if (root.free.compareTo(BigInteger.valueOf(2))<=0)
			push();

		transactionally(tid->{
			BigInteger lengthRemaining = length;
			Location target = locate(offset, tid);
			
			// hunt off of zero / tail location
			if (target.node.size.get(tid).equals(target.offset)) {
				target.node = target.node.next;
				while(target.node.size.get(tid).equals(BigInteger.ZERO))
					target.node = target.node.next;
				target.offset = BigInteger.ZERO;
			}
			
			// handle initial split:
			if (!target.offset.equals(BigInteger.ZERO) && target.offset.compareTo(target.node.size.get(tid))<0) {
				target.node = split(target.node, target.offset.intValue(), tid);
				target.offset = BigInteger.ZERO;
			}
			
			// handle the full boat inbetween nodes:
			while (target.node != null && lengthRemaining.compareTo(target.node.size.get(tid))>=0) {
				if (!target.node.size.get(tid).equals(BigInteger.ZERO)) {
					lengthRemaining = lengthRemaining.subtract(target.node.size.get(tid));
					target.node.object.set(ZERO_BUFFER, tid);
					propagateToParents(target.node, target.node.size.get(tid).negate(), tid);
					freeToParents(target.node, BigInteger.ONE);
				}
				target.node = target.node.next;
			}

			// handle the tail split:
			if (target.node != null && lengthRemaining.compareTo(BigInteger.ZERO)>0) {
				target.node = split(target.node,lengthRemaining.intValue(),tid);
				target.node = target.node.previous;
				target.node.object.set(ZERO_BUFFER, tid);
				propagateToParents(target.node, target.node.size.get(tid).negate(), tid);
				freeToParents(target.node, BigInteger.ONE);
			}
		});
		
	}

	public Iterator<ByteBuffer> get(BigInteger offset, BigInteger length) {
		return get(offset, length, highWatermark);
	}
	
	public Iterator<ByteBuffer> get(BigInteger offset, BigInteger length, UUID tid) {
		
		if (transactionally(()->{ return length.add(offset).compareTo(root.size.get(tid)); })>0)
			throw new IndexOutOfBoundsException();
		
		return new Iterator<ByteBuffer>() {
			BigInteger remainingOffset = offset;
			BigInteger remainingLength = length;
			ByteBuffer pending = moveToNextPiece();
			Node lastNode = null;

			@Override
			public synchronized boolean hasNext() {
				return pending != null;
			}

			@Override
			public synchronized ByteBuffer next() {
				ByteBuffer result = pending;
				if (pending != null)
					moveToNextPiece();
				return result;
			}
			
			private synchronized ByteBuffer moveToNextPiece() {
				if (remainingLength.compareTo(BigInteger.ZERO)<=0) {
					pending = null;
					return null;
				}
				return transactionally(()->{
					Location target = new Location(lastNode, BigInteger.ZERO);
					if (lastNode != null && tid.equals(highWatermark)) { // shortcut if nothing has changed
						target.node = target.node.next;
						while (target.node != null && target.node.size.get(tid).equals(BigInteger.ZERO))
							target.node = target.node.next;
					} else {
						target = locate(remainingOffset, tid);
					}
					if (target.offset.equals(target.node.size.get(tid))) { // this will still work if start is 0 and head is a space
						target.node = target.node.next;
						while (target.node != null && target.node.size.get(tid).equals(BigInteger.ZERO))
							target.node = target.node.next;
						target.offset = BigInteger.ZERO;
					} 
					if (target.node == null) {
						pending = null;
						return null;
					}	
					BigInteger takeLength = target.node.size.get(tid).subtract(target.offset).min(remainingLength);
					ByteBuffer result = (ByteBuffer)target.node.object.get(tid).duplicate().position(target.offset.intValue());
					if (result.position()!=0) 
						result = result.slice();
					result = (ByteBuffer)result.limit(takeLength.intValue());
					remainingOffset = remainingOffset.add(takeLength);
					remainingLength = remainingLength.subtract(takeLength);
					lastNode = target.node;
					pending = result;
					return result;
				});
			}
			
		};
		
	}
	
	
	
}
