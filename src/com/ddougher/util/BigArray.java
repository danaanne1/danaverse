package com.ddougher.util;

import java.lang.ref.WeakReference;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

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

	private static final BigInteger defaultOptimizerThreshold = BigInteger.valueOf(2);
	private static final int defaultFragmentLimit = Integer.MAX_VALUE;
	private static final HashSet<WeakReference<BigArray>> optimizerParticipants = new HashSet<WeakReference<BigArray>>();
	private static final Optimizer defaultOptimizer = p -> { synchronized(optimizerParticipants) { optimizerParticipants.add(new WeakReference<BigArray>(p)); } };
	private static final Object spaceLock = new Object();
	private static final Thread optimizerThread = new Thread(BigArray::optimizeSpaceLoop);
	private static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate(0);

	static {
		optimizerThread.setDaemon(true);
		optimizerThread.start();
	}

	
	private final ReentrantReadWriteLock structureLock = new ReentrantReadWriteLock(true);
	private final Object transactionLock = new Object();
	private final Object pushLock = new Object();
	private final int fragmentLimit;
	private final BigInteger optimizerThreshold;
	private final Set<Node> spaceWalkQueue = Collections.synchronizedSet(new LinkedHashSet<>());

	private AssetFactory assetFactory;

	private Node root; // After the first push, the root never changes
	private Node head; // The head never changes
	
	private volatile UUID highWatermark;
	private volatile BigInteger depth; // is the number of layers from root to leaf, inclusive. The number of leafs is 2^(depth-1)

	public BigArray(AssetFactory assetFactory, Optimizer optimizer, int fragmentLimit, BigInteger optimizerThreshold) {
		super();
		this.assetFactory = assetFactory;
		this.fragmentLimit = fragmentLimit;
		this.optimizerThreshold = optimizerThreshold;
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
		this(assetFactory, defaultOptimizer, defaultFragmentLimit, defaultOptimizerThreshold);
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
		}

		/** Adds a new layer */
		public void push() {
			if (object == null) 
				throw new IllegalStateException("Push can only be called on leafs");
			Node newParent = new Node(null, size.duplicate(), free, parent, this, null, null, null );
			Node newSibling = new Node(assetFactory.createAddressable(), assetFactory.createSizeable(), BigInteger.ZERO ,newParent, null, null, this, next );
			structurally(tid->{
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
				if (next.object.size(tid)==0) 
					return false;
				if ((next.object.size(tid) + object.size(tid)) <= sizeLimit ) {
					BigInteger difference = next.size.get(tid);
					object.append(next.object, tid);
					next.object.set(ZERO_BUFFER, tid);
					propagateToCommon(this, next, difference, difference.negate(), tid);
					freeToParents(next, BigInteger.ONE);
					return true;
				}
				return false;
			});
		}
		
		public void swapPrevious(UUID tid) {
			previous.object.swap(object, tid);

			if (previous.size.get(tid).equals(BigInteger.ZERO)) 
				freeToCommon(previous,this,BigInteger.ONE.negate(), BigInteger.ONE);
			if (size.get(tid).equals(BigInteger.ZERO))
				freeToCommon(previous,this,BigInteger.ONE, BigInteger.ONE.negate());

			BigInteger difference = previous.size.get(tid).subtract(size.get(tid));
			propagateToCommon(previous, this, difference.negate(), difference, tid);
		}
		
		public void swapNext(UUID tid) {
			next.object.swap(object, tid);

			if (next.size.get(tid).equals(BigInteger.ZERO)) 
				freeToCommon(next,this,BigInteger.ONE.negate(), BigInteger.ONE);
			if (size.get(tid).equals(BigInteger.ZERO))
				freeToCommon(next,this,BigInteger.ONE, BigInteger.ONE.negate());

			BigInteger difference = next.size.get(tid).subtract(size.get(tid));
			propagateToCommon(next, this, difference.negate(), difference, tid);
		}
		
		public String dump(String prefix, int depth) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < depth; i++)
				sb.append("    ");
			String space = sb.toString();
			sb = new StringBuilder();
			sb
				.append(space+prefix+System.identityHashCode(this))
				.append("( ")
				.append("s: "+size.get(highWatermark))
				.append(", ")
				.append("f: "+free)
				.append(", ")
				.append("p: "+System.identityHashCode(previous))
				.append(", ")
				.append("n: "+System.identityHashCode(next))
				.append(", ")
				.append("u: "+System.identityHashCode(parent))
				.append(", ")
				.append("o: "+object)
				.append(")\n");
			
			if (left != null)
				sb.append(left.dump("l:",depth+1));
			if (right != null)
				sb.append(right.dump("r:",depth+1));
			
			return sb.toString();
		}	
	}

	public void dump() {
		transactionally(tid->{
			StringBuilder sb = new StringBuilder();
			sb
				.append("HWM: " + highWatermark + "\n")
				.append("FRE: " + root.free + "\n")
				.append("DEP: " + depth + "\n")
				.append("ROOT\n" +root.dump("",1))
				.append("HEAD\n" + head.dump("",1));
			System.out.println(sb.toString());
		});
	}
	
	
	/**
	 * invoked anytime content structure is subject to change
	 * @param transaction
	 */
	private <T> T transactionally(Function<UUID, T> transaction) {
		structureLock.readLock().lock();
		try {
			synchronized(transactionLock) {
				UUID transactionId = TimeBasedUUIDGenerator.instance().nextUUID();
				T result = transaction.apply(transactionId);
				highWatermark = transactionId;
				return result;
			}
		} finally {
			structureLock.readLock().unlock();
		}
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
		structureLock.writeLock().lock();
		try {
			return transactionally(transaction);
		} finally {
			structureLock.writeLock().unlock();
		}
	}
	
	private void structurally(Consumer<UUID> transaction) {
		structurally(tid->{
			transaction.accept(tid);
			return null;
		});
	}
	
	protected void push() {
		synchronized(pushLock) {
			for (Node active = head; active!=null; active = active.next.next) 
				active.push();
			transactionally(tid->{
				depth = depth.add(BigInteger.ONE);
				return null;
			});
		}
	}
	
	protected void defragment() {
		for (Node active = head; active != null; active = active.next.next) 
			active.defragment(fragmentLimit);
	}
	
	private Location locate(BigInteger offset, UUID atMark) {
		Node active = root;
		while (active.object == null) {
			if (active.size.get(atMark).compareTo(offset) < 0) {
				offset = offset.subtract(active.size.get(atMark));
				active = active.right;
			} else {
				active = active.left;
			}
		}
		return new Location(active,offset);
	}
	
	/**
	 * Inserts a block of data at the given offset.
	 * 
	 * @param data
	 * .
	 * @param offset
	 * @param length
	 */
	public void insert(ByteBuffer data, BigInteger offset) {
		int length = data.limit();
		
		// handle emergency push:
		if (root.free.compareTo(BigInteger.valueOf(2))<=0)
			push();

		transactionally(tid->{
			Location l = locate(offset,tid);
			if (!l.offset.equals(BigInteger.ZERO)) {
				split(l.node, l.offset.intValue() ,tid);
			}
			// the addressable may have moved post split:
			l = locate(offset,tid);
			
			Node target = makeSpace(l.node, tid);
			
			// now that current node is 0, do insert:
			target.object.set((ByteBuffer)data.duplicate().position(length).flip(), tid);
			// and propagate size to root:
			propagateToParents(target, BigInteger.valueOf(length).subtract(target.size.get(tid)), tid);

			freeToParents(target, BigInteger.ONE.negate());

			spaceCheck(target, tid);
		});
	}

	
	/** While busy, keep the space optimizer awake */
	private void spaceCheck(Node target, UUID tid) {

		preBalance(target, tid);

		synchronized (spaceLock) {
			spaceLock.notify();
		}
	}

	/*
	 * Eventually this will get replaced with an "Optimizer" constructor parameter to allow for different pooling options.
	 */
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

	// VisibleForTesting
	protected boolean optimizeSpace() {
		//  ( 2^(depth-1) ) / freespace > 10 when there is less than 10% freespace
		BigInteger freeSpace = root.free;
		if (freeSpace.compareTo(BigInteger.ONE) <= 0 || BigInteger.valueOf(2).pow(depth.intValue()-1).divide(freeSpace).compareTo(BigInteger.valueOf(10))>0) {
			defragment();
		}
		if (freeSpace.compareTo(BigInteger.ONE) <= 0 || BigInteger.valueOf(2).pow(depth.intValue()-1).divide(freeSpace).compareTo(BigInteger.valueOf(10))>0) {
			push();
		}

		return spaceWalk();
	};
	
	private void preBalance(Node node, UUID tid) {
		Node current = node;
		Node winner = current.parent;
		BigInteger largestDifference = BigInteger.ZERO;
		while (current.parent != null) {
			current = current.parent;
			BigInteger difference = current.left.free.subtract(current.right.free);
			if (difference.abs().compareTo(largestDifference.abs()) > 0) {
				winner = current;
				largestDifference = difference;
			}
		}
		if (largestDifference.abs().compareTo(optimizerThreshold)>0) { // difference is > FREE_OFFSET
			spaceWalkQueue.add(winner);
		}
	}

	/**
	 * Never has there been a more appropriate method name. This literally finds the high density spaces
	 * and walks them over to the low space density area.
	 * 
	 * @return true if the spacewalk queue is empty
	 */
	private boolean spaceWalk() {
		return transactionally(tid->{
			if (spaceWalkQueue.isEmpty())
				return true;

			Node node;
			synchronized (spaceWalkQueue) {
				node = spaceWalkQueue.iterator().next();
			}
			Node source;
			Node destination;
			BigInteger difference = node.left.free.subtract(node.right.free);
			if (difference.abs().compareTo(optimizerThreshold)<=0) { // difference is <= FREE_OFFSET
				spaceWalkQueue.remove(node);
			} else {
				System.out.println("SPACE");
				if (difference.compareTo(BigInteger.ZERO)<0) { 	
					// right side has more space:
					source = leftFreeDescent(node.right);
					destination = rightBusyDescent(node.left);
					for (Node n = source; n != destination; n = n.previous)
						n.swapPrevious(tid);
				} else { 
					// left side has more space:
					source = rightFreeDescent(node.left);
					destination = leftBusyDescent(node.right);
					for (Node n = source; n != destination; n = n.next)
						n.swapNext(tid);
				}
			}
			return spaceWalkQueue.isEmpty();
		});
	}

	private Node rightFreeDescent(Node node) {
		if (node.object != null)
			return node;
		if (node.left.free.compareTo(node.right.free)>0)
			return rightFreeDescent(node.left);
		return rightFreeDescent(node.right);
	}
	
	private Node rightBusyDescent(Node node) {
		if (node.object != null)
			return node;
		if (node.left.free.compareTo(node.right.free)<0)
			return rightBusyDescent(node.left);
		return rightBusyDescent(node.right);
	}

	private Node leftFreeDescent(Node node) {
		if (node.object != null)
			return node;
		if (node.right.free.compareTo(node.left.free)>0)
			return leftFreeDescent(node.right);
		return leftFreeDescent(node.left);
	}

	private Node leftBusyDescent(Node node) {
		if (node.object != null)
			return node;
		if (node.right.free.compareTo(node.left.free)<0)
			return leftBusyDescent(node.right);
		return leftBusyDescent(node.left);
	}
	
	private void propagateToCommon(Node a, Node b, BigInteger diffa, BigInteger diffb, UUID tid) {
		while (a!=b) {
			a.size.set(a.size.get(tid).add(diffa), tid);
			b.size.set(b.size.get(tid).add(diffb), tid);
			a = a.parent;
			b = b.parent;
		}
	}
	
	private void freeToCommon(Node a, Node b, BigInteger diffa, BigInteger diffb) {
		while (a!=b) {
			a.free = a.free.add(diffa);
			b.free = b.free.add(diffb);
			a = a.parent;
			b = b.parent;
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
		if (forwards.size.get(tid).equals(BigInteger.ZERO)) {
			while (forwards != active) {
				forwards.swapPrevious(tid);
				forwards = forwards.previous;
			}
			return forwards;
		} else {
			while (backwards.next != active) {
				backwards.swapNext(tid);
				backwards = backwards.next;
			}
			return backwards;
		}
	}

	private void split(Node node, int offset, UUID tid) {
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
	}

	public void remove(BigInteger offset, BigInteger length ) {
		
	}

	public Iterator<Addressable> get(BigInteger offset, long length) {
		
		return null;
	}
	
	
	
}
