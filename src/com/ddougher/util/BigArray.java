package com.ddougher.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
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
	
	public interface AssetFactory {
		Addressable createAddressable();
		Sizeable createSizeable();
	}
	
	AssetFactory assetFactory;
	private static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate(0);
	
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
		
		public void swapPrevious(UUID mark) {
			previous.object.swap(object, mark);

			if (previous.size.get(mark).equals(BigInteger.ZERO)) 
				freeToCommon(previous,this,BigInteger.ONE.negate(), BigInteger.ONE);
			else if (size.get(mark).equals(BigInteger.ZERO))
				freeToCommon(previous,this,BigInteger.ONE, BigInteger.ONE.negate());

			BigInteger difference = previous.size.get(mark).subtract(size.get(mark));
			propagateToCommon(previous, this, difference.negate(), difference, mark);
		}
		
		public void swapNext(UUID mark) {
			next.object.swap(object, mark);

			if (next.size.get(mark).equals(BigInteger.ZERO)) 
				freeToCommon(next,this,BigInteger.ONE.negate(), BigInteger.ONE);
			else if (size.get(mark).equals(BigInteger.ZERO))
				freeToCommon(next,this,BigInteger.ONE, BigInteger.ONE.negate());

			BigInteger difference = next.size.get(mark).subtract(size.get(mark));
			propagateToCommon(next, this, difference.negate(), difference, mark);
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

	Node root; // After the first push, the root never changes
	Node head; // The head never changes
	
	// a pair of locks and the set of variables guarded by them
	private static final ReentrantReadWriteLock structureLock = new ReentrantReadWriteLock(true);
	private static final Object transactionLock = new Object();
	volatile UUID highWatermark;
	volatile BigInteger depth; // is the number of layers from root to leaf, inclusive. The number of leafs is 2^(depth-1)
	final int fragmentLimit = Integer.MAX_VALUE;

	public void dump() {
		StringBuilder sb = new StringBuilder();
		sb
			.append("HWM: " + highWatermark + "\n")
			.append("FRE: " + root.free + "\n")
			.append("DEP: " + depth + "\n")
			.append("ROOT\n" +root.dump("",1))
			.append("HEAD\n" + head.dump("",1));
		System.out.println(sb.toString());
	}
	
	
	
	/**
	 * @param assetFactory
	 */
	public BigArray(AssetFactory assetFactory) {
		super();
		this.assetFactory = assetFactory;
		highWatermark = TimeBasedUUIDGenerator.instance().nextUUID();
		depth = BigInteger.valueOf(2);
		root = new Node(null, assetFactory.createSizeable(), BigInteger.valueOf(2), null, null, null, null, null);
		head = new Node(assetFactory.createAddressable(), assetFactory.createSizeable(), BigInteger.ONE, root, null, null, null, null);
		head = new Node(assetFactory.createAddressable(), assetFactory.createSizeable(), BigInteger.ONE, root, null, null, null, head);
		root.left = head;
		root.right = head.next;
		push();
//		Thread t = new Thread(this::optimizeSpace);
//		t.setDaemon(true);
//		t.start();
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
	
	// in addition to the structure lock, there can only be one push active at a time
	private static final Object pushLock = new Object();
	private void push() {
		synchronized(pushLock) {
			for (Node active = head; active!=null; active = active.next.next) 
				active.push();
			transactionally(tid->{
				depth = depth.add(BigInteger.ONE);
				return null;
			});
		}
	}
	
	private void defragment() {
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
		});
		
		checkFreeSpace();
	}

	public final Object freeSpaceOptimizer = new Object();
	public int freeSpaceTicker = 0;
	
	/** If there is less than 20% free space then starts the space optimizer if it isnt already started */
	private void checkFreeSpace() {
		synchronized (freeSpaceOptimizer) {
			if (0==(freeSpaceTicker = (freeSpaceTicker+1)%100))
				freeSpaceOptimizer.notify();
		}
	}

	@SuppressWarnings("unused")
	private final void optimizeSpace() {
		//  ( 2^(depth-1) ) / freespace > 10 when there is less than 10% freespace
 		while (true) {
			try {
				BigInteger freeSpace = root.free;
	 			if (freeSpace.compareTo(BigInteger.ONE) <= 0 || BigInteger.valueOf(2).pow(depth.intValue()-1).divide(freeSpace).compareTo(BigInteger.valueOf(10))>0) {
	 				defragment();
	 			}
	 			if (freeSpace.compareTo(BigInteger.ONE) <= 0 || BigInteger.valueOf(2).pow(depth.intValue()-1).divide(freeSpace).compareTo(BigInteger.valueOf(10))>0) {
	 				push();
	 			}
	 			
	 			// optimize free space balance
	 			
	 			
	 			synchronized (freeSpaceOptimizer) {
						freeSpaceOptimizer.wait(1000);
	 			}
			} catch (Exception e) {
				e.printStackTrace(System.err);
			}
		}
	};

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
