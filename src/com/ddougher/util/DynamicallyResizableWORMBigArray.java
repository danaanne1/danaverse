package com.ddougher.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

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
 */
public class DynamicallyResizableWORMBigArray {

	public interface Sizeable {
		void set(BigInteger size, UUID mark);
		BigInteger get(UUID mark);
		Sizeable clone();
	}
	
	public interface Addressable {
		void set(ByteBuffer data, UUID mark);
		void append(Addressable a, UUID mark);
		long size(UUID mark);
		ByteBuffer get(UUID effectiveDate);
	}
	
	public interface AssetFactory {
		Addressable createAddressable();
		Sizeable createSizeable();
	}
	
	AssetFactory assetFactory;

	class Node {
		Addressable object;
		Sizeable size;
		Node parent;
		Node left;
		Node right;
		Node previous;
		Node next;
		public Node
		(
				Addressable object, 
				Sizeable size, 
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
		}

		/** Adds a new layer */
		public void push() {
			if (object == null) 
				throw new IllegalStateException("Push can only be called on leafs");
			Node newParent = new Node(null, size.clone(), parent, this, null, null, null );
			Node newSibling = new Node(assetFactory.createAddressable(), assetFactory.createSizeable(), newParent, null, null, this, next );
			transactionally((tid)->{
				if (parent.left == this) {
					parent.left = newParent;
				} else {
					parent.right = newParent;
				}
				parent = newParent;
				parent.right = newSibling;
				next = newSibling;
				freeSpace.add(BigInteger.ONE);
			});
		}
		
		/** Defragment's by appending and zeroing the next (if it would be under sizeLimit) */
		public void defragment(long sizeLimit) {
			if (object == null) 
				throw new IllegalStateException("Defrag can only be called on leafs");
			if (next == null)
				throw new IllegalStateException("Defrag should not be called on right nodes");
			transactionally((tid)->{
				if ((next.object.size(tid) + object.size(tid)) <= sizeLimit ) {
					object.append(next.object, tid);
					next.object.set(ByteBuffer.allocate(0), tid);
					freeSpace.add(BigInteger.ONE);
				}
			});
		}
		
	}

	Node root; // The root never changes
	Node head; // The head never changes
	
	// a lock and the set of variables guarded by it
	private static final Object transactionLock = new Object();
	UUID highWatermark;
	BigInteger freeSpace;
	BigInteger depth;
	long sizeLimit;

	private void transactionally(Consumer<UUID> transaction) {
		synchronized(transactionLock) {
			UUID transactionId = TimeBasedUUIDGenerator.instance().nextUUID();
			transaction.accept(transactionId);
			highWatermark = transactionId;
		}
	}
	
	// there can only be one push active at a time
	private static final Object pushLock = new Object();
	private void push() {
		synchronized(pushLock) {
			for (Node active = head; active!=null; active = active.next.next) 
				active.push();
			transactionally((tid)->{
				depth.add(BigInteger.ONE);
			});
		}
	}
	
	private void defragment() {
		for (Node active = head; active != null; active = active.next.next) 
			active.defragment(sizeLimit);
	}
	
	public void insert(ByteBuffer data, BigInteger offset, long length) {
		
	}

	public void remove(BigInteger offset, BigInteger length ) {
		
	}

	public Iterator<Addressable> get(BigInteger offset, long length) {
		
		return null;
	}
	
	
	
}
