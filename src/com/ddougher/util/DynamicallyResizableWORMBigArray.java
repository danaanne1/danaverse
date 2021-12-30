package com.ddougher.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

/**
 * A Dynamically Resizeable 'Write Once Read Many' Unlimited Size Array
 * <p>
 * This structure meets the following tenets:
 * <ul>
 *  <li>Handles randomly sized blocks of bytes organized sequentially and accessable as a large array</li>
 * 	<li>Can handle mid insertion at a max cost of O*Log(n), where n is the block count</li>
 *  <li>Can handle O(n) linear iteration in either direction</li>
 *  <li>Can handle concurrent modification and log structured transactions with the support of an external coordinator</li>
 *  <li>Can front a memory, memory mapped, disk, or cloud based storage subsystem</li>
 *  <li>Has an overhead cost of 100*2^(Log2(blockcount)+1))</li>
 *  <li>Is capable of transparent defragmentation</li>
 *  <li>Is capable of transparent consolidation to a desired block size</li>
 *  <li>Can self maintain to the optimal overhead size</li>
 * </ul>
 * <p>
 * Based on these tenets, a system could memory map a 2 tb array using 500mb blocks with 1mb of overhead.
 * @author Dana
 *
 */
public class DynamicallyResizableWORMBigArray {

	public interface Addressable {
		ByteBuffer get(long offset, long length);
		Addressable slice(long offset, long length);
		long size();
	}

	public interface AddressableFactory {
		Addressable createAddressable(ByteBuffer data, long length);
		Addressable consolidate(Addressable a, Addressable b);
		public void deleteAddressable(Addressable a);
	}
	
	/*
	 * Unlimited cosmic power cost approximately 100 bytes per node.
	 */
	class Node {
		Addressable object;
		BigInteger size;
		Node parent;
		Node left;
		Node right;
		Node previous;
		Node next;
		UUID lockOwner;
		Long lockReleaseTime;
		public Node
		(
				Addressable object, 
				BigInteger size, 
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
			this.lockOwner = null;
			this.lockReleaseTime = Long.MIN_VALUE;
		}

		/** 
		 * Consolidates the right addressable into the left addressable. Returns true if the result is over targetSize.
		 *  
		 * @param targetSize
		 */
		public boolean consolidate(long targetSize) {
			if (left == null) {
				// just flip left and right:
				left = right;
				right = null;
				
			} else if (right != null) {
				
				if (!right.size.equals(BigInteger.ZERO)) {
					if (left.size.equals(BigInteger.ZERO)) {
						// short cut:
						left.object = right.object;
						left.size = right.size;
						right.object = addressableFactory.createAddressable(ByteBuffer.allocate(0), 0);
						right.size = BigInteger.ZERO;
					} else {
						// long way:
						Addressable oldRight = right.object;
						Addressable oldLeft = left.object;
						left.object = addressableFactory.consolidate(oldLeft, oldRight);
						left.size = BigInteger.valueOf(left.object.size());
						right.object = addressableFactory.createAddressable(ByteBuffer.allocate(0), 0);
						right.size = BigInteger.ZERO;
						addressableFactory.deleteAddressable(oldLeft);
						addressableFactory.deleteAddressable(oldRight);
					}
				}
			}

			if (left != null)
				return left.size.compareTo(BigInteger.valueOf(targetSize)) > 0;

			return false;
		}
		
		/**
		 * Adds the 0 length entries as a precursor to push
		 */
		public void fill() {
			if ((left == null)&&(right==null))
				throw new IllegalStateException("Both edges can not be null");
			if (left == null) {
				left = 	
						new Node
						(
							addressableFactory.createAddressable(ByteBuffer.allocate(0), 0), 
							BigInteger.ZERO, 
							right.parent, 
							null, 
							null, 
							right.previous, 
							right
						);
				right.previous = left;
			} 
			if (right == null) {
				right = 
						new Node
						(
								addressableFactory.createAddressable(ByteBuffer.allocate(0), 0), 
								BigInteger.ZERO, 
								right.parent, 
								null, 
								null, 
								left, 
								left.next
						);
				left.next = right;
			}
		}
		
		public void push() {
			
		}
		
		public Node bubbleLeft(Node incoming) {
			// insert incoming into chain, fix parent links, and return the orphaned left link of the parent
			return null;
		}

		public Node bubbleRight(Node incoming) {
			// insert incoming into chain, fix parent links, and return the orphaned right link of the parent
			return null;
		}
	}

	Node root;
	AddressableFactory addressableFactory;

	public void insert(ByteBuffer data, BigInteger offset, long length) {
		
	}

	public void remove(BigInteger offset, BigInteger length ) {
		
	}

	public Iterator<Addressable> get(BigInteger offset, long length) {
		
		return null;
	}
	
	
	
}
