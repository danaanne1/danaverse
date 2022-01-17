package com.ddougher.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.UUID;
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
 * The idea of maintaining a constant, cross linked tree structure with all nodes represented. 
 * was discarded because, although push performant, structure and space optimization are not.
 * 
 * The rest of this comment is historical.
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
/*
 * Some tenants for the new structure
 * 
 * 1) Once created, leafs can only be deleted by merging with their neighbors.
 * 2) Leafs will always remain in sequential order.
 * 3) Leafs are doubly linked forwards and backwards, and keep transaction history for their previous/next link changes
 * 4) Structure is considered ephemeral.
 *   4a) There will never be a structural node that doesnt have 2 leafs
 * 5) Markers for transaction ordering are type 2 (time based) UUIDS. So the transaction density limit is 10m/s.
 * 6) Our target for random insertion performance is > 50k/s
 * 7) A typical m5 does 300k JOPS sec. A typical i7: 50k
 * 8) The head and the tail can change due to insertion, but the root is permanent.
 * 
 */
public class BigArray {

	public interface AssetFactory {
		Addressable createAddressable();
		Sizeable createSizeable();
	}

	public interface Sizeable {
		void set(BigInteger value, UUID tid);
		BigInteger get(UUID tid);
		Sizeable duplicate();
	}
	
	public interface Addressable {
		void set(ByteBuffer data, UUID tid);
		void set(Addressable src, UUID tid);
		void append(Addressable a, UUID tid);
		int size(UUID tid);
		ByteBuffer get(UUID tid);

		/** swaps a and b */
		default void swap(Addressable b, UUID tid) {
			ByteBuffer tmp = b.get(tid);
			b.set(get(tid), tid);
			set(tmp,tid);
		}
		
		/** changes the effective size of this addressable. Essentially the same as set(get(mark).duplicate().position(length).flip(),mark) */
		default void resize(int length, UUID tid) {
			set((ByteBuffer)get(tid).duplicate().position(length).flip(),tid);
		}
	}
	
	public class Transaction {
		UUID transactionId;;
		LinkedHashMap<String, Runnable> postTransactionOperations = null;

		public void addPostTransactionOperation(String key, Runnable r) {
			if (postTransactionOperations == null)
				postTransactionOperations = new LinkedHashMap<String, Runnable>();
			postTransactionOperations.put(key, r);
		}
	}

	class Location {
		Node node;
		int nodeOffset;
		public Location(Node node, int nodeOffset) {
			super();
			this.node = node;
			this.nodeOffset = nodeOffset;
		}
	}

	class Node {
		final Addressable data;
		final Sizeable size;
		BigInteger free;   // number of free leafs
		BigInteger count;  // number of leafs
		Node parent;
		Node left;
		Node right;
		Node previous;
		Node next;
		BigInteger id;
		public Node
		(
				Addressable data,
				Sizeable size, 
				BigInteger free,
				BigInteger count,
				Node parent,
				Node left, 
				Node right,
				Node previous,
				Node next
		) {
			super();
			this.data = data;
			this.size = size;
			this.free = free;
			this.count = count;
			this.parent = parent;
			this.left = left;
			this.right = right;
			this.previous = previous;
			this.next = next;
			this.id = (nodeIdCounter = nodeIdCounter.add(BigInteger.ONE));
		}

		public String dump(String prefix, int depth) {
			return metricsHelper.withMetric( "Node.Dump", () -> {
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
					.append("o: "+data)
					.append("}\n");
				
				if (left != null)
					sb.append(left.dump("L: ",depth+1));
				if (right != null)
					sb.append(right.dump("R: ",depth+1));
				
				return sb.toString();
			});
		}	
	}

	public static final Comparator<UUID> timePrioritizedComparator = new Comparator<UUID>() {
		@Override
		public int compare(UUID o1, UUID o2) {
			if (o1 == o2)
				return 0;
			if (o1 == null) 
				return 1;
			if (o2 == null)
				return -1;
			int i = Long.compare(o1.timestamp(), o2.timestamp());
			if (i==0)
				i = o1.compareTo(o2);
			return i;
		}
	};
	public static final int defaultFragmentLimit = 0;

	private static final Thread optimizerThread = new Thread(BigArray::optimizerLoop);
	private static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate(0);

	static {
		optimizerThread.setDaemon(true);
		optimizerThread.start();
	}

	
	private final ReentrantReadWriteLock structureLock = new ReentrantReadWriteLock(true);
	private final Object transactionLock = new Object();
	private final int fragmentLimit;
	
	private AssetFactory assetFactory;

	private Node root; // After the first push, the root never changes
	private Node head;
	private Node tail;
	private BigInteger nodeIdCounter = BigInteger.ZERO;
	private MetricsHelper metricsHelper = new MetricsHelper();
	
	private volatile UUID highWatermark;
	private volatile BigInteger depth; // is the number of layers from root to leaf, inclusive. The number of leafs is 2^(depth-1)

	public BigArray(AssetFactory assetFactory, int fragmentLimit) {
		super();
		this.assetFactory = assetFactory;
		this.fragmentLimit = fragmentLimit;
		highWatermark = TimeBasedUUIDGenerator.instance().nextUUID();
	}
	
	public BigArray(AssetFactory assetFactory) {
		this(assetFactory, defaultFragmentLimit);
	}
	

	public String dump() {
		return metricsHelper.withMetric("BigArray.Dump", () -> {
			return transactionally(()->{
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

	public <T> T transactionally(Supplier<T> transaction) {
		structureLock.readLock().lock();
		try {
			synchronized(transactionLock) {
				return transaction.get();
			}
		} finally {
			structureLock.readLock().unlock();
		}
	}
	
	@SuppressWarnings("unused")
	public void transactionally(Runnable transaction) {
		transactionally(()->{
			transaction.run();
			return null;
		});
	}
	
	/**
	 * invoked anytime content structure is subject to change
	 * @param transaction
	 */
	private <T> T transactionally(Function<Transaction, T> transaction) {
		Transaction t = new Transaction();
		T result = transactionally(() -> {
			t.transactionId = TimeBasedUUIDGenerator.instance().nextUUID();
			T ret = transaction.apply(t);
			highWatermark = t.transactionId;
			return ret;
		});
		t.postTransactionOperations.values().forEach(a->a.run());
		return result;
	}			
	
	@SuppressWarnings("unused")
	private void transactionally(Consumer<Transaction> transaction) {
		transactionally(t->{
			transaction.accept(t);
			return null;
		});
	}
	
	/**
	 * invoked anytime tree structure is subject to change
	 * @param transaction
	 */
	private <T> T structurally(Function<Transaction, T> transaction) {
		structureLock.writeLock().lock();
		try {
			return transactionally(transaction);
		} finally {
			structureLock.writeLock().unlock();
		}
	}
	
	@SuppressWarnings("unused")
	private void structurally(Consumer<Transaction> transaction) {
		structurally(t->{
			transaction.accept(t);
			return null;
		});
	}
	
	
	private Location locate(BigInteger off, UUID tid) {
		return metricsHelper.withMetric("BigArray.Locate", () -> {
			Node active = root;
			BigInteger remaining  = off;
			while (active.data == null && remaining.compareTo(active.size.get(tid))>=0) {
				if (active.left.size.get(tid).compareTo(remaining)>=0) {
					active = active.left;
				} else {
					remaining = remaining.subtract(active.left.size.get(tid));
					active = active.right;
				}
			}

			if (!remaining.equals(BigInteger.ZERO) && remaining.equals(active.size.get(tid)) && active.next != null) {
				remaining = remaining.subtract(active.size.get(tid));
				active = active.next;
			}
			
			return new Location(active, remaining.intValue());
		});
	}

	private Location split(BigInteger offset, Location l, Transaction t) {
		ByteBuffer data = ((ByteBuffer) l.node.data.get(t.transactionId).duplicate().position(l.nodeOffset)).slice();
		l.node.data.resize(l.nodeOffset, t.transactionId);
		BigInteger difference = BigInteger.valueOf(l.nodeOffset).subtract(l.node.size.get(t.transactionId));
		propagateDelta(l.node, difference, BigInteger.ZERO, BigInteger.ZERO, t);
		insert(data, offset, l.nodeOffset, t);
		return new Location(l.node.next, 0);
	}
	
	private void propagateDelta(Node node, BigInteger size, BigInteger free, BigInteger count, Transaction t) {
		for (Node active = node; active!=null; active=active.parent) {
			if (!size.equals(BigInteger.ZERO))
				active.size.set(active.size.get(t.transactionId).add(size),t.transactionId);
			active.free = active.free.add(free);
			active.count = active.count.add(count);
		}
	}
	
	/**
	 * Inserts a block of data at the given offset.
	 * 
	 * @param data
	 * @param length
	 */
	public void insert(final ByteBuffer data, BigInteger offset) {
		int length = data.limit();
		transactionally(t->{
			insert (data, offset, length, t);
		});
	}
	
	private void insert(ByteBuffer data, BigInteger offset, int length, Transaction t) {
		Addressable a = assetFactory.createAddressable();
		a.set(data, t.transactionId);

		// steps: locate the node at the insertion point
		Location l = locate(offset, t.transactionId);
		
		// if the current node is a space, wake it back up and return
		if (l.node.size.get(t.transactionId).equals(BigInteger.ZERO)) {
			l.node.data.set(data, t.transactionId);
			propagateDelta(l.node, BigInteger.valueOf(length), BigInteger.ONE.negate(), BigInteger.ZERO, t);
			return;
		} 

		// if the current node is the tail, the only option is to insert right
		if (l.nodeOffset >= l.node.size.get(t.transactionId).intValue() && l.node.next == null) {
			insertToRight(l.node, data, length, t);
			// there should be a rebalancing step here
			return;
		}
		
		// possibly split the node, which involves a size reduction and then reinserts the fragment
		if ((l.nodeOffset != 0)&&(l.nodeOffset < l.node.data.size(t.transactionId))) 
			l = split(offset, l, t);
		
		
		// do the insert
		if (l.node.previous != null && rightSideIsDeeper(l.node.previous,l.node, t.transactionId)) {
			insertToRight(l.node.previous, data, length, t);
		} else {
			insertToLeft(l.node, data, length, t);
		}
	}

	@SuppressWarnings("unused")
	private void balance(Node node, Transaction t) {
		BigInteger countDifference = node.left.count.subtract(node.right.count);
		int cval = countDifference.compareTo(BigInteger.ZERO);
		if (cval<0) {
			balanceRightToLeft(node.right, node.left, countDifference.abs(), t);
		} else if (cval>0) {
			balanceLeftToRight(node.left, node.right, countDifference.abs(), t);
		}
		if (node.parent!=null) 
			balance(node.parent, t);
	}

	private void balanceRightToLeft(Node right, Node left, BigInteger abs, Transaction t) {
		// TODO Auto-generated method stub
		
	}

	private void balanceLeftToRight(Node left, Node right, BigInteger abs, Transaction t) {
		// TODO Auto-generated method stub
		
	}

	private boolean rightSideIsDeeper(Node left, Node right, UUID tid) {
		HashSet<Node> discovered = new HashSet<>();
		while (left.parent!=null || right.parent!=null) {
			if (right.parent != null) {
				if (!discovered.add(right.parent))
					return false; // left got there first
				right = right.parent;
			}
			if (left.parent != null) {
				if (!discovered.add(left.parent)) 
					return true; // this means right got there first
				left = left.parent;
			}
		}
		throw new IllegalStateException("Nodes do not resolve to a common parent");
	}

	private void insertToLeft(Node node, ByteBuffer data, int length, Transaction t) {
		Node newParent = new Node( null, node.size.duplicate(), node.free, node.count, node.parent, null, node, null, null );
		newParent.left = new Node( assetFactory.createAddressable(), assetFactory.createSizeable(), BigInteger.ZERO, BigInteger.ZERO, newParent, null, null, node.previous, node);
		node.previous = newParent.left;
		if (node.parent.left == node) {
			node.parent.left = newParent;
		} else {
			node.parent.right = newParent;
		}
		node.parent = newParent;
		propagateDelta(node.previous, BigInteger.valueOf(length), length == 0 ? BigInteger.ONE: BigInteger.ZERO, BigInteger.ONE, t);
	}

	private void insertToRight(Node node, ByteBuffer data, int length, Transaction t) {
		Node newParent = new Node( null, node.size.duplicate(), node.free, node.count, node.parent, node, null, null, null );
		newParent.right = new Node( assetFactory.createAddressable(), assetFactory.createSizeable(), BigInteger.ZERO, BigInteger.ZERO, newParent, null, null, node, node.next);
		node.next = newParent.right;
		if (node.parent.left == node) {
			node.parent.left = newParent;
		} else {
			node.parent.right = newParent;
		}
		node.parent = newParent;
		propagateDelta(node.next, BigInteger.valueOf(length), (length==0?BigInteger.ONE:BigInteger.ZERO), BigInteger.ONE, t);
	}

	@SuppressWarnings({ "unchecked" })
	private static final void optimizerLoop() {
	}
	
	protected boolean optimizeSpace() {
		//  ( 2^(depth-1) ) / freespace > 10 when there is less than 10% freespace
		return metricsHelper.withMetric("BigArray.OptimizeSpace", () -> {
			return false;
		});
	};
	
	public BigInteger size() {
		return size(null);
	}

	public BigInteger size(UUID tid) {
		return transactionally(()-> { return root.size.get(tid!=null?tid:highWatermark); }  );
	}
	
	public void remove(BigInteger offset, BigInteger length ) {
	}

	public Iterator<ByteBuffer> get(BigInteger offset, BigInteger length) {
		return get(offset, length, highWatermark);
	}
	
	public Iterator<ByteBuffer> get(BigInteger offset, BigInteger length, UUID tid) {
		return new ArrayList<ByteBuffer>().iterator();
	}
	
	
	
}
