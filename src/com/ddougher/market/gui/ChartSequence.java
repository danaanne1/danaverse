package com.ddougher.market.gui;

import java.util.EventListener;
import java.util.EventObject;

@SuppressWarnings({"serial","unchecked"})
public interface ChartSequence<T> {
	
	class SequenceEvent<T> extends EventObject {
		public SequenceEvent(ChartSequence<T> source) {
			super(source);
		}
		ChartSequence<T> sequence() { return (ChartSequence<T>)getSource(); }
	}
	
	class SizeChangedEvent<T> extends SequenceEvent<T> {
		int sizeDifference;
		public SizeChangedEvent(ChartSequence<T> source, int sizeDifference) {
			super(source);
			this.sizeDifference = sizeDifference;
		}
		public int sizeDifference() {
			return sizeDifference;
		}
	}

	class ValueChangedEvent<T> extends SequenceEvent<T> {
		int startOffset, endOffset;
		public ValueChangedEvent(ChartSequence<T> source, int startOffset, int endOffset) {
			super(source);
			this.startOffset = startOffset;
			this.endOffset = endOffset;
		}
		public int startOffset() {
			return startOffset;
		}
		public int endOffset() {
			return endOffset;
		}
		
	}

	interface Listener<T> extends EventListener {
		void sizeChanged(SizeChangedEvent<T> evt);
		void valuesChanged(ValueChangedEvent<T> evt);
	}
	
	int size();
	T get(int offset);
	
	void addListener(Listener<T> listener);
	void removeListener(Listener<T> listener);
	
}
