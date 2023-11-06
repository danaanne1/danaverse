package com.ddougher.market.gui.chart;

import java.util.HashSet;

public abstract class AbstractChartSequence<T> implements ChartSequence<T> {

	HashSet<Listener<T>> listeners = new HashSet<Listener<T>>();
	
	@Override
	public void addListener(Listener<T> listener) {
		synchronized(listeners) {
			listeners.add(listener);
		}
	}

	@Override
	public void removeListener(Listener<T> listener) {
		synchronized(listeners) {
			listeners.remove(listener);
		}
	}

	void fireSizeChangedEvent(int sizeDifference) {
		synchronized(listeners) {
			if (listeners.isEmpty()) return;
			SizeChangedEvent<T> evt = new SizeChangedEvent<T>(this, sizeDifference);
			listeners.forEach(l->l.sizeChanged(evt));
		}
	}

	void fireValueChangedEvent(int startOffset, int length) {
		synchronized(listeners) {
			if (listeners.isEmpty()) return;
			ValueChangedEvent<T> evt = new ValueChangedEvent<T>(this, startOffset, startOffset+length);
			listeners.forEach(l->l.valuesChanged(evt));
		}
	}

}
