package com.ddougher.market.gui;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.Line2D;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.LinkedHashSet;

import javax.swing.JComponent;

import com.ddougher.market.ChartSequence;
import com.ddougher.market.data.MetricConstants.Candle;

@SuppressWarnings({"serial","rawtypes", "unchecked"})
public class JChart extends JComponent {

	LinkedHashSet<ChartSequence> sequences = new LinkedHashSet<ChartSequence>();

	ChartSequence.Listener chartListener = new ChartSequence.Listener() {
		public void sizeChanged(com.ddougher.market.ChartSequence.SizeChangedEvent evt) { repaint(); };
		public void valuesChanged(com.ddougher.market.ChartSequence.ValueChangedEvent evt) { repaint(); };
	};
	
	
	public JChart() {
		super();
	}

	public <T> void addChartSequence(ChartSequence<T> sequence) {
		sequences.add(sequence);
		sequence.addListener(chartListener);
		repaint();
	}
	
	public <T> void removeChartSequence(ChartSequence<T> sequence) {
		if (!sequences.contains(sequence)) 
			return;
		sequence.removeListener(chartListener);
		sequences.remove(sequence);
		repaint();
	}
	
	@Override
	public void addNotify() {
		super.addNotify();
		sequences.forEach(s->s.addListener(chartListener));
	}
	
	@Override
	public void removeNotify() {
		super.removeNotify();
		sequences.forEach(s->s.removeListener(chartListener));
	}
	
	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		
		// invoke the painters for each sequence in turn
		sequences.forEach(s->paintSequence(g,s));

	}

	// This is basically the candle sequence painter (Default for Float[5] as the sequence value)
	// TODO: refactor to use a painter selector
	void paintSequence(Graphics g, ChartSequence s) {

		// calculate scale
		float max = 0f;
		Float min = 0f;
		float xScale = (float) (g.getClipBounds().getWidth()/s.size());
		for (int i = 0; i < s.size(); i++) {
			Float [] values = (Float [])s.get(i);
			min = Math.min(min, Math.min(values[Candle.LOW.value], values[Candle.VWAP.value]));
			max = Math.max(max, Math.max(values[Candle.HIGH.value], values[Candle.VWAP.value]));
		}
		float yScale = (float)g.getClipBounds().getHeight() / ( max - min );

		Graphics2D g2d = (Graphics2D)g;
		for (int i = 0; i < s.size(); i++) {
			Float [] values = (Float [])s.get(i);
			Color foreground = g2d.getColor();

			// draw the stick
			g2d.draw
			(
					new Line2D.Float
					(
							new Point2D.Float
							(
									i*xScale, 
									(max-values[Candle.LOW.value])*yScale
							), 
							new Point2D.Float
							(
									i*xScale, 
									(max-values[Candle.HIGH.value])*yScale
							)
					)
			);

			// select a candle color
			
			Float top,bottom;
			if (values[Candle.CLOSE.value] > values[Candle.OPEN.value]) {
				g2d.setColor(Color.GREEN);
				top = values[Candle.CLOSE.value];
				bottom = values[Candle.OPEN.value];
			} else {
				g2d.setColor(Color.RED);
				top = values[Candle.OPEN.value];
				bottom = values[Candle.CLOSE.value];
			}

			// draw the candle
			g2d.fill
			(
					new Rectangle2D.Float
					(
						((float)i -.5f)*xScale,
						(max - top)*yScale, 
						1f*xScale,
						(top-bottom)*yScale
					)
			);
			
			// set back to foreground color
			g2d.setColor(foreground);
			
		}
	}
	
}
