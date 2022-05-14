package com.ddougher.market.gui;

import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Stroke;
import java.awt.geom.AffineTransform;
import java.awt.geom.Line2D;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.swing.JComponent;
import javax.swing.JFrame;

import com.ddougher.market.AbstractChartSequence;
import com.ddougher.market.ChartSequence;
import com.ddougher.market.data.MetricConstants.Candle;

@SuppressWarnings({"serial","rawtypes", "unchecked"})
public class JChart extends JComponent {

	LinkedHashMap<ChartSequence, SequencePainter> sequences = new LinkedHashMap<>();

	ChartSequence.Listener chartListener = new ChartSequence.Listener() {
		public void sizeChanged(com.ddougher.market.ChartSequence.SizeChangedEvent evt) { repaint(); };
		public void valuesChanged(com.ddougher.market.ChartSequence.ValueChangedEvent evt) { repaint(); };
	};
	
//	public interface SequencePainter {
//		public void paintSequence(Graphics g, ChartSequence<?> seq);
//	}
	
	public interface SequencePainter<T> {
		public Point2D.Float getMinMax(ChartSequence s);
		public void PaintSequence(Graphics2D g2d, ChartSequence<T> sequence, AffineTransform t);
	}
	
	// float xScale = (float) (g.getClipBounds().getWidth()/s.size());
	// float yScale = (float)g.getClipBounds().getHeight() / ( max - min );
	
	public static final SequencePainter BasicLinePainter = new SequencePainter<Float []>() {
		@Override
		public Point2D.Float getMinMax(ChartSequence s) {
			// calculate scale
			float max = s.size() > 0 ? Integer.MIN_VALUE: 100;
			float min = s.size() > 0 ? Integer.MAX_VALUE: 0;
			for (int i = 0; i < s.size(); i++) {
				java.lang.Float val = (Float)s.get(i);
				min = Math.min(min, (float)val);
				max = Math.max(max, (float)val);
			}
			return new Point2D.Float(min,max);
		}
		public void PaintSequence(Graphics2D g2d, ChartSequence s, AffineTransform t) {
			// Draw
			Stroke stroke = g2d.getStroke();
			g2d.setStroke(new BasicStroke(2f));
			for (int i = 0; i < s.size()-1; i++) {
				Float  [] values = { (Float)s.get(i), (Float)s.get(i+1) };
				// draw the connecting lines
				g2d.draw
				(
						new Line2D.Float
						(
								t.transform
								(
										new Point2D.Float
										(
												i+.5f, 
												values[0]
										),
										null
								), 
								t.transform
								(
										new Point2D.Float
										(
												i+1.5f, 
												values[1]
										),
										null
								)
						)
				);
			}
			g2d.setStroke(stroke);
		}

	};

	public static final SequencePainter CandlePainter = new SequencePainter<Float []>() {
		public java.awt.geom.Point2D.Float getMinMax(ChartSequence s) {
			// calculate scale
			float max = s.size() > 0 ? Float.MIN_VALUE: 100;
			float min = s.size() > 0 ? Float.MAX_VALUE: 0;
			for (int i = 0; i < s.size(); i++) {
				Float [] values = (Float [])s.get(i);
				min = Math.min(min, Math.min(values[Candle.LOW.value], values[Candle.VWAP.value]));
				max = Math.max(max, Math.max(values[Candle.HIGH.value], values[Candle.VWAP.value]));
			}
			return new Point2D.Float(min,max);
		}
		public void PaintSequence(Graphics2D g2d, ChartSequence<Float[]> s, AffineTransform t) {
			
			Stroke stroke_old = g2d.getStroke();
			Stroke stickStroke = new BasicStroke(2f);
			g2d.setStroke(stickStroke);
			for (int i = 0; i < s.size(); i++) {
				Float [] values = (Float [])s.get(i);
				Color foreground = g2d.getColor();
	
				// draw the stick
				g2d.draw
				(
						new Line2D.Float
						(
							t.transform
							(
								new Point2D.Float
								(
										i+.5f, 
										values[Candle.LOW.value]
								),
								null
							),
							t.transform
							(
								new Point2D.Float
								(
										i+.5f, 
										values[Candle.HIGH.value]
								),
								null
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
	
				Point2D topLeft = t.transform(new Point2D.Float(((float)i+.1f),top),null);
				Point2D bottomRight = t.transform(new Point2D.Float(((float)i+.9f),bottom),null);
					
				Rectangle2D.Double candleRect =
					new Rectangle2D.Double
					(
						topLeft.getX(),
						topLeft.getY(), 
						Math.abs(bottomRight.getX()-topLeft.getX()),
						Math.abs(bottomRight.getY()-topLeft.getY())
					);
				
				// draw the candle
				g2d.fill(candleRect);
				// set back to foreground color
				g2d.setColor(foreground);
				g2d.draw(candleRect);
			}
			g2d.setStroke(stroke_old);
		}
	};
	
	
	public JChart() {
		super();
	}

	public <T> JChart(ChartSequence<T> sequence, SequencePainter painter) {
		this();
		addChartSequence(sequence, painter);
	}

	public <T> void addChartSequence(ChartSequence<T> sequence, SequencePainter painter) {
		sequences.put(sequence, painter);
		sequence.addListener(chartListener);
		repaint();
	}
	
	public <T> void removeChartSequence(ChartSequence<T> sequence) {
		if (!sequences.containsKey(sequence)) 
			return;
		sequence.removeListener(chartListener);
		sequences.remove(sequence);
		repaint();
	}
	
	@Override
	public void addNotify() {
		super.addNotify();
		sequences.keySet().forEach(s->s.addListener(chartListener));
	}
	
	@Override
	public void removeNotify() {
		super.removeNotify();
		sequences.keySet().forEach(s->s.removeListener(chartListener));
	}
	
	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		Graphics2D g2d = (Graphics2D)g;
		AffineTransform t = g2d.getTransform();
		
		AtomicReference<Float> min = new AtomicReference<Float>(Float.MAX_VALUE);
		AtomicReference<Float> max = new AtomicReference<Float>(Float.MIN_VALUE);
		AtomicReference<Integer> size = new AtomicReference<Integer>(0);
		sequences.forEach((s,p)-> {
			Point2D.Float p2d =  p.getMinMax(s);
			min.getAndUpdate(f->Math.min(f, p2d.x));
			max.getAndUpdate(f->Math.max(f, p2d.y));
			size.getAndUpdate(i->Math.max(i, s.size()));
		});
		float xScale = (float) (g2d.getClipBounds().getWidth()/(float)size.get());
		float yScale = (float)g2d.getClipBounds().getHeight() / ( max.get() - min.get() );

		AffineTransform at = new AffineTransform();
		at.scale(xScale, -yScale);
		at.translate(0, -max.get());
		
		sequences.forEach((s,p)->p.PaintSequence(g2d, s, at));

		
		g2d.setTransform(t);
	}

	
	public static void main(String [] args) {
		Float [][] values = new Float [][] { 
			{3f, 4f, 1f, 2f, 1f, 2.5f},
			{3f, 5f, 2f, 4f, 1f, 3.5f},
			{3.2f, 4.7f, .5f, 2.12f, 1f, 2.5f},
			{3f, 4f, 1f, 2f, 1f, 2.5f},
			{3f, 4f, 1f, 2f, 1f, 2.5f},
			{3f, 4f, 1f, 2f, 1f, 2.5f},
			{3f, 4f, 1f, 2f, 1f, 2.5f}
		};
		JFrame f = new JFrame();
		f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		f.setPreferredSize(new Dimension(500,500));
		f.getContentPane().setBackground(Color.white);
		JChart chart= new JChart();
		chart.addChartSequence(new AbstractChartSequence<Float[]>() {
			@Override public int size() { return values.length; }
			@Override public Float[] get( int offset ) { return values[offset]; }
		}, CandlePainter);
		chart.addChartSequence(new AbstractChartSequence<Float>() {
			@Override public int size() { return values.length; }
			@Override public Float get( int offset ) { return (values[offset][0]+values[offset][3])/2f; }
		}, BasicLinePainter);
		chart.setBackground(Color.white);
		f.getContentPane().add(chart, BorderLayout.CENTER);
		f.pack();
		f.setVisible(true);
	}
	
	
}
