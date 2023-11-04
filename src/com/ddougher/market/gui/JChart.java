package com.ddougher.market.gui;

import java.awt.AlphaComposite;
import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Composite;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Stroke;
import java.awt.geom.AffineTransform;
import java.awt.geom.Line2D;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import javax.swing.JComponent;
import javax.swing.JFrame;

import com.ddougher.market.data.MetricConstants.Candle;

@SuppressWarnings({"serial","rawtypes", "unchecked"})
public class JChart extends JComponent {

	class SequenceInfo<T> {
		SequencePainter<T> sequencePainter;
		boolean scalesSeparately = false;
		AffineTransform customTansform = new AffineTransform();
		Composite composite = null;
		/**
		 * @param sequencePainter
		 * @param scalesSeparately
		 * @param customTansform
		 */
		public SequenceInfo(
				Optional<SequencePainter<T>> sequencePainter, 
				Optional<Boolean> scalesSeparately,
				Optional<AffineTransform> customTansform,
				Optional<Composite> composite) {
			super();
			this.sequencePainter = sequencePainter.orElseThrow(()->new IllegalArgumentException());
			this.scalesSeparately = scalesSeparately.orElse(Boolean.FALSE);
			this.customTansform = customTansform.orElse(new AffineTransform());
			this.composite = composite.orElse(null);
		}
	}
	
	LinkedHashMap<ChartSequence, SequenceInfo> sequences = new LinkedHashMap<>();

	ChartSequence.Listener chartListener = new ChartSequence.Listener() {
		public void sizeChanged(ChartSequence.SizeChangedEvent evt) { repaint(); };
		public void valuesChanged(ChartSequence.ValueChangedEvent evt) { repaint(); };
	};
	
	public interface SequencePainter<T> {
		public Point2D.Float getMinMax(ChartSequence s);
		public void PaintSequence(Graphics2D g2d, ChartSequence<T> sequence, AffineTransform t);
	}
	
	public static final SequencePainter<Float> BasicLinePainter = new SequencePainter<Float>() {
		@Override
		public Point2D.Float getMinMax(ChartSequence s) {
			// calculate scale
			float max = s.size() > 0 ? Float.MIN_VALUE: 100;
			float min = s.size() > 0 ? Float.MAX_VALUE: 0;
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

	public static final SequencePainter<Float []> CandlePainter = new SequencePainter<Float []>() {
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
	
	public static final SequencePainter<Float> BarPainter = new SequencePainter<Float>() {
		public java.awt.geom.Point2D.Float getMinMax(ChartSequence s) {
			// calculate scale
			float max = s.size() > 0 ? Float.MIN_VALUE: 100;
			float min = s.size() > 0 ? Float.MAX_VALUE: 0;
			for (int i = 0; i < s.size(); i++) {
				Float val = (Float)s.get(i);
				max = Math.max(max, val);
			}
			return new Point2D.Float(0f,max);
		}
		public void PaintSequence(Graphics2D g2d, ChartSequence<Float> s, AffineTransform t) {
			
			Stroke stroke_old = g2d.getStroke();
			Stroke stickStroke = new BasicStroke(2f);
			g2d.setStroke(stickStroke);
			
			for (int i = 0; i < s.size(); i++) {
				Float val = (Float)s.get(i);
				Color foreground = g2d.getColor();
	
				Point2D topLeft = t.transform(new Point2D.Float(((float)i+.2f),val),null);
				Point2D bottomRight = t.transform(new Point2D.Float(((float)i+.8f),0f),null);
					
				Rectangle2D.Double candleRect =
					new Rectangle2D.Double
					(
						topLeft.getX(),
						topLeft.getY(), 
						Math.abs(bottomRight.getX()-topLeft.getX()),
						Math.abs(bottomRight.getY()-topLeft.getY())
					);
				
				g2d.setBackground(new Color(foreground.getRed(), foreground.getGreen(), foreground.getBlue(), 50));
				
				// draw the candle
				g2d.fill(candleRect);
				// set back to foreground color
				g2d.draw(candleRect);
			}
			g2d.setStroke(stroke_old);
			
		}
	};
	
	public JChart() {
		super();
	}

	public <T> JChart(ChartSequence<T> sequence, SequencePainter<T> painter, Optional<Boolean> scalesSeparately, Optional<AffineTransform> customTransform , Optional<Composite> composite ) {
		this();
		addChartSequence(sequence, painter, scalesSeparately, customTransform, composite);
	}

	public <T> void addChartSequence(ChartSequence<T> sequence, SequencePainter<T> painter, Optional<Boolean> scalesSeparately, Optional<AffineTransform> customTransform, Optional<Composite> composite) {
		sequences.put(sequence, new SequenceInfo<T>(Optional.of(painter), scalesSeparately, customTransform, composite));
		sequence.addListener(chartListener);
		repaint();
	}
	
	public void clearChartSequences() {
		sequences.keySet().forEach(cs->cs.removeListener(chartListener));
		sequences.clear();
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
	
	/*
	 * We pass the transform to the painter because pre-transforming the graphics object will also transform the stroke,
	 * which results in very off style borders and lines. 
	 */
	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		Graphics2D g2d = (Graphics2D)g;
		AffineTransform t = g2d.getTransform();
		
		AtomicReference<Float> min = new AtomicReference<Float>(Float.MAX_VALUE);
		AtomicReference<Float> max = new AtomicReference<Float>(Float.MIN_VALUE);
		AtomicReference<Integer> size = new AtomicReference<Integer>(0);
		sequences.forEach((s,si)-> {
			if (!si.scalesSeparately) {
				Point2D.Float p2d =  si.sequencePainter.getMinMax(s);
				min.getAndUpdate(f->Math.min(f, p2d.x));
				max.getAndUpdate(f->Math.max(f, p2d.y));
				size.getAndUpdate(i->Math.max(i, s.size()));
			}
		});
		float xScale = (float) (g2d.getClipBounds().getWidth()/(float)size.get());
		float yScale = (float)g2d.getClipBounds().getHeight() / ( max.get() - min.get() );
		AffineTransform at = new AffineTransform();
		at.scale(xScale, -yScale);
		at.translate(0, -max.get());

		Composite composite = g2d.getComposite();
		
		sequences.forEach((s,si)-> {
			if (si.composite!=null)
				g2d.setComposite(si.composite);
			if (!si.scalesSeparately) {
				AffineTransform at1 = new AffineTransform(at);
				at1.concatenate(si.customTansform);
				si.sequencePainter.PaintSequence(g2d, s, at1);
			} else {
				Point2D.Float p2d =  si.sequencePainter.getMinMax(s);
				float xScale1 = (float) (g2d.getClipBounds().getWidth()/(float)s.size());
				float yScale1 = (float)g2d.getClipBounds().getHeight() / ( p2d.y - p2d.x );
				AffineTransform at1 = new AffineTransform();
				at1.scale(xScale1, -yScale1);
				at1.translate(0, -p2d.y);
				at1.concatenate(si.customTansform);
				si.sequencePainter.PaintSequence(g2d, s, at1);
			}
			g2d.setComposite(composite);
		});
		
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
		}, CandlePainter, Optional.empty(), Optional.empty(), Optional.empty());
		chart.addChartSequence(new AbstractChartSequence<Float>() {
			@Override public int size() { return values.length; }
			@Override public Float get( int offset ) { return (values[offset][0]+values[offset][3])/2f; }
		}, BasicLinePainter, Optional.empty(), Optional.empty(), Optional.empty());
		chart.addChartSequence(new AbstractChartSequence<Float>() {
			@Override public int size() { return values.length; }
			@Override public Float get( int offset ) { return (values[offset][0]+values[offset][3])/2f; }
		}, BarPainter, Optional.of(Boolean.TRUE), Optional.of(AffineTransform.getScaleInstance(1d, .25d)), Optional.of(AlphaComposite.SrcOver.derive(.5f)));

		
		chart.setBackground(Color.white);
		f.getContentPane().add(chart, BorderLayout.CENTER);
		f.pack();
		f.setVisible(true);
	}
	
	
}
