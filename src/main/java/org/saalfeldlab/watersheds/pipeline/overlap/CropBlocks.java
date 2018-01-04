package org.saalfeldlab.watersheds.pipeline.overlap;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.view.Views;
import scala.Tuple2;

public class CropBlocks< T > implements PairFunction< Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< T > >, HashWrapper< long[] >, RandomAccessibleInterval< T > >
{

	private final Broadcast< CellGrid > grid;

	public CropBlocks( final Broadcast< CellGrid > grid )
	{
		super();
		this.grid = grid;
	}

	@Override
	public Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< T > > call( final Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< T > > t ) throws Exception
	{
		final RandomAccessibleInterval< T > rai = t._2();
		final long[] blockMin = t._1().getData().clone();
		final long[] blockMax = new long[ blockMin.length ];
		for ( int d = 0; d < blockMin.length; ++d )
			blockMax[ d ] = Math.min( blockMin[ d ] + grid.getValue().cellDimension( d ), grid.getValue().imgDimension( d ) ) - 1;
		final RandomAccessibleInterval< T > target = Views.interval( rai, new FinalInterval( blockMin, blockMax ) );
		return new Tuple2<>( t._1(), target );
	}

}
