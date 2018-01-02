package de.hanslovsky.examples.pipeline;

import org.apache.spark.api.java.function.PairFunction;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import scala.Tuple2;

public class Expand< T > implements PairFunction< Tuple2< HashWrapper< long[] >, RandomAccessible< T > >, HashWrapper< long[] >, Tuple2< Interval, RandomAccessible< T > > >
{

	private final int halo;

	private final long[] min;

	private final long[] max;

	private final int[] blockSize;

	public Expand( final int halo, final long[] max, final int[] blockSize )
	{
		this( halo, new long[ max.length ], max, blockSize );
	}

	public Expand( final int halo, final long[] min, final long[] max, final int[] blockSize )
	{
		super();
		this.halo = halo;
		this.min = min;
		this.max = max;
		this.blockSize = blockSize;
	}

	@Override
	public Tuple2< HashWrapper< long[] >, Tuple2< Interval, RandomAccessible< T > > > call( final Tuple2< HashWrapper< long[] >, RandomAccessible< T > > t ) throws Exception
	{
		final long[] min = t._1().getData().clone();
		final long[] max = min.clone();
		for ( int d = 0; d < min.length; ++d )
		{
			max[ d ] = Math.min( this.max[ d ], max[ d ] + halo + blockSize[ d ] );
			min[ d ] = Math.max( this.min[ d ], min[ d ] - halo );
		}
		return new Tuple2<>( t._1(), new Tuple2<>( new FinalInterval( min, max ), t._2() ) );
	}

}
