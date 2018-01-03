package de.hanslovsky.examples.pipeline;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;

import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.type.NativeType;
import scala.Tuple2;

public class Expand< T extends NativeType< T > > implements PairFunction< Tuple2< HashWrapper< long[] >, RandomAccessible< T > >, HashWrapper< long[] >, Tuple2< Interval, RandomAccessible< T > > >
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

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
			max[ d ] = Math.min( this.max[ d ], max[ d ] + halo + blockSize[ d ] - 1 );
			min[ d ] = Math.max( this.min[ d ], min[ d ] - halo );
		}
		final FinalInterval interval = new FinalInterval( min, max );
		LOG.debug( "Created interval (min={} max={}) for {}", Arrays.toString( min ), Arrays.toString( max ), Arrays.toString( t._1().getData() ) );
		return new Tuple2<>( t._1(), new Tuple2<>( interval, t._2() ) );
	}

}
