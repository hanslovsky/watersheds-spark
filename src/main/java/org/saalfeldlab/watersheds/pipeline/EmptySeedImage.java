package org.saalfeldlab.watersheds.pipeline;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import net.imglib2.FinalInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.util.Intervals;
import scala.Tuple2;

public class EmptySeedImage< T extends NativeType< T > > implements PairFunction< HashWrapper< long[] >, HashWrapper< long[] >, Tuple2< ArrayImg< T, ? >, long[] > >
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	private final int halo;

	private final long[] min;

	private final long[] max;

	private final int[] blockSize;

	private final Broadcast< T > t;

	public EmptySeedImage( final int halo, final long[] max, final int[] blockSize, final Broadcast< T > t )
	{
		this( halo, new long[ max.length ], max, blockSize, t );
	}

	public EmptySeedImage( final int halo, final long[] min, final long[] max, final int[] blockSize, final Broadcast< T > t )
	{
		super();
		this.halo = halo;
		this.min = min;
		this.max = max;
		this.blockSize = blockSize;
		this.t = t;
	}

	@Override
	public Tuple2< HashWrapper< long[] >, Tuple2< ArrayImg< T, ? >, long[] > > call( final HashWrapper< long[] > t ) throws Exception
	{
		final long[] min = t.getData().clone();
		final long[] max = min.clone();
		for ( int d = 0; d < min.length; ++d )
		{
			max[ d ] = Math.min( this.max[ d ], max[ d ] + halo + blockSize[ d ] - 1 );
			min[ d ] = Math.max( this.min[ d ], min[ d ] - halo );
		}
		final FinalInterval interval = new FinalInterval( min, max );
		LOG.debug( "Created interval (min={} max={}) for {}", Arrays.toString( min ), Arrays.toString( max ), Arrays.toString( t.getData() ) );
		return new Tuple2<>( t, new Tuple2<>( new ArrayImgFactory< T >().create( Intervals.dimensionsAsLongArray( interval ), this.t.getValue() ), min ) );
	}

}
