package de.hanslovsky.examples.pipeline;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.function.Predicate;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;

import de.hanslovsky.examples.UnionFindSparse;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.localextrema.LocalExtrema;
import net.imglib2.algorithm.localextrema.LocalExtrema.LocalNeighborhoodCheck;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import scala.Tuple2;
import scala.Tuple3;

public class MakeSeeds
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static < T extends RealType< T > > ExtremaAndThreshold< T > localExtremaAndThreshold(
			final Broadcast< LocalExtrema.LocalNeighborhoodCheck< Point, T > > extremumCheck,
			final Broadcast< Predicate< T > > threshold )
	{
		return new ExtremaAndThreshold<>( extremumCheck, threshold );
	}

	public static < T extends Comparable< T > > Extrema< T > localExtrema(
			final Broadcast< LocalExtrema.LocalNeighborhoodCheck< Point, T > > extremumCheck )
	{
		return new Extrema<>( extremumCheck );
	}

	public static < T extends Comparable< T > > Extrema< T > localMinima(
			final JavaSparkContext sc,
			final T maxPeakValue
			) {
		return localExtrema( sc.broadcast( new LocalExtrema.MinimumCheck<>( maxPeakValue ) ) );
	}

	public static < T extends Comparable< T > > Extrema< T > localMaxima(
			final JavaSparkContext sc,
			final T minPeakValue )
	{
		return localExtrema( sc.broadcast( new LocalExtrema.MaximumCheck<>( minPeakValue ) ) );
	}

	public static class Extrema< T extends Comparable< T > > implements Function<
	Tuple2< Interval, RandomAccessible< T > >, Tuple3< RandomAccessibleInterval< UnsignedLongType >, RandomAccessible< T >, Long > >
	{

		private final Broadcast< LocalExtrema.LocalNeighborhoodCheck< Point, T > > extremumCheck;

		private Extrema( final Broadcast< LocalNeighborhoodCheck< Point, T > > extremumCheck )
		{
			super();
			this.extremumCheck = extremumCheck;
		}

		@Override
		public Tuple3< RandomAccessibleInterval< UnsignedLongType >, RandomAccessible< T >, Long > call( final Tuple2< Interval, RandomAccessible< T > > intervalAndRelief ) throws Exception
		{
			final IntervalView< UnsignedLongType > labels = Views.translate( ArrayImgs.unsignedLongs( Intervals.dimensionsAsLongArray( intervalAndRelief._1() ) ), Intervals.minAsLongArray( intervalAndRelief._1() ) );
			final ArrayList< Point > extrema = LocalExtrema.findLocalExtrema( Views.interval( intervalAndRelief._2(), Intervals.expand( labels, 1 ) ), extremumCheck.getValue(), MoreExecutors.sameThreadExecutor() );
			final RandomAccess< UnsignedLongType > ra = labels.randomAccess();
			for ( int label = 1, index = 0; index < extrema.size(); ++index, ++label ) {
				ra.setPosition( extrema.get( index ) );
				ra.get().set( label );
			}
			return new Tuple3<>( labels, intervalAndRelief._2(), Long.valueOf( extrema.size() ) );
		}
	}

	public static class ExtremaAndThreshold< T extends RealType< T > > implements Function<
	Tuple2< Interval, RandomAccessible< T > >, Tuple3< RandomAccessibleInterval< UnsignedLongType >, RandomAccessible< T >, Long > >
	{

		private final Broadcast< LocalExtrema.LocalNeighborhoodCheck< Point, T > > extremumCheck;

		private final Broadcast< Predicate< T > > threshold;

		private ExtremaAndThreshold( final Broadcast< LocalNeighborhoodCheck< Point, T > > extremumCheck, final Broadcast< Predicate< T > > threshold )
		{
			super();
			this.extremumCheck = extremumCheck;
			this.threshold = threshold;
		}

		@Override
		public Tuple3< RandomAccessibleInterval< UnsignedLongType >, RandomAccessible< T >, Long > call( final Tuple2< Interval, RandomAccessible< T > > intervalAndRelief ) throws Exception
		{
			final Interval interval = intervalAndRelief._1();
			final RandomAccessible< T > relief = intervalAndRelief._2();
			final IntervalView< UnsignedLongType > labels = Views.translate( ArrayImgs.unsignedLongs( Intervals.dimensionsAsLongArray( interval ) ), Intervals.minAsLongArray( interval ) );
			LOG.debug( "Using extremum check class {}", extremumCheck.getValue().getClass() );
			final ArrayList< Point > extrema = LocalExtrema.findLocalExtrema( Views.interval( relief, Intervals.expand( labels, 1 ) ), extremumCheck.getValue(), MoreExecutors.sameThreadExecutor() );

			final RandomAccess< UnsignedLongType > ra = labels.randomAccess();
			int label = 1;
			for ( int index = 0; index < extrema.size(); ++index, ++label )
			{
				ra.setPosition( extrema.get( index ) );
				ra.get().set( label );
			}

			LOG.debug( "Got {}/{} local extrema seeds.", label, extrema.size() );

			final UnsignedLongType zero = new UnsignedLongType();
			zero.setZero();

			final Predicate< T > threshold = this.threshold.getValue();

			final Cursor< UnsignedLongType > lc = Views.flatIterable( labels ).cursor();
			final Cursor< T > rc = Views.flatIterable( Views.interval( relief, interval ) ).cursor();
			for ( ; lc.hasNext(); )
			{
				final UnsignedLongType l = lc.next();
				final T r = rc.next();
				if ( threshold.test( r ) && l.valueEquals( zero ) )
				{
					l.set( label++ );
					extrema.add( new Point( lc ) );
				}
			}

			LOG.debug( "After thresholding: {} seeds", label );

			final TLongLongHashMap parents = new TLongLongHashMap();
			final TLongLongHashMap ranks = new TLongLongHashMap();
			final UnionFindSparse uf = new UnionFindSparse( parents, ranks, 0 );

			final int nDim = labels.numDimensions();
			for ( int d = 0; d < nDim; ++d )
			{
				final long[] min = Intervals.minAsLongArray( labels );
				final long[] max = Intervals.maxAsLongArray( labels );
				final long[] minPlusOne = min.clone();
				final long[] maxMinusOne = max.clone();
				minPlusOne[ d ] += 1;
				maxMinusOne[ d ] -= 1;
				final Cursor< UnsignedLongType > c1 = Views.flatIterable( Views.interval( labels, new FinalInterval( min, maxMinusOne ) ) ).cursor();
				final Cursor< UnsignedLongType > c2 = Views.flatIterable( Views.interval( labels, new FinalInterval( minPlusOne, max ) ) ).cursor();
				while ( c1.hasNext() )
				{
					final long v1 = c1.next().get();
					final long v2 = c2.next().get();
					if ( v1 != 0 && v2 != 0 )
						uf.join( uf.findRoot( v1 ), uf.findRoot( v2 ) );
				}
			}

			LOG.debug( "Current label count {} {} {}", label, parents.size(), ranks.size() );

			label = 0;
			final TLongLongHashMap mapping = new TLongLongHashMap();
			for ( final TLongLongIterator it = parents.iterator(); it.hasNext();  ) {
				it.advance();
				final long k = it.key();
				final long r = uf.findRoot( k );
				if ( !mapping.contains( r ) )
					mapping.put( r, ++label );
				mapping.put( k, mapping.get( r ) );
			}

			for ( final UnsignedLongType l : labels )
				if ( !l.valueEquals( zero ) )
					l.set( mapping.get( l.get() ) );

			return new Tuple3<>( labels, intervalAndRelief._2(), Long.valueOf( label ) );
		}
	}

}
