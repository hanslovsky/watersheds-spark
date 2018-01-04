package org.saalfeldlab.watersheds.pipeline;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.saalfeldlab.watersheds.UnionFindSparse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;

import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.algorithm.localextrema.LocalExtrema;
import net.imglib2.algorithm.localextrema.LocalExtrema.LocalNeighborhoodCheck;
import net.imglib2.img.array.ArrayImg;
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
			final Broadcast< Predicate< T > > threshold,
			final Broadcast< Supplier< RandomAccessible< T > > > relief )
	{
		return new ExtremaAndThreshold<>( extremumCheck, threshold, relief );
	}

	public static < T extends Comparable< T > > Extrema< T > localExtrema(
			final Broadcast< LocalExtrema.LocalNeighborhoodCheck< Point, T > > extremumCheck,
			final Broadcast< Supplier< RandomAccessible< T > > > relief )
	{
		return new Extrema<>( extremumCheck, relief );
	}

	public static < T extends Comparable< T > > Extrema< T > localMinima(
			final JavaSparkContext sc,
			final T maxPeakValue,
			final Supplier< RandomAccessible< T > > relief
			) {
		return localExtrema( sc.broadcast( new LocalExtrema.MinimumCheck<>( maxPeakValue ) ), sc.broadcast( relief ) );
	}

	public static < T extends Comparable< T > > Extrema< T > localMaxima(
			final JavaSparkContext sc,
			final T minPeakValue,
			final Supplier< RandomAccessible< T > > relief )
	{
		return localExtrema( sc.broadcast( new LocalExtrema.MaximumCheck<>( minPeakValue ) ), sc.broadcast( relief ) );
	}

	public static class Extrema< T extends Comparable< T > > implements Function< Tuple2< ArrayImg< UnsignedLongType, ? >, long[] >, Tuple3< ArrayImg< UnsignedLongType, ? >, long[], Long > >
	{

		private final Broadcast< LocalExtrema.LocalNeighborhoodCheck< Point, T > > extremumCheck;

		private final Broadcast< Supplier< RandomAccessible< T > > > relief;

		private Extrema( final Broadcast< LocalNeighborhoodCheck< Point, T > > extremumCheck, final Broadcast< Supplier< RandomAccessible< T > > > relief )
		{
			super();
			this.extremumCheck = extremumCheck;
			this.relief = relief;
		}

		@Override
		public Tuple3< ArrayImg< UnsignedLongType, ? >, long[], Long > call( final Tuple2< ArrayImg< UnsignedLongType, ? >, long[] > dataAndOffset ) throws Exception
		{
			final ArrayImg< UnsignedLongType, ? > store = dataAndOffset._1();
			final long[] offset = dataAndOffset._2();
			final IntervalView< UnsignedLongType > labels = Views.translate( store, offset );
			final RandomAccessible< T > relief = this.relief.getValue().get();
			final ArrayList< Point > extrema = LocalExtrema.findLocalExtrema( Views.interval( relief, Intervals.expand( labels, 1 ) ), extremumCheck.getValue(), MoreExecutors.sameThreadExecutor() );
			final RandomAccess< UnsignedLongType > ra = labels.randomAccess();
			for ( int label = 1, index = 0; index < extrema.size(); ++index, ++label ) {
				ra.setPosition( extrema.get( index ) );
				ra.get().set( label );
			}
			return new Tuple3<>( store, offset, Long.valueOf( extrema.size() ) );
		}
	}

	public static class ExtremaAndThreshold< T extends RealType< T > > implements Function< Tuple2< ArrayImg< UnsignedLongType, ? >, long[] >, Tuple3< ArrayImg< UnsignedLongType, ? >, long[], Long > >
	{

		private final Broadcast< LocalExtrema.LocalNeighborhoodCheck< Point, T > > extremumCheck;

		private final Broadcast< Predicate< T > > threshold;

		private final Broadcast< Supplier< RandomAccessible< T > > > relief;

		private ExtremaAndThreshold(
				final Broadcast< LocalNeighborhoodCheck< Point, T > > extremumCheck,
				final Broadcast< Predicate< T > > threshold,
				final Broadcast< Supplier< RandomAccessible< T > > > relief )
		{
			super();
			this.extremumCheck = extremumCheck;
			this.threshold = threshold;
			this.relief = relief;
		}

		@Override
		public Tuple3< ArrayImg< UnsignedLongType, ? >, long[], Long > call( final Tuple2< ArrayImg< UnsignedLongType, ? >, long[] > dataAndOffset ) throws Exception
		{
			final ArrayImg< UnsignedLongType, ? > store = dataAndOffset._1();
			final long[] offset = dataAndOffset._2();
			final RandomAccessible< T > relief = this.relief.getValue().get();
			final IntervalView< UnsignedLongType > labels = Views.translate( store, offset );

			final RandomAccess< UnsignedLongType > ra = labels.randomAccess();
			int label = 1;

			final Predicate< T > threshold = this.threshold.getValue();

			final Cursor< UnsignedLongType > labelsCursor = Views.flatIterable( labels ).cursor();
			final Cursor< T > reliefCursor = Views.flatIterable( Views.interval( relief, labels ) ).cursor();
			for ( ; labelsCursor.hasNext(); )
			{
				final UnsignedLongType l = labelsCursor.next();
				final T r = reliefCursor.next();
				if ( threshold.test( r ) )
				{
					l.set( label );
					++label;
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

			label = 1;
			final TLongLongHashMap mapping = new TLongLongHashMap();
			for ( final TLongLongIterator it = parents.iterator(); it.hasNext();  ) {
				it.advance();
				final long k = it.key();
				final long r = uf.findRoot( k );
				if ( !mapping.contains( r ) )
				{
					mapping.put( r, label );
					++label;
				}
				mapping.put( k, mapping.get( r ) );
			}

			final UnsignedLongType zero = new UnsignedLongType();
			zero.setZero();

			for ( final UnsignedLongType l : labels )
				if ( !l.valueEquals( zero ) )
					l.set( mapping.get( l.get() ) );


			LOG.debug( "Using extremum check class {}", extremumCheck.getValue().getClass() );
			final ArrayList< Point > extrema = LocalExtrema.findLocalExtrema( Views.interval( relief, Intervals.expand( labels, 1 ) ), extremumCheck.getValue(), MoreExecutors.sameThreadExecutor() );
			LOG.debug( "Got {}/{} local extrema seeds.", label, extrema.size() );
			for ( final Point extremum : extrema )
			{
				ra.setPosition( extremum );
				final UnsignedLongType v = ra.get();
				if ( v.valueEquals( zero ) )
				{
					v.set( label );
					++label;
				}
			}

			return new Tuple3<>( store, offset, Long.valueOf( label + 1 ) );
		}
	}

}
