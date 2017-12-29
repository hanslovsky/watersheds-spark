package de.hanslovsky.examples;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.ToDoubleBiFunction;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.util.concurrent.MoreExecutors;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.Localizable;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.localextrema.LocalExtrema;
import net.imglib2.algorithm.localextrema.LocalExtrema.MaximumCheck;
import net.imglib2.algorithm.morphology.watershed.PriorityQueueFactory;
import net.imglib2.algorithm.morphology.watershed.Watersheds;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import scala.Tuple2;

public class RunWatershedsOnDistanceTransform< T extends RealType< T > & NativeType< T >, L extends IntegerType< L > & NativeType< L > >
implements Function< Tuple2< Interval, RandomAccessible< T > >, Tuple2< RandomAccessibleInterval< L >, Long > >
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	private final boolean processBySlice;

	private final Broadcast< Predicate< T > > threshold;

	private final Broadcast< ToDoubleBiFunction< T, T > > dist;

	private final Broadcast< L > l;

	private final Broadcast< PriorityQueueFactory > factory;

	public RunWatershedsOnDistanceTransform(
			final JavaSparkContext sc,
			final boolean processBySlice,
			final Predicate< T > threshold,
			final ToDoubleBiFunction< T, T > dist,
			final L l,
			final PriorityQueueFactory factory )
	{
		super();
		this.processBySlice = processBySlice;
		this.threshold = sc.broadcast( threshold );
		this.dist = sc.broadcast( dist );
		this.l = sc.broadcast( l );
		this.factory = sc.broadcast( factory );
	}

	@Override
	public Tuple2< RandomAccessibleInterval< L >, Long > call( final Tuple2< Interval, RandomAccessible< T > > offsetAndAffs ) throws Exception
	{
		LOG.debug( "Calculating watersheds for interval " + Arrays.toString( Intervals.minAsLongArray( offsetAndAffs._1() ) ) + " " + Arrays.toString( Intervals.maxAsLongArray( offsetAndAffs._1() ) ) );
		if ( Arrays.stream( Intervals.minAsLongArray( offsetAndAffs._1() ) ).filter( m -> m == 225 ).count() > 0 )
			System.out.println( "Calculating watersheds for interval " + Arrays.toString( Intervals.minAsLongArray( offsetAndAffs._1() ) ) + " " + Arrays.toString( Intervals.maxAsLongArray( offsetAndAffs._1() ) ) );
		final Interval interval = offsetAndAffs._1();
		final long[] imageOffset = Intervals.minAsLongArray( interval );
		final long[] dims = Intervals.dimensionsAsLongArray( interval );
		final RandomAccessible< T > affinities = offsetAndAffs._2();
		final RandomAccessibleInterval< L > labels = Views.translate( new ArrayImgFactory< L >().create( dims, l.getValue() ), imageOffset );

		final long nLabels;
		if ( dims.length == 3 && this.processBySlice )
		{
			long nextLabel = 1;
			for ( long z = interval.min( 2 ); z <= interval.max( 2 ); ++z )
			{
				final RandomAccessibleInterval< T > hs = Views.hyperSlice( Views.interval( affinities, interval ), 2, z );
				nextLabel = process( hs, Views.hyperSlice( labels, 2, z ), threshold.getValue(), dist.getValue(), nextLabel, factory.getValue() );
			}
			nLabels = nextLabel;
		}
		else
			nLabels = process( affinities, labels, threshold.getValue(), dist.getValue(), 1, factory.getValue() );

		LOG.debug( "Got {} labels in watershed", nLabels );

		return new Tuple2<>( labels, nLabels );
	}

	public static < A extends RealType< A >, L extends IntegerType< L > > long process(
			final RandomAccessible< A > affs,
			final RandomAccessibleInterval< L > labels,
			final Predicate< A > threshold,
			final ToDoubleBiFunction< A, A > dist,
			final long firstLabel,
			final PriorityQueueFactory factory ) throws InterruptedException, ExecutionException
	{
//		System.out.println( "DOING WATERSHEDS FOR " + Arrays.toString( Intervals.minAsLongArray( labels ) ) + " " + Arrays.toString( Intervals.dimensionsAsLongArray( labels ) ) );
		final A negativeInfinityExtension = Util.getTypeFromInterval( Views.interval( affs, labels ) ).createVariable();
		negativeInfinityExtension.setReal( Double.NEGATIVE_INFINITY );
		final MaximumCheck< A > maximumCheck = new LocalExtrema.MaximumCheck<>( negativeInfinityExtension );
		final RandomAccessible< A > affsZeroExtended = Views.extendValue( Views.interval( affs, labels ), negativeInfinityExtension );
		final List< ? extends Localizable > maximumSeeds = LocalExtrema.findLocalExtrema( Views.interval( affsZeroExtended, Intervals.expand( labels, 1 ) ), maximumCheck, MoreExecutors.sameThreadExecutor() );
//		final AtomicLong id = new AtomicLong( firstLabel );
		long id = firstLabel;
		final RandomAccess< L > labelAccess = labels.randomAccess();
		for ( final Localizable seed : maximumSeeds )
		{
			labelAccess.setPosition( seed );
			labelAccess.get().setInteger( id++ );
		}

		final Cursor< A > ac = Views.flatIterable( Views.interval( affs, labels ) ).cursor();
		for ( final Cursor< L > lc = Views.flatIterable( labels ).cursor(); lc.hasNext(); )
		{
			final L label = lc.next();
			final A a = ac.next();
			if ( label.getIntegerLong() == 0l && threshold.test( a ) )
				label.setInteger( id++ );
		}

		final UnionFindSparse unionFind = new UnionFindSparse();
		final L zero = Util.getTypeFromInterval( labels ).createVariable();
		zero.setZero();
		final int numDim = labels.numDimensions();
		final TLongArrayList seedLocations = new TLongArrayList();
		for ( int d = 0; d < numDim; ++d )
		{
			final long[] min = Intervals.minAsLongArray( labels );
			final long[] max = Intervals.maxAsLongArray( labels );
			final long[] minPlusOne = min.clone();
			final long[] maxMinusOne = max.clone();
			minPlusOne[ d ] += 1;
			maxMinusOne[ d ] -= 1;
			final IntervalView< L > lower = Views.interval( labels, new FinalInterval( min, maxMinusOne ) );
			final IntervalView< L > upper = Views.interval( labels, new FinalInterval( minPlusOne, max ) );
			for ( Cursor< L > lc = Views.flatIterable( lower ).cursor(), uc = Views.flatIterable( upper ).cursor(); lc.hasNext(); )
			{
				final L l = lc.next();
				final L u = uc.next();
				if ( !zero.valueEquals( l ) && !zero.valueEquals( u ) )
				{
					LOG.trace( "Joining {} {} {} {} {}", l, u, unionFind.size(), maximumSeeds.size(), id );
					final long r1 = unionFind.findRoot( l.getIntegerLong() );
					final long r2 = unionFind.findRoot( u.getIntegerLong() );
					unionFind.join( r1, r2 );
				}
				else if ( zero.valueEquals( l ) && !zero.valueEquals( u ) )
					seedLocations.add( IntervalIndexer.positionToIndexForInterval( uc, labels ) );
				else if ( !zero.valueEquals( l ) && zero.valueEquals( u ) )
					seedLocations.add( IntervalIndexer.positionToIndexForInterval( lc, labels ) );
			}
		}

		for ( final L l : Views.flatIterable( labels ) )
			if ( !zero.valueEquals( l ) )
				l.setInteger( unionFind.findRoot( l.getInteger() ) );

		final List< Point > seeds = new ArrayList<>();

		for ( final TLongIterator location = seedLocations.iterator(); location.hasNext(); )
		{
			final Point p = new Point( labels.numDimensions() );
			IntervalIndexer.indexToPositionForInterval( location.next(), labels, p );
			seeds.add( p );
		}

		LOG.trace( "Running watersheds for {} {}", Arrays.toString( Intervals.minAsLongArray( labels ) ), Arrays.toString( Intervals.maxAsLongArray( labels ) ) );
		Watersheds.flood(
				Views.extendValue( Views.interval( affs, labels ), negativeInfinityExtension ),
				Views.extendZero( labels ),
				labels,
				seeds,
				new DiamondShape( 1 ),
				dist,
				factory );


		return id;

	}

	public static class IntervalsToString
	{
		private final Interval interval;

		public IntervalsToString( final Interval interval )
		{
			super();
			this.interval = interval;
		}

		@Override
		public String toString()
		{
			return "( " + Arrays.toString( Intervals.minAsLongArray( interval ) ) + " " + Arrays.toString( Intervals.maxAsLongArray( interval ) ) + " )";
		}
	}


}