package de.hanslovsky.examples;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleBiFunction;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.Localizable;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.localextrema.LocalExtrema;
import net.imglib2.algorithm.morphology.watershed.PriorityQueueFactory;
import net.imglib2.algorithm.morphology.watershed.Watersheds;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import scala.Tuple2;

public class RunWatershedsOn< T extends RealType< T > & NativeType< T >, L extends IntegerType< L > & NativeType< L > >
implements Function< Tuple2< Interval, RandomAccessible< T > >, Tuple2< RandomAccessibleInterval< L >, Long > >
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	private final boolean processBySlice;

	private final Broadcast< LocalExtrema.LocalNeighborhoodCheck< ? extends Localizable, T > > extremumCheck;

	private final Broadcast< ToDoubleBiFunction< T, T > > dist;

	private final Broadcast< L > l;

	private final Broadcast< PriorityQueueFactory > factory;

	public RunWatershedsOn(
			final JavaSparkContext sc,
			final boolean processBySlice,
			final LocalExtrema.LocalNeighborhoodCheck< ? extends Localizable, T > extremumCheck,
			final ToDoubleBiFunction< T, T > dist,
			final L l,
			final PriorityQueueFactory factory )
	{
		super();
		this.processBySlice = processBySlice;
		this.extremumCheck = sc.broadcast( extremumCheck );
		this.dist = sc.broadcast( dist );
		this.l = sc.broadcast( l );
		this.factory = sc.broadcast( factory );
	}

	@Override
	public Tuple2< RandomAccessibleInterval< L >, Long > call( final Tuple2< Interval, RandomAccessible< T > > offsetAndAffs ) throws Exception
	{
		LOG.debug( "Calculating watersheds for interval " + Arrays.toString( Intervals.minAsLongArray( offsetAndAffs._1() ) ) + " " + Arrays.toString( Intervals.maxAsLongArray( offsetAndAffs._1() ) ) );
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
				nextLabel = process( hs, Views.hyperSlice( labels, 2, z ), extremumCheck.getValue(), dist.getValue(), nextLabel, factory.getValue() );
			}
			nLabels = nextLabel;
		}
		else
			nLabels = process( affinities, labels, extremumCheck.getValue(), dist.getValue(), 1, factory.getValue() );

		LOG.info( "Got {} labels in watershed", nLabels );

		return new Tuple2<>( labels, nLabels );
	}

	public static < A extends RealType< A >, L extends IntegerType< L > > long process(
			final RandomAccessible< A > affs,
			final RandomAccessibleInterval< L > labels,
			final LocalExtrema.LocalNeighborhoodCheck< ? extends Localizable, A > extremumCheck,
			final ToDoubleBiFunction< A, A > dist,
			final long firstLabel,
			final PriorityQueueFactory factory ) throws InterruptedException, ExecutionException
	{
		final List< ? extends Localizable > seeds = LocalExtrema.findLocalExtrema( Views.interval( affs, Intervals.expand( labels, 1 ) ), extremumCheck, MoreExecutors.newDirectExecutorService() );
		final AtomicLong id = new AtomicLong( firstLabel );
		final RandomAccess< L > labelAccess = labels.randomAccess();
		for ( final Localizable seed : seeds )
		{
			labelAccess.setPosition( seed );
			labelAccess.get().setInteger( id.getAndIncrement() );
		}
		final UnionFind unionFind = new UnionFind( ( int ) id.get() );
		final L zero = Util.getTypeFromInterval( labels ).createVariable();
		zero.setZero();
		final int numDim = labels.numDimensions();
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
					LOG.trace( "Joining {} {} {} {} {}", l, u, unionFind.size(), seeds.size(), id );
					final int r1 = unionFind.findRoot( l.getInteger() );
					final int r2 = unionFind.findRoot( u.getInteger() );
					unionFind.join( r1, r2 );
				}
			}
		}

		for ( final L l : Views.flatIterable( labels ) )
			if ( !zero.valueEquals( l ) )
				l.setInteger( unionFind.findRoot( l.getInteger() ) );

		LOG.trace( "Running watersheds for {} {}", Arrays.toString( Intervals.minAsLongArray( labels ) ), Arrays.toString( Intervals.maxAsLongArray( labels ) ) );
		Watersheds.flood(
				Views.extendZero( Views.interval( affs, labels ) ),
				Views.extendZero( labels ),
				labels,
				seeds,
				new DiamondShape( 1 ),
				dist,
				factory );


		return id.get();

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