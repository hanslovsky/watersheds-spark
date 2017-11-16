package de.hanslovsky.examples;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleBiFunction;
import java.util.stream.LongStream;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;

import net.imglib2.Interval;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.localextrema.LocalExtrema;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.algorithm.morphology.watershed.HierarchicalPriorityQueueQuantized;
import net.imglib2.algorithm.morphology.watershed.Watersheds;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.converter.Converters;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.Composite;
import scala.Tuple2;

public class RunWatersheds< T extends RealType< T > & NativeType< T >, L extends IntegerType< L > & NativeType< L > >
implements Function< Tuple2< Interval, RandomAccessible< ? extends Composite< T > > >, Tuple2< RandomAccessibleInterval< L >, Long > >
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	private final boolean processBySlice;

	private final double dtWeight;

	private final Broadcast< T > extension;

	private final Broadcast< L > l;

	public RunWatersheds( final JavaSparkContext sc, final boolean processBySlice, final double dtWeight, final T extension, final L l )
	{
		super();
		this.processBySlice = processBySlice;
		this.dtWeight = dtWeight;
		this.extension = sc.broadcast( extension );
		this.l = sc.broadcast( l );
	}

	@Override
	public Tuple2< RandomAccessibleInterval< L >, Long > call( final Tuple2< Interval, RandomAccessible< ? extends Composite< T > > > offsetAndAffs ) throws Exception
	{
		LOG.debug( "Calculating watersheds for interval " + Arrays.toString( Intervals.minAsLongArray( offsetAndAffs._1() ) ) + " " + Arrays.toString( Intervals.maxAsLongArray( offsetAndAffs._1() ) ) );
		final Interval interval = offsetAndAffs._1();
		final long[] imageOffset = Intervals.minAsLongArray( interval );
		final RandomAccessible< ? extends Composite< T > > affinities = offsetAndAffs._2();
		final long[] dims = Intervals.dimensionsAsLongArray( interval );
		final RandomAccessibleInterval< L > labels = Views.translate( new ArrayImgFactory< L >().create( dims, l.getValue() ), imageOffset );

		final ArrayImgFactory< T > fac = new ArrayImgFactory<>();
		final long nLabels;
		if ( dims.length == 3 && this.processBySlice )
		{
			long offset = 1;
			for ( long z = interval.min( 2 ); z <= interval.max( 2 ); ++z )
			{
				final RandomAccessibleInterval< ? extends Composite< T > > hs = Views.hyperSlice( Views.interval( affinities, interval ), 2, z );
				offset += process( hs, Views.hyperSlice( labels, 2, z ), fac, extension.getValue(), dtWeight, offset );
			}
			nLabels = offset - 1;
		}
		else
			nLabels = process( affinities, labels, fac, extension.getValue(), dtWeight, 1 );

		return new Tuple2<>( labels, nLabels );
	}

	public static < A extends RealType< A >, C extends Composite< A >, L extends IntegerType< L > > int process(
			final RandomAccessible< C > affs,
			final RandomAccessibleInterval< L > labels,
			final ImgFactory< A > fac,
			final A ext,
			final double dtWeight,
			final long firstLabel ) throws InterruptedException, ExecutionException
	{
		final int nDim = labels.numDimensions();
		final RandomAccessibleInterval< A > avg = Converters.convert( ( RandomAccessibleInterval< C > ) Views.interval( affs, labels ), ( s, t ) -> {
			t.setZero();
			int count = 0;
			for ( int i = 0; i < nDim; ++i )
				if ( !Double.isNaN( s.get( i ).getRealDouble() ) )
				{
					t.add( s.get( i ) );
					++count;
				}
			if ( count > 0 )
				t.mul( 1.0 / count );
			else
				t.setReal( 0.0 );
		}, ext.createVariable() );
		final RandomAccessibleInterval< A > dt = Views.translate( fac.create( labels, ext ), Intervals.minAsLongArray( labels ) );
		final RandomAccessibleInterval< A > avgCopy = Views.translate( fac.create( labels, ext ), Intervals.minAsLongArray( labels ) );
		for ( final Pair< A, A > p : Views.interval( Views.pair( avg, avgCopy ), avgCopy ) )
			p.getB().set( p.getA() );
		LOG.debug( "Distance transform with interval {} {}", Arrays.toString( Intervals.minAsLongArray( avgCopy ) ), Arrays.toString( Intervals.maxAsLongArray( avgCopy ) ) );
		DistanceTransform.transform( Converters.convert( Views.extendValue( avgCopy, ext ), ( s, t ) -> {
			t.setReal( s.getRealDouble() > 0.5 ? 1e20 : 0 );
		}, ext.createVariable() ), dt, DISTANCE_TYPE.EUCLIDIAN, 1, 1.0 );

		final LocalExtrema.LocalNeighborhoodCheck< Point, A > check = new LocalExtrema.MaximumCheck<>( ext );
		final A extremaExtension = ext.createVariable();
		extremaExtension.setReal( Double.NEGATIVE_INFINITY );
		final ArrayList< Point > extrema = LocalExtrema.findLocalExtrema( Views.expandValue( dt, extremaExtension, LongStream.generate( () -> 1 ).limit( dt.numDimensions() ).toArray() ), check, MoreExecutors.newDirectExecutorService() );
		LOG.trace( "Found extrema: " + extrema );

		final ToDoubleBiFunction< A, A > dist = ( comparison, reference ) -> comparison.getRealDouble() > ext.getRealDouble() ? 1.0 - comparison.getRealDouble() : 0.9999;
//		System.out.println( Arrays.toString( Intervals.minAsLongArray( labels ) ) + " " + Arrays.toString( Intervals.minAsLongArray( affs ) ) + " " + Arrays.toString( Intervals.minAsLongArray( avgCopy ) ) );
//		System.out.println( extrema );

		final AtomicLong id = new AtomicLong( firstLabel );
		final RandomAccess< L > labelAccess = labels.randomAccess();
		extrema.forEach( extremum -> {
			LOG.trace( "Extremum {} for interval {} {}", extremum, Arrays.toString( Intervals.minAsLongArray( labels ) ), Arrays.toString( Intervals.maxAsLongArray( labels ) ) );
			labelAccess.setPosition( extremum );
			labelAccess.get().setInteger( id.getAndIncrement() );
		} );
		// WHY CAN I NOT USE AVG? WHY DOES IT USE FIRST SLICE OF AFFINITIES
		// THEN? WHYYYYYYYY?
		// WHY DO I NEED TO COPY AVG? THIS SUCKS!!!
		final A ext2 = ext.createVariable();
		ext2.setReal( Double.POSITIVE_INFINITY );
//		System.out.println( Arrays.toString( Intervals.minAsLongArray( labels ) ) + " " + Arrays.toString( Intervals.maxAsLongArray( labels ) ) + " " + Arrays.toString( Intervals.minAsLongArray( avgCopy ) ) + " " + Arrays.toString( Intervals.maxAsLongArray( avgCopy ) ) );
		Watersheds.flood(
				( RandomAccessible< A > ) Views.extendValue( avgCopy, ext2 ), // dt,
				Views.extendZero( labels ),
				labels,
				extrema,
				new DiamondShape( 1 ),
				dist,
				new HierarchicalPriorityQueueQuantized.Factory( 256, 0.0, 1.0 ) );

		return extrema.size();

	}


}