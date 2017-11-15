package de.hanslovsky.examples;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleBiFunction;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.function.Function;

import bdv.img.h5.H5Utils;
import ij.ImageJ;
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
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.Composite;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class InitialSeededWatershedBlock
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.DEBUG );
	}

	public static class RunWatersheds implements Function< ArrayImg< FloatType, FloatArray >, Tuple2< ArrayImg< LongType, LongArray >, Long > >
	{

		private final boolean processBySlice;

		private final double dtWeight;

		public RunWatersheds( final boolean processBySlice, final double dtWeight )
		{
			super();
			this.processBySlice = processBySlice;
			this.dtWeight = dtWeight;
		}

		@Override
		public Tuple2< ArrayImg< LongType, LongArray >, Long > call( final ArrayImg< FloatType, FloatArray > affs ) throws Exception
		{
			final CompositeIntervalView< FloatType, RealComposite< FloatType > > collapsed = Views.collapseReal( affs );
			final long[] dims = Intervals.dimensionsAsLongArray( collapsed );
			LOG.debug( "dims: " + Arrays.toString( dims ) );
			final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( dims );
			for ( final LongType l : labels )
				l.set( 0 );

			final ArrayImgFactory< FloatType > fac = new ArrayImgFactory<>();
			final long nLabels;
			if ( dims.length == 3 && this.processBySlice )
			{
				long offset = 1;
				for ( long z = 0; z < dims[ 2 ]; ++z )
					//					System.out.println( "Processing for z=" + z + " " + Views.hyperSlice( labels, 2, z ).numDimensions() );
					offset += process( Views.hyperSlice( collapsed, 2, z ), Views.hyperSlice( labels, 2, z ), fac, new FloatType( 0.5f ), dtWeight, offset );
				nLabels = offset - 1;
			}
			else
				nLabels = process( collapsed, labels, fac, new FloatType( 0.5f ), dtWeight, 1 );

			return new Tuple2<>( labels, nLabels );
		}

	}

	public static < A extends RealType< A >, C extends Composite< A >, L extends IntegerType< L > > int process(
			final RandomAccessibleInterval< C > affs,
			final RandomAccessibleInterval< LongType > labels,
			final ImgFactory< A > fac,
			final A ext,
			final double dtWeight,
			final long firstLabel ) throws InterruptedException, ExecutionException
	{
		final int nDim = labels.numDimensions();
		final double norm = 1.0 / nDim;
		final RandomAccessibleInterval< A > avg = Converters.convert( affs, ( s, t ) -> {
			t.setZero();
			int count = 0;
			for ( int i = 0; i < nDim; ++i )
				if ( ! Double.isNaN( s.get( i ).getRealDouble() ) ) {
					t.add( s.get( i ) );
					++count;
				}
			if ( count > 0 )
				t.mul( 1.0 / count );
			else
				t.setReal( 0.0 );
		}, ext.createVariable() );
		final Img< A > dt = fac.create( affs, ext );
		final Img< A > avgCopy = fac.create( avg, ext );
		for ( final Pair< A, A > p : Views.interval( Views.pair( avg, avgCopy ), avgCopy ) )
			p.getB().set( p.getA() );
		DistanceTransform.transform( Converters.convert( ( RandomAccessibleInterval< A > ) avgCopy, ( s, t ) -> {
			t.setReal( s.getRealDouble() > 0.5 ? 1e20 : 0 );
		}, ext.createVariable() ), dt, DISTANCE_TYPE.EUCLIDIAN, 1, 1.0 );

		final A thresh = ext.createVariable();
		thresh.set( ext );
		final LocalExtrema.LocalNeighborhoodCheck< Point, A > check = new LocalExtrema.MaximumCheck<>( thresh );
		final A extremaExtension = ext.createVariable();
		extremaExtension.setReal( -1.0 );
		final ArrayList< Point > extrema = LocalExtrema.findLocalExtrema( Views.interval( Views.extendValue( dt, extremaExtension ), Intervals.expand( dt, 1 ) ), check, Executors.newFixedThreadPool( 1 ) );
		LOG.trace( "Found extrema: " + extrema );

		final ToDoubleBiFunction< A, A > dist = ( comparison, reference ) -> comparison.getRealDouble() > ext.getRealDouble() ? 1.0 - comparison.getRealDouble() : 0.9999;
//		System.out.println( Arrays.toString( Intervals.minAsLongArray( labels ) ) + " " + Arrays.toString( Intervals.minAsLongArray( affs ) ) + " " + Arrays.toString( Intervals.minAsLongArray( avgCopy ) ) );
//		System.out.println( extrema );

		final AtomicLong id = new AtomicLong( firstLabel );
		final RandomAccess< LongType > labelAccess = labels.randomAccess();
		extrema.forEach( extremum -> {
			labelAccess.setPosition( extremum );
			labelAccess.get().set( id.getAndIncrement() );
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

	public static void main( final String[] args ) throws Exception
	{
		new ImageJ();

		PropertyConfigurator.configure( new File( "resources/log4j.properties" ).getAbsolutePath() );

		final String path = Util.HOME_DIR + "/Downloads/excerpt.h5";
		final String ds = "main";
//		final String path = "/groups/saalfeld/home/hanslovskyp/from_papec/for_philipp/sampleA+/unet_lr_sampleA+_gpV1.h5";
//		final String ds = "data";
		final RandomAccessibleInterval< FloatType > input = H5Utils.loadFloat( path, ds, new int[] { 300, 300, 100, 3 } );
//		final String path = Util.HOME_DIR + "/local/affinities/tstvol-520-2-h5.h5";
//		final String path = "/nrs/saalfeld/hanslovskyp/watersheds-examples/setup61-200000-sample_C.augmented.0.hdf";
//		final CellImg< FloatType, ?, ? > input = H5Utils.loadFloat( path, "main", new int[] { 1708, 1827, 153, 3 } );
		final ArrayImg< FloatType, FloatArray > affs = ArrayImgs.floats( Intervals.dimensionsAsLongArray( input ) );

		final int nDim = affs.numDimensions() - 1;
		final double norm = 1.0 / nDim;
		final RandomAccessibleInterval< FloatType > avg = Converters.convert( Views.collapseReal( affs ), ( s, t ) -> {
			t.setZero();
			for ( int i = 0; i < nDim; ++i )
				t.add( s.get( i ) );
			t.mul( norm );
		}, new FloatType() );

		final int[] perm = Util.getFlipPermutation( input.numDimensions() - 1 );
		final RandomAccessibleInterval< FloatType > inputPerm = Views.permuteCoordinates( input, perm, input.numDimensions() - 1 );

		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( inputPerm, affs ), affs ) )
			p.getB().set( p.getA() );

		final RunWatersheds rw = new RunWatersheds( true, 0.001 );
		final Tuple2< ArrayImg< LongType, LongArray >, Long > labelsAndCounts = rw.call( affs );
		LOG.info( "Found " + labelsAndCounts._2() + " distinct labels." );

		final int[] colors = new int[ labelsAndCounts._2().intValue() + 1 ];
		final Random rng = new Random( 100 );
		colors[ 0 ] = 0;
		for ( int i = 1; i < colors.length; ++i )
			colors[i] = rng.nextInt();

//		System.out.println( Arrays.toString( Intervals.dimensionsAsLongArray( labelsAndCounts._1() ) ) );
		System.out.println( "Saving to h5..." );
		H5Utils.saveUnsignedLong( labelsAndCounts._1(), path, "zws", Intervals.dimensionsAsIntArray( labelsAndCounts._1() ) );
		System.out.println( "Done saving to h5..." );

		ImageJFunctions.show( avg, "aff avg" );
		ImageJFunctions.show( Converters.convert( ( RandomAccessibleInterval< LongType > ) labelsAndCounts._1(), ( s, t ) -> {
			t.set( colors[ Math.max( s.getInteger(), 0 ) ] );
		}, new ARGBType() ), "labels" );
	}


}
