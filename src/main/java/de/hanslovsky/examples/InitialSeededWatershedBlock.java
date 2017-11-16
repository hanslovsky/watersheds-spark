package de.hanslovsky.examples;

import java.io.File;
import java.lang.invoke.MethodHandles;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bdv.img.h5.H5Utils;
import ij.ImageJ;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;

public class InitialSeededWatershedBlock
{

	public static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );



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

//		final RunWatersheds< FloatType, LongType > rw = new RunWatersheds<>( true, 0.001, new FloatType( 0.5f ), new LongType() );
//		final Tuple2< RandomAccessibleInterval< LongType >, Long > labelsAndCounts = rw.call( new Tuple2<>( Views.collapse( affs ), affs ) );
//		LOG.info( "Found " + labelsAndCounts._2() + " distinct labels." );
//
//		final int[] colors = new int[ labelsAndCounts._2().intValue() + 1 ];
//		final Random rng = new Random( 100 );
//		colors[ 0 ] = 0;
//		for ( int i = 1; i < colors.length; ++i )
//			colors[i] = rng.nextInt();
//
////		System.out.println( Arrays.toString( Intervals.dimensionsAsLongArray( labelsAndCounts._1() ) ) );
//		System.out.println( "Saving to h5..." );
//		H5Utils.saveUnsignedLong( labelsAndCounts._1(), path, "zws", Intervals.dimensionsAsIntArray( labelsAndCounts._1() ) );
//		System.out.println( "Done saving to h5..." );
//
//		ImageJFunctions.show( avg, "aff avg" );
//		ImageJFunctions.show( Converters.convert( labelsAndCounts._1(), ( s, t ) -> {
//			t.set( colors[ Math.max( s.getInteger(), 0 ) ] );
//		}, new ARGBType() ), "labels" );
	}


}
