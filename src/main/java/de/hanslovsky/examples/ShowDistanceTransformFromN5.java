package de.hanslovsky.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.sun.javafx.application.PlatformImpl;

import bdv.bigcat.viewer.atlas.Atlas;
import bdv.bigcat.viewer.atlas.data.DataSource;
import bdv.bigcat.viewer.atlas.data.LabelDataSourceFromDelegates;
import bdv.bigcat.viewer.atlas.data.RandomAccessibleIntervalDataSource;
import bdv.bigcat.viewer.state.FragmentSegmentAssignmentWithHistory;
import bdv.util.volatiles.SharedQueue;
import gnu.trove.map.hash.TLongLongHashMap;
import javafx.application.Platform;
import javafx.stage.Stage;
import net.imglib2.FinalInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.type.volatiles.VolatileFloatType;
import net.imglib2.type.volatiles.VolatileUnsignedByteType;
import net.imglib2.type.volatiles.VolatileUnsignedLongType;
import net.imglib2.util.Intervals;

public class ShowDistanceTransformFromN5
{

	public static void main( final String[] args ) throws IOException
	{
		PlatformImpl.startup( () -> {} );

		final String n5Path = "/groups/saalfeld/home/hanslovskyp/from_heinrichl/distance/gt/n5";// "/groups/saalfeld/home/hanslovskyp/from_heinrichl/distance/first-300/n5";
		final String raw = "raw";
		final String dist = "distance-transform";
		final String sv = "spark-supervoxels-merged";
		final String svUnmerged = "spark-supervoxels";
		final String[] lowers = IntStream.range( 0, 3 ).mapToObj( i -> "spark-supervoxels-merged-lower-" + i ).toArray( String[]::new );
		final String[] uppers = IntStream.range( 0, 3 ).mapToObj( i -> "spark-supervoxels-merged-upper-" + i ).toArray( String[]::new );

		final String fbPath = "/home/hanslovskyp/workspace/imglib2-watersheds/imglib2-algorithm-watershed-examples-spark/first-block";
		final N5Reader fbReader = N5.openFSReader( fbPath );

		final int numPriorities = 20;
		final SharedQueue sharedQueue = new SharedQueue( 1, numPriorities );

		final double[] resolution = new double[] { 4, 4, 4 };
		final double[][] resolutions = new double[ lowers.length ][];
		final double[][] offsets = new double[ lowers.length ][];
		for ( int i = 0; i < resolution.length; ++i )
		{
			resolutions[ i ] = resolution.clone();
			resolutions[ i ][ i ] *= 25;
			offsets[ i ] = new double[ resolution.length ];
			offsets[ i ][ i ] = 25 * resolution[ i ] / 2 - resolution[ i ] / 2;
		}

		final N5Reader reader = N5.openFSReader( n5Path );

		final DataSource< UnsignedByteType, VolatileUnsignedByteType > rawSource =
				DataSource.createN5RawSource( "raw", reader, raw, resolution, sharedQueue, 0, UnsignedByteType::new, VolatileUnsignedByteType::new );

		final DataSource< FloatType, VolatileFloatType > distSource =
				DataSource.createN5RawSource( "dist", reader, dist, resolution, sharedQueue, 0, FloatType::new, VolatileFloatType::new );

		final RandomAccessibleIntervalDataSource< UnsignedLongType, VolatileUnsignedLongType > labelSource =
				DataSource.createN5RawSource( "supervoxels", reader, sv, resolution, sharedQueue, 0, UnsignedLongType::new, VolatileUnsignedLongType::new );
		final LabelDataSourceFromDelegates< UnsignedLongType, VolatileUnsignedLongType > delegated = new LabelDataSourceFromDelegates<>( labelSource, noBroadcastOrReceiveAssignment() );

		final RandomAccessibleIntervalDataSource< UnsignedLongType, VolatileUnsignedLongType > labelSourceUnmerged =
				DataSource.createN5RawSource( "supervoxels unmerged", reader, svUnmerged, resolution, sharedQueue, 0, UnsignedLongType::new, VolatileUnsignedLongType::new );
		final LabelDataSourceFromDelegates< UnsignedLongType, VolatileUnsignedLongType > delegatedUnmerged = new LabelDataSourceFromDelegates<>( labelSourceUnmerged, noBroadcastOrReceiveAssignment() );

		final List< LabelDataSourceFromDelegates< UnsignedLongType, VolatileUnsignedLongType > > lowerSources = IntStream
				.range( 0, lowers.length )
				.mapToObj( i -> {
					try
					{
						return DataSource.createN5RawSource( lowers[ i ], reader, lowers[ i ], resolutions[ i ], offsets[ i ], sharedQueue, 0, UnsignedLongType::new, VolatileUnsignedLongType::new );
					}
					catch ( final IOException e1 )
					{
						throw new RuntimeException( e1 );
					}
				} )
				.map( src -> new LabelDataSourceFromDelegates<>( src, noBroadcastOrReceiveAssignment() ) )
				.collect( Collectors.toList() );

		final List< LabelDataSourceFromDelegates< UnsignedLongType, VolatileUnsignedLongType > > upperSources = IntStream
				.range( 0, lowers.length )
				.mapToObj( i -> {
					try
					{
						return DataSource.createN5RawSource( uppers[ i ], reader, uppers[ i ], resolutions[ i ], offsets[ i ], sharedQueue, 0, UnsignedLongType::new, VolatileUnsignedLongType::new );
					}
					catch ( final IOException e1 )
					{
						throw new RuntimeException( e1 );
					}
				} )
				.map( src -> new LabelDataSourceFromDelegates<>( src, noBroadcastOrReceiveAssignment() ) )
				.collect( Collectors.toList() );




		final double[] min = Arrays.stream( Intervals.minAsLongArray( rawSource.getSource( 0, 0 ) ) ).mapToDouble( v -> v ).toArray();
		final double[] max = Arrays.stream( Intervals.maxAsLongArray( rawSource.getSource( 0, 0 ) ) ).mapToDouble( v -> v ).toArray();
		final AffineTransform3D affine = new AffineTransform3D();
		rawSource.getSourceTransform( 0, 0, affine );
		affine.apply( min, min );
		affine.apply( max, max );

		final Atlas viewer = new Atlas(
				new FinalInterval( Arrays.stream( min ).mapToLong( Math::round ).toArray(),
						Arrays.stream( max ).mapToLong( Math::round ).toArray() ),
				sharedQueue );

		final CountDownLatch latch = new CountDownLatch( 1 );
		Platform.runLater( () -> {
			final Stage stage = new Stage();
			try
			{
				viewer.start( stage );
			}
			catch ( final InterruptedException e )
			{
				e.printStackTrace();
			}

			stage.show();
			latch.countDown();
		} );
		viewer.addRawSource( rawSource, 0, 255 );
		viewer.addRawSource( distSource, 0, 1.0 );
		viewer.addLabelSource( delegated, t -> t.get().getIntegerLong() );
		viewer.addLabelSource( delegatedUnmerged, t -> t.get().getIntegerLong() );
//		viewer.addLabelSource( lowerSources.get( 0 ), t -> t.get().getIntegerLong() );
//		viewer.addLabelSource( upperSources.get( 0 ), t -> t.get().getIntegerLong() );
//		final Cursor< UnsignedLongType > l = Views.flatIterable( lowerSources.get( 0 ).getDataSource( 0, 0 ) ).cursor();
//		final Cursor< UnsignedLongType > u = Views.flatIterable( upperSources.get( 0 ).getDataSource( 0, 0 ) ).cursor();
//		while ( l.hasNext() )
//			if ( !l.next().valueEquals( u.next() ) )
//				System.out.println( "Differs at " + new Point( l ) + " " + new Point( u ));
//		System.out.println( lowerSources.get( 0 ).getName() );
//		System.out.println( upperSources.get( 0 ).getName() );
	}

	public static FragmentSegmentAssignmentWithHistory noBroadcastOrReceiveAssignment()
	{
		return new FragmentSegmentAssignmentWithHistory( new TLongLongHashMap(), action -> {}, () -> {
			try
			{
				Thread.sleep( Long.MAX_VALUE );
			}
			catch ( final InterruptedException e )
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return new TLongLongHashMap();
		} );
	}

}
