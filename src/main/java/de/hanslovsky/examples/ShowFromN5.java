package de.hanslovsky.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

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
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.volatiles.VolatileUnsignedByteType;
import net.imglib2.type.volatiles.VolatileUnsignedLongType;
import net.imglib2.util.Intervals;

public class ShowFromN5
{

	public static void main( final String[] args ) throws IOException
	{
		PlatformImpl.startup( () -> {} );

		final String n5Path = "/groups/saalfeld/home/hanslovskyp/from_papec/for_philipp/sampleA+/n5";
		final String raw = "raw";
		final String sv = "spark-supervoxels";

		final int numPriorities = 20;
		final SharedQueue sharedQueue = new SharedQueue( 12, numPriorities );

		final double[] resolution = new double[] { 4, 4, 40 };

		final N5Reader reader = N5.openFSReader( n5Path );

		final DataSource< UnsignedByteType, VolatileUnsignedByteType > rawSource =
				DataSource.createN5RawSource( "raw", reader, raw, resolution, sharedQueue, 0 );

		final RandomAccessibleIntervalDataSource< UnsignedLongType, VolatileUnsignedLongType > labelSource =
				DataSource.createN5RawSource( "supervoxels", reader, sv, resolution, sharedQueue, 0 );
		final LabelDataSourceFromDelegates< UnsignedLongType, VolatileUnsignedLongType > delegated = new LabelDataSourceFromDelegates<>( labelSource, noBroadcastOrReceiveAssignment() );

		final double[] min = Arrays.stream( Intervals.minAsLongArray( rawSource.getSource( 0, 0 ) ) ).mapToDouble( v -> v ).toArray();
		final double[] max = Arrays.stream( Intervals.maxAsLongArray( rawSource.getSource( 0, 0 ) ) ).mapToDouble( v -> v ).toArray();
		final AffineTransform3D affine = new AffineTransform3D();
		rawSource.getSourceTransform( 0, 0, affine );
		affine.apply( min, min );
		affine.apply( max, max );

		final Atlas viewer = new Atlas( sharedQueue );

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
		viewer.addLabelSource( delegated, t -> t.get().getIntegerLong() );
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
