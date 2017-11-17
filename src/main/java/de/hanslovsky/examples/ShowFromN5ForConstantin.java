package de.hanslovsky.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.sun.javafx.application.PlatformImpl;

import bdv.bigcat.viewer.atlas.Atlas;
import bdv.bigcat.viewer.atlas.data.DataSource;
import bdv.bigcat.viewer.state.FragmentSegmentAssignmentWithHistory;
import bdv.util.volatiles.SharedQueue;
import gnu.trove.map.hash.TLongLongHashMap;
import javafx.application.Platform;
import javafx.stage.Stage;
import net.imglib2.FinalInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.volatiles.VolatileUnsignedByteType;
import net.imglib2.util.Intervals;

public class ShowFromN5ForConstantin
{

	public static void main( final String[] args ) throws IOException
	{
		PlatformImpl.startup( () -> {} );

		final String n5Path = "/groups/saalfeld/home/papec/for_philipp/sampleA+_realigned_out.n5";
		final String raw = "data";

		final int numPriorities = 20;
		final SharedQueue sharedQueue = new SharedQueue( 12, numPriorities );

		final double[] resolution = new double[] { 4, 4, 40 };

		final N5Reader reader = N5.openFSReader( n5Path );

		final DataSource< UnsignedByteType, VolatileUnsignedByteType > rawSource =
				DataSource.createN5RawSource( "raw", reader, raw, resolution, sharedQueue, 0, UnsignedByteType::new, VolatileUnsignedByteType::new );

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
