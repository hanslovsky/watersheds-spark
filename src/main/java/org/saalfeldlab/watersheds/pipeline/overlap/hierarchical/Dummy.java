package org.saalfeldlab.watersheds.pipeline.overlap.hierarchical;

import java.io.IOException;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import net.imglib2.Cursor;
import net.imglib2.Point;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.Views;

public class Dummy
{

	public static void main( final String[] args ) throws IOException
	{
		final String group = "/groups/saalfeld/home/hanslovskyp/from_heinrichl/fib25_sub_prediction_at_296000.n5/subset";
		final String dataset = "supervoxels-merged";
		final N5FSReader n5 = new N5FSReader( group );
		final RandomAccessibleInterval< UnsignedLongType > data = N5Utils.openVolatile( n5, dataset );
		System.out.println( Arrays.toString( n5.getDatasetAttributes( dataset ).getBlockSize() ) );
		final long sum = 0;
		final Cursor< UnsignedLongType > c = Views.flatIterable( data ).cursor();
		while ( c.hasNext() )
			try {
				c.next().get();
			} catch( final Exception e ) {
				System.out.println( e.getMessage() );
				System.out.println( new Point( c ) );
				throw e;
			}

		System.out.println( sum );
	}

}
