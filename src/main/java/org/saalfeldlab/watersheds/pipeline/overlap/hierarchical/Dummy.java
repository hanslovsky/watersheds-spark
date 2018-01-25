package org.saalfeldlab.watersheds.pipeline.overlap.hierarchical;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class Dummy
{

	public static void main( final String[] args ) throws IOException
	{
//		final String group = "/groups/saalfeld/home/hanslovskyp/from_heinrichl/fib25_sub_prediction_at_296000.n5/subset";
//		final String dataset = "supervoxels-merged";
//		final N5FSReader n5 = new N5FSReader( group );
//		final RandomAccessibleInterval< UnsignedLongType > data = N5Utils.openVolatile( n5, dataset );
//		System.out.println( Arrays.toString( n5.getDatasetAttributes( dataset ).getBlockSize() ) );
//		final long sum = 0;
//		final Cursor< UnsignedLongType > c = Views.flatIterable( data ).cursor();
//		while ( c.hasNext() )
//			try {
//				c.next().get();
//			} catch( final Exception e ) {
//				System.out.println( e.getMessage() );
//				System.out.println( new Point( c ) );
//				throw e;
//			}
//
//		System.out.println( sum );

		final File f = File.createTempFile( "pre", "suf" );
		System.out.print( f.getAbsolutePath() );

		final int val = 3;

		try( FileOutputStream fos = new FileOutputStream( f ) ) {
			fos.write( val );
			fos.write( 100 );
		}

		try( FileInputStream fis = new FileInputStream( f ) ) {
			final int valRead = fis.read();
			System.out.println( "READ VALUE " + valRead + " " + val );
		}


	}

}
