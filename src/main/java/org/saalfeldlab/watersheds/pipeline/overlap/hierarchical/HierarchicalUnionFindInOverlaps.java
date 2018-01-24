package org.saalfeldlab.watersheds.pipeline.overlap.hierarchical;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.saalfeldlab.watersheds.UnionFindSparse;
import org.saalfeldlab.watersheds.Util;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Dimensions;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import scala.Tuple2;

public class HierarchicalUnionFindInOverlaps
{

	public static void createOverlaps(
			final JavaSparkContext sc,
			final CellGrid grid,
			final BiConsumer< Tuple2< long[], long[] >, UnionFindSparse > populateUnionFind,
			final String group,
			final String upperStripDatasetPattern,
			final String lowerStripDatasetPattern,
			final BiFunction< Integer, long[], String > unionFindSerializationPattern )
	{
		final int[] blockSize = IntStream.range( 0, grid.numDimensions() ).map( grid::cellDimension ).toArray();
		final long[] dims = grid.getImgDimensions();
		final int nDim = dims.length;

		final List< HashWrapper< long[] > > blocks = Util.collectAllOffsets( dims, blockSize, HashWrapper::longArray );
		final JavaRDD< HashWrapper< long[] > > blocksRDD = sc.parallelize( blocks );
		final int multiplier = 2;

		final Broadcast< BiConsumer< Tuple2< long[], long[] >, UnionFindSparse > > populateUnionFindBC = sc.broadcast( populateUnionFind );

		// need to start with factor 2 for every other block
		for ( int factor = 2; checkIfMoreThanOneBlock( dims, blockSize ); factor *= multiplier )
		{
			final int step = factor;
			final int offset = factor / multiplier - 1;
			final JavaPairRDD< HashWrapper< long[] >, Tuple2< long[], long[] > > localAssignments = blocksRDD.mapToPair( blockMinimum -> {

				final TLongLongHashMap parents = new TLongLongHashMap();
				final TLongLongHashMap ranks = new TLongLongHashMap();
				final UnionFindSparse uf = new UnionFindSparse( parents, ranks, 0 );

				final CellGrid cellGrid = new CellGrid( dims, blockSize );
				final long[] min = blockMinimum.getData().clone();
				final long[] cellPos = new long[ min.length ];
				cellGrid.getCellPosition( min, cellPos );

				final N5FSWriter writer = new N5FSWriter( group );

				@SuppressWarnings( "unchecked" )
				final DataBlock< long[] >[] lowers = new DataBlock[ nDim ];
				@SuppressWarnings( "unchecked" )
				final DataBlock< long[] >[] uppers = new DataBlock[ nDim ];
				final DatasetAttributes[] lowerAttributes = new DatasetAttributes[ nDim ];
				final DatasetAttributes[] upperAttributes = new DatasetAttributes[ nDim ];

				for ( int d = 0; d < nDim; ++d )
				{

					final String lowerDataset = String.format( lowerStripDatasetPattern, d );
					final String upperDataset = String.format( upperStripDatasetPattern, d );
					final DatasetAttributes lowerAttrs = writer.getDatasetAttributes( lowerDataset );
					final DatasetAttributes upperAttrs = writer.getDatasetAttributes( upperDataset );
					lowerAttributes[ d ] = lowerAttrs;
					upperAttributes[ d ] = upperAttrs;
					@SuppressWarnings( "unchecked" )
					final DataBlock< long[] > lowerBlock = ( DataBlock< long[] > ) writer.readBlock( lowerDataset, lowerAttrs, cellPos );
					@SuppressWarnings( "unchecked" )
					final DataBlock< long[] > upperBlock = ( DataBlock< long[] > ) writer.readBlock( upperDataset, upperAttrs, cellPos );
					lowers[ d ] = lowerBlock;
					uppers[ d ] = upperBlock;

					final long cellPosInDimension = cellPos[ d ];
					if ( ( cellPosInDimension - offset ) % step == 0 && cellPosInDimension + 1 < cellGrid.gridDimension( d ) )
					{

						final long[] otherCellPos = cellPos.clone();
						otherCellPos[ d ] += 1;
						final long[] lowerForOtherBlock = ( long[] ) writer.readBlock( String.format( lowerStripDatasetPattern, d ), lowerAttrs, otherCellPos ).getData();
						// find matches and add to union find
						populateUnionFindBC.getValue().accept( new Tuple2<>( lowerForOtherBlock, upperBlock.getData() ), uf );
					}
				}


				for ( int d = 0; d < lowers.length; ++d )
				{
					final long[] lowerForBlock = lowers[ d ].getData();
					final long[] upperForBlock = uppers[ d ].getData();
					// relabel data (both lower and upper)
					for ( int i = 0; i < lowerForBlock.length; ++i )
					{
						final long v = lowerForBlock[ i ];
						if ( v != 0 )
							lowerForBlock[ i ] = uf.findRoot( v );
					}
					for ( int i = 0; i < upperForBlock.length; ++i )
					{
						final long v = upperForBlock[ i ];
						if ( v != 0 )
							upperForBlock[ i ] = uf.findRoot( v );
					}
					writer.writeBlock( String.format( lowerStripDatasetPattern, d ), lowerAttributes[ d ], new LongArrayDataBlock( lowers[ d ].getSize(), lowers[ d ].getGridPosition(), lowerForBlock ) );
					writer.writeBlock( String.format( upperStripDatasetPattern, d ), upperAttributes[ d ], new LongArrayDataBlock( uppers[ d ].getSize(), uppers[ d ].getGridPosition(), upperForBlock ) );
					Views.extendBorder( null );
				}

				final long[] targetCellPos = cellPos.clone();

				for ( int d = 0; d < cellPos.length; ++d )
					targetCellPos[ d ] /= step;


				for ( final TLongLongIterator it = parents.iterator(); it.hasNext(); )
				{
					it.advance();
					uf.findRoot( it.key() );
				}

				return new Tuple2<>( HashWrapper.longArray( targetCellPos ), new Tuple2<>( parents.keys(), parents.values() ) );
			} );

			localAssignments
			.aggregateByKey( new ArrayList< Tuple2< long[], long[] > >(), ( al, t ) -> {
				al.add( t );
				return al;
			},
					( al1, al2 ) -> {
						final ArrayList< Tuple2< long[], long[] > > al = new ArrayList<>();
						al.addAll( al1 );
						al.addAll( al2 );
						return al;
					} )
			.mapValues( list -> {

				final TLongLongHashMap p = new TLongLongHashMap();
				final TLongLongHashMap r = new TLongLongHashMap();
				final UnionFindSparse uf = new UnionFindSparse( p, r, 0 );

				list.forEach( t -> {
					final long[] k = t._1();
					final long[] v = t._2();
					for ( int i = 0; i < k.length; ++i )
						uf.join( uf.findRoot( k[ i ] ), uf.findRoot( v[ i ] ) );
				} );

				//				System.out.println( "GOT THESE MATCHES !!! " + p );

				return new Tuple2<>( p.keys(), p.values() );
			} )
			.map( t -> {
				final String path = unionFindSerializationPattern.apply( step, t._1().getData().clone() );
				final File f = new File( path );
				f.getParentFile().mkdirs();
				f.createNewFile();

				final long[] keys = t._2()._1();
				final long[] values = t._2()._2();

				final byte[] data = new byte[ Integer.BYTES + keys.length * Long.BYTES * 2 ];
				final ByteBuffer dataBuffer = ByteBuffer.wrap( data );
				dataBuffer.putInt( keys.length );
				Arrays.stream( keys ).forEach( dataBuffer::putLong );
				Arrays.stream( values ).forEach( dataBuffer::putLong );

				//				final byte[] kBytes = new byte[ keys.length * Long.BYTES ];
				//				final byte[] vBytes = new byte[ values.length * Long.BYTES ];
				//				final ByteBuffer kBB = ByteBuffer.wrap( kBytes );
				//				final ByteBuffer vBB = ByteBuffer.wrap( vBytes );
				//
				//				for ( int i = 0; i < keys.length; ++i )
				//				{
				//					if ( keys[ i ] == 13682 || values[ i ] == 13682 )
				//						System.out.println( "WWWWAAAAAAS!" + " " + keys[ i ] + " " + values[ i ] + " " + step + " " + Arrays.toString( t._1().getData().clone() ) );
				//					kBB.putLong( keys[ i ] );
				//					vBB.putLong( values[ i ] );
				//				}

				try (final FileOutputStream fos = new FileOutputStream( f ))
				{
					fos.write( data );
				}

				try (final FileInputStream fis = new FileInputStream( f ))
				{
					final byte[] readData = new byte[ (int)f.length() ];
					fis.read( readData );

							if ( ByteBuffer.wrap( readData ).getInt() != keys.length || readData.length != data.length )
						throw new RuntimeException( "SOMETHING WRONG! ");
				}

				return true;
			} )
			.count();

			for ( int d = 0; d < nDim; ++d )
				blockSize[ d ] *= multiplier;



		}
	}

	public static boolean checkIfMoreThanOneBlock( final Dimensions dim, final int[] blockSize )
	{
		return checkIfMoreThanOneBlock( Intervals.dimensionsAsLongArray( dim ), blockSize );
	}

	public static boolean checkIfMoreThanOneBlock( final long[] dim, final int[] blockSize )
	{
		final CellGrid grid = new CellGrid( dim, blockSize );
//		System.out.println( "WAAAS ? " + Arrays.toString( grid.getGridDimensions() ) + " " + Arrays.toString( dim ) + " " + Arrays.toString( blockSize ) );
		return Arrays.stream( grid.getGridDimensions() ).reduce( 1, ( l1, l2 ) -> l1 * l2 ) > 1;
	}


}
