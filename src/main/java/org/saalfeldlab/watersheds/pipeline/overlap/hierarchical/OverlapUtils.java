package org.saalfeldlab.watersheds.pipeline.overlap.hierarchical;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.saalfeldlab.watersheds.UnionFindSparse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Dimensions;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.util.Intervals;

public class OverlapUtils
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static boolean checkIfMoreThanOneBlock( final Dimensions dim, final int[] blockSize )
	{
		return checkIfMoreThanOneBlock( Intervals.dimensionsAsLongArray( dim ), blockSize );
	}

	public static boolean checkIfMoreThanOneBlock( final long[] dim, final int[] blockSize )
	{
		final CellGrid grid = new CellGrid( dim, blockSize );
		return Arrays.stream( grid.getGridDimensions() ).reduce( 1, ( l1, l2 ) -> l1 * l2 ) > 1;
	}

	public static void relabel( final long[] data, final TLongLongHashMap parents, final UnionFindSparse uf )
	{
		for ( int i = 0; i < data.length; ++i )
		{
			final long v = data[ i ];
			if ( v != 0 && parents.containsKey( v ) )
			{
				final long r = uf.findRoot( v );
				if ( r != v )
					data[ i ] = r;
			}
		}
	}

	public static boolean relabelAndWriteLowerAndUpper(
			final long[] cellPos,
			final int step,
			final String group,
			final String lowerStripDatasetPattern,
			final String upperStripDatasetPattern,
			final BiFunction< Integer, long[], String > unionFindSerializationPattern ) throws IOException
	{
		final long[] targetCellPos = cellPos.clone();
		for ( int d = 0; d < cellPos.length; ++d )
			targetCellPos[ d ] /= step;

		final TLongLongHashMap parents = readFromFileOrEmpty( unionFindSerializationPattern.apply( step, targetCellPos ) );
		final UnionFindSparse uf = new UnionFindSparse( 0 );
		parents.forEachEntry( ( k, v ) -> {
			uf.join( uf.findRoot( k ), uf.findRoot( v ) );
			return true;
		} );

		final N5FSWriter n5 = new N5FSWriter( group );
		relabelAndWriteUpperAndLower( n5, lowerStripDatasetPattern, upperStripDatasetPattern, cellPos, parents, uf );
		return true;
	}

	public static void relabelAndWriteUpperAndLower(
			final N5Writer n5,
			final String lowerStripDatasetPattern,
			final String upperStripDatasetPattern,
			final long[] cellPos,
			final TLongLongHashMap parents,
			final UnionFindSparse uf ) throws IOException
	{
		for ( int d = 0; d < cellPos.length; ++d )
		{
			final String lowerDataset = String.format( lowerStripDatasetPattern, d );
			final String upperDataset = String.format( upperStripDatasetPattern, d );
			final DatasetAttributes lowerAttributes = n5.getDatasetAttributes( lowerDataset );
			final DatasetAttributes upperAttributes = n5.getDatasetAttributes( upperDataset );
			@SuppressWarnings( "unchecked" )
			final DataBlock< long[] > lower = ( DataBlock< long[] > ) n5.readBlock( lowerDataset, lowerAttributes, cellPos );
			@SuppressWarnings( "unchecked" )
			final DataBlock< long[] > upper = ( DataBlock< long[] > ) n5.readBlock( upperDataset, upperAttributes, cellPos );
			relabelAndWrite( lower.getData().clone(), parents, uf, n5, lowerDataset, lowerAttributes, lower.getSize(), lower.getGridPosition() );
			relabelAndWrite( upper.getData().clone(), parents, uf, n5, upperDataset, upperAttributes, upper.getSize(), upper.getGridPosition() );
		}
	}

	public static void relabelAndWrite(
			final long[] data,
			final TLongLongHashMap parents,
			final UnionFindSparse uf,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final int[] size,
			final long[] position )
	{
		relabel( data, parents, uf );
		try
		{
			n5.writeBlock( dataset, attributes, new LongArrayDataBlock( size, position, data ) );
		}
		catch ( final IOException e )
		{
			throw new RuntimeException( e );
		}
	}

	public static boolean writeToFile(
			final String fileName,
			final long[] keys,
			final long[] values ) throws IOException
	{
		final byte[] data = new byte[ Integer.BYTES + keys.length * Long.BYTES * 2 ];
		final ByteBuffer dataBuffer = ByteBuffer.wrap( data );
		dataBuffer.putInt( keys.length );
		Arrays.stream( keys ).forEach( dataBuffer::putLong );
		Arrays.stream( values ).forEach( dataBuffer::putLong );

		final File f = new File( fileName );
		f.getParentFile().mkdirs();
		f.createNewFile();

		try (final FileOutputStream fos = new FileOutputStream( f ))
		{
			fos.write( data );
		}
		return true;
	}

	public static TLongLongHashMap readFromFileOrEmpty(
			final String fileName )
	{
		try
		{
			return readFromFile( fileName );
		}
		catch ( final IOException e )
		{
			LOG.warn( "Failed to read file {}: {}", fileName, e.getMessage() );
			return new TLongLongHashMap();
		}
	}

	public static TLongLongHashMap readFromFile(
			final String fileName ) throws IOException
	{
		final File f = new File( fileName );

		try (final FileInputStream fis = new FileInputStream( f ))
		{
			final byte[] readData = new byte[ ( int ) f.length() ];
			fis.read( readData );
			final ByteBuffer bb = ByteBuffer.wrap( readData );
			final int numEntries = bb.getInt();
			final long[] keys = IntStream.range( 0, numEntries ).mapToLong( i -> bb.getLong() ).toArray();
			final long[] values = IntStream.range( 0, numEntries ).mapToLong( i -> bb.getLong() ).toArray();
			return new TLongLongHashMap( keys, values );
		}
	}
}
