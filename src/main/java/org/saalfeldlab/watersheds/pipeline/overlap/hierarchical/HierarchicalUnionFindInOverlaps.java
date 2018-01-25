package org.saalfeldlab.watersheds.pipeline.overlap.hierarchical;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.IntToLongFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.saalfeldlab.watersheds.UnionFindSparse;
import org.saalfeldlab.watersheds.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Dimensions;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.util.Intervals;
import scala.Tuple2;

public class HierarchicalUnionFindInOverlaps
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static void createOverlaps(
			final JavaSparkContext sc,
			final CellGrid grid,
			final BiConsumer< Tuple2< long[], long[] >, UnionFindSparse > populateUnionFind,
			final String group,
			final String upperStripDatasetPattern,
			final String lowerStripDatasetPattern,
			final BiFunction< Integer, long[], String > unionFindSerializationPattern )
	{
		final long[] dims = grid.getImgDimensions();
		final int nDim = dims.length;

		final long[] gridDims = grid.getGridDimensions();
		final int[] stepSize = IntStream.generate( () -> 1 ).limit( gridDims.length ).toArray();

		final List< long[] > blocks = Util.collectAllOffsets( gridDims, stepSize, b -> b );
		final int multiplier = 2;
		final Broadcast< CellGrid > gridBC = sc.broadcast( grid );
		final Broadcast< BiConsumer< Tuple2< long[], long[] >, UnionFindSparse > > populateUnionFindBC = sc.broadcast( populateUnionFind );

		// need to start with factor 2 for every other block
		for ( int factor = 2; checkIfMoreThanOneBlock( gridDims, stepSize ); factor *= multiplier )
		{
			final int step = factor;
			final int offset = factor / multiplier - 1;
			final List< HashWrapper< long[] > > relevantBlocks = blocks.stream().filter( new RelevantBlocksLowerOnly( offset, step, d -> gridDims[ d ] ) ).map( HashWrapper::longArray ).collect( Collectors.toList() );
			final JavaRDD< HashWrapper< long[] > > blocksRDD = sc.parallelize( relevantBlocks );
			final JavaPairRDD< HashWrapper< long[] >, Tuple2< long[], long[] > > localAssignments = blocksRDD.mapToPair( hashableCellPos -> {

				final TLongLongHashMap parents = new TLongLongHashMap();
				final UnionFindSparse uf = new UnionFindSparse( parents, 0 );

				final long[] cellPos = hashableCellPos.getData().clone();

				final CellGrid cellGrid = gridBC.getValue();

				final N5FSWriter writer = new N5FSWriter( group );

				final List< DataBlock< long[] > > lowers = new ArrayList<>();
				final List< DataBlock< long[] > > uppers = new ArrayList<>();
				final List< DataBlock< long[] > > otherLowers = new ArrayList<>();
				final List< DataBlock< long[] > > otherUppers = new ArrayList<>();
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
					lowers.add( lowerBlock );
					uppers.add( upperBlock );

					if ( cellPos[ d ] + 1 < cellGrid.gridDimension( d ) )
					{
						final long[] otherCellPos = cellPos.clone();
						otherCellPos[ d ] += 1;
						@SuppressWarnings( "unchecked" )
						final DataBlock< long[] > lowerForOtherBlock = ( DataBlock< long[] > ) writer.readBlock( String.format( lowerStripDatasetPattern, d ), lowerAttrs, otherCellPos );
						@SuppressWarnings( "unchecked" )
						final DataBlock< long[] > upperForOtherBlock = ( DataBlock< long[] > ) writer.readBlock( String.format( upperStripDatasetPattern, d ), upperAttrs, otherCellPos );
						otherLowers.add( lowerForOtherBlock );
						otherUppers.add( upperForOtherBlock );
						// find matches and add to union find
						populateUnionFindBC.getValue().accept( new Tuple2<>( lowerForOtherBlock.getData(), upperBlock.getData() ), uf );
					}
					else
					{
						otherLowers.add( null );
						otherUppers.add( null );
					}
				}

				boolean pointsToSelfOnly = true;

				for ( final TLongLongIterator it = parents.iterator(); it.hasNext() && pointsToSelfOnly; )
				{
					it.advance();
					if ( it.key() != it.value() )
						pointsToSelfOnly = false;
				}

				for ( final TLongLongIterator it = parents.iterator(); it.hasNext(); )
				{
					it.advance();
					uf.findRoot( it.key() );
				}

				final long[] targetCellPos = cellPos.clone();

				for ( int d = 0; d < cellPos.length; ++d )
					targetCellPos[ d ] /= step;

				if ( pointsToSelfOnly )
					return new Tuple2<>( HashWrapper.longArray( targetCellPos ), new Tuple2<>( new long[] {}, new long[] {} ) );
				else
					return new Tuple2<>( HashWrapper.longArray( targetCellPos ), new Tuple2<>( parents.keys(), parents.values() ) );
			} );

			localAssignments
			.aggregateByKey( new ArrayList< Tuple2< long[], long[] > >(), ( l, t ) -> addAndReturn( l, t ), ( l1, l2 ) -> combineAndReturn( l1, l2 ) )
			.mapValues( HierarchicalUnionFindInOverlaps::combineUnionFinds )
			.map( t -> writeToFile( unionFindSerializationPattern.apply( step, t._1().getData().clone() ), t._2()._1(), t._2()._2() ) )
			.count();

			final List< long[] > upperAndLowerBlocks = blocks
					.stream()
					.filter( new RelevantBlocksLowerAndUpper( offset, step ) )
					.collect( Collectors.toList() );
			sc
			.parallelize( upperAndLowerBlocks )
			.map( cellPos -> relabelAndWriteUpperAndLower( cellPos, step, group, lowerStripDatasetPattern, upperStripDatasetPattern, unionFindSerializationPattern ) )
			.count();

			for ( int d = 0; d < nDim; ++d )
				stepSize[ d ] *= multiplier;
		}
	}

	public static boolean checkIfMoreThanOneBlock( final Dimensions dim, final int[] blockSize )
	{
		return checkIfMoreThanOneBlock( Intervals.dimensionsAsLongArray( dim ), blockSize );
	}

	public static boolean checkIfMoreThanOneBlock( final long[] dim, final int[] blockSize )
	{
		final CellGrid grid = new CellGrid( dim, blockSize );
		return Arrays.stream( grid.getGridDimensions() ).reduce( 1, ( l1, l2 ) -> l1 * l2 ) > 1;
	}

	private static void relabel( final long[] data, final TLongLongHashMap parents, final UnionFindSparse uf )
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

	private static boolean relabelAndWriteUpperAndLower(
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

		final TLongLongHashMap parents = readFromFile( unionFindSerializationPattern.apply( step, targetCellPos ) );
		final UnionFindSparse uf = new UnionFindSparse( 0 );
		parents.forEachEntry( ( k, v ) -> {
			uf.join( uf.findRoot( k ), uf.findRoot( v ) );
			return true;
		} );

		final N5FSWriter n5 = new N5FSWriter( group );
		relabelAndWriteUpperAndLower( n5, lowerStripDatasetPattern, upperStripDatasetPattern, cellPos, parents, uf );
		return true;
	}

	private static void relabelAndWriteUpperAndLower(
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

	private static void relabelAndWrite(
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

	private static boolean writeToFile(
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

	private static TLongLongHashMap readFromFile(
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

	private static < T, L extends List< T > > L addAndReturn( final L l, final T t )
	{
		l.add( t );
		return l;
	}

	private static < T, L extends List< T > > L combineAndReturn( final L l1, final L l2 )
	{
		l1.addAll( l2 );
		return l1;
	}

	private static Tuple2< long[], long[] > combineUnionFinds( final List< Tuple2< long[], long[] > > assignments )
	{
		final TLongLongHashMap p = new TLongLongHashMap();
		final UnionFindSparse uf = new UnionFindSparse( p, 0 );

		assignments.forEach( t -> {
			final long[] k = t._1();
			final long[] v = t._2();
			for ( int i = 0; i < k.length; ++i )
				uf.join( uf.findRoot( k[ i ] ), uf.findRoot( v[ i ] ) );
		} );
		return new Tuple2<>( p.keys(), p.values() );
	}

	public static class RelevantBlocksLowerOnly implements Predicate< long[] >
	{

		private final long offset;

		private final long step;

		private final IntToLongFunction gridDimension;

		public RelevantBlocksLowerOnly( final long offset, final long step, final IntToLongFunction gridDimension )
		{
			super();
			this.offset = offset;
			this.step = step;
			this.gridDimension = gridDimension;
		}

		@Override
		public boolean test( final long[] t )
		{
			for ( int d = 0; d < t.length; ++d )
			{
				final long pos = t[ d ];
				if ( ( pos - offset ) % step == 0 && pos + 1 < gridDimension.applyAsLong( d ) )
					return true;
			}
			return false;
		}
	}

	public static class RelevantBlocksLowerAndUpper implements Predicate< long[] >
	{

		private final long offset;

		private final long step;

		public RelevantBlocksLowerAndUpper( final long offset, final long step )
		{
			super();
			this.offset = offset;
			this.step = step;
		}

		@Override
		public boolean test( final long[] t )
		{
			for ( int d = 0; d < t.length; ++d )
			{
				final long pos = t[ d ];
				if ( ( pos - offset ) % step == 0 || ( pos - offset - 1 ) % step == 0 )
					return true;
			}
			return false;
		}
	}


}
