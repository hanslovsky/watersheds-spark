package de.hanslovsky.examples;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.ToDoubleBiFunction;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5CellLoader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.HierarchicalPriorityQueueQuantized;
import net.imglib2.algorithm.morphology.watershed.HierarchicalPriorityQueueQuantized.Factory;
import net.imglib2.algorithm.morphology.watershed.PriorityQueueFactory;
import net.imglib2.cache.Cache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.Composite;
import scala.Tuple2;

public class WatershedsOnDistanceTransform
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static < T extends RealType< T > & NativeType< T >, A extends ArrayDataAccess< A > > JavaRDD< Tuple2< RandomAccessibleInterval< UnsignedLongType >, Long > > flood(
			final JavaSparkContext sc,
			final CellLoader< T > affinityLoader,
			final CellGrid affinitiesGrid,
			final CellGrid watershedsGrid,
			final T extension,
			final A access,
			final boolean processBySlice,
			final Predicate< T > threshold,
			final ToDoubleBiFunction< T, T > dist,
			final PriorityQueueFactory factory )
	{
		final Broadcast< CellGrid > gridBroadcast = sc.broadcast( affinitiesGrid );
		final Broadcast< CellLoader< T > > loaderBroadcast = sc.broadcast( affinityLoader );

		final List< HashWrapper< long[] > > offsets = Util.collectAllOffsets(
				watershedsGrid.getImgDimensions(),
				IntStream.range( 0, watershedsGrid.numDimensions() ).map( d -> watershedsGrid.cellDimension( d ) ).toArray(),
				a -> HashWrapper.longArray( a ) );

		final JavaPairRDD< HashWrapper< long[] >, RandomAccessible< T > > affinities = sc
				.parallelize( offsets )
				.mapToPair( new RAIFromLoader< T, A >( sc, gridBroadcast, loaderBroadcast, extension, access ) )
				.mapValues( new Extend<>( sc.broadcast( extension ) ) );
		return flood( sc, affinities, watershedsGrid, processBySlice, threshold, dist, factory );
	}

	public static < T extends RealType< T > & NativeType< T > > JavaRDD< Tuple2< RandomAccessibleInterval< UnsignedLongType >, Long > > flood(
			final JavaSparkContext sc,
			final JavaPairRDD< HashWrapper< long[] >, RandomAccessible< T > > affinities,
			final CellGrid watershedsGrid,
			final boolean processBySlice,
			final Predicate< T > threshold,
			final ToDoubleBiFunction< T, T > dist,
			final PriorityQueueFactory factory )
	{
		final RunWatershedsOnDistanceTransform< T, UnsignedLongType > rw = new RunWatershedsOnDistanceTransform<>( sc, processBySlice, threshold, dist, new UnsignedLongType(), factory );
		final JavaRDD< Tuple2< RandomAccessibleInterval< UnsignedLongType >, Long > > mapped = affinities
				.map( new ToInterval<>( sc, watershedsGrid ) )
				.map( rw );
		return mapped;
	}

	public static void main( final String[] args ) throws IOException
	{

		final String n5Path = "/groups/saalfeld/home/hanslovskyp/from_heinrichl/distance/n5";
		final String n5Dataset = "distance-transform";
		final String n5Target = "spark-supervoxels-merged";
		final int[] wsBlockSize = new int[] { 100, 100, 100 };
		final double minVal = 0.035703465;
		final double maxVal = 1.4648689;
		final double threshold = 0.9;// 0.5 * ( minVal + maxVal );
		final boolean processSliceBySlice = false;

		final Factory queueFactory = new HierarchicalPriorityQueueQuantized.Factory( 256, -maxVal, -minVal );

		final N5FSReader reader = new N5FSReader( n5Path );
		final DatasetAttributes attrs = reader.getDatasetAttributes( n5Dataset );
		final CellGrid affinitiesGrid = new CellGrid( attrs.getDimensions(), attrs.getBlockSize() );
		final N5CellLoader< FloatType > affinitiesLoader = new N5CellLoader<>( reader, n5Dataset, attrs.getBlockSize() );

		final CellGrid wsGrid = new CellGrid( Arrays.stream( attrs.getDimensions() ).limit( 3 ).toArray(), wsBlockSize );

		final SparkConf conf = new SparkConf()
				.setAppName( MethodHandles.lookup().lookupClass().getName() )
				.setMaster( "local[*]" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryo.registrator", Registrator.class.getName() )
				;

		final JavaSparkContext sc = new JavaSparkContext( conf );

		final Predicate< FloatType > thresholdPredicate = c -> c.getRealDouble() > threshold;

		final ToDoubleBiFunction< FloatType, FloatType > dist = ( c, r ) -> c.getRealDouble() > minVal ? -c.getRealDouble() : Double.POSITIVE_INFINITY;

		final JavaRDD< Tuple2< RandomAccessibleInterval< UnsignedLongType >, Long > > data = flood(
				sc,
				affinitiesLoader,
				affinitiesGrid,
				wsGrid,
				new FloatType( 0.0f ),
				new FloatArray( 1 ),
				processSliceBySlice,
				thresholdPredicate,
				dist,
				queueFactory );
		data.cache().count();

		final N5FSWriter writer = new N5FSWriter( n5Path );
		writer.createDataset( n5Target, wsGrid.getImgDimensions(), wsBlockSize, DataType.UINT64, CompressionType.GZIP );
		final DatasetAttributes wsAttrs = writer.getDatasetAttributes( n5Target );

		final List< Tuple2< long[], Long > > counts = data
				.map( t -> new Tuple2<>( Intervals.minAsLongArray( t._1() ), t._2() ) )
				.collect();

		long totalCount = 0;
		final TObjectLongHashMap< HashWrapper< long[] > > offsetMap = new TObjectLongHashMap<>();
		for ( final Tuple2< long[], Long > count : counts )
		{
			offsetMap.put( HashWrapper.longArray( count._1() ), totalCount );
			totalCount += count._2();
		}
		LOG.info( "Got {} labels ({} {})", totalCount, 1.0 * totalCount / Integer.MAX_VALUE, 1.0 * totalCount / Long.MAX_VALUE );
		final Broadcast< TObjectLongHashMap< HashWrapper< long[] > > > offsetMapBC = sc.broadcast( offsetMap );

		final JavaRDD< RandomAccessibleInterval< UnsignedLongType > > remapped = data
				.map( t -> {
					final RandomAccessibleInterval< UnsignedLongType > source = t._1();
					final HashWrapper< long[] > key = HashWrapper.longArray( Intervals.minAsLongArray( source ) );
					final long offset = offsetMapBC.getValue().get( key );
					final UnsignedLongType zero = new UnsignedLongType( 0 );
					for ( final UnsignedLongType s : Views.flatIterable( source ) )
						if ( !zero.valueEquals( s ) )
							s.setInteger( s.get() + offset );
					return source;
				} )
				.cache();
		remapped.count();
		data.unpersist();

		final TLongLongHashMap sparseUFParents = new TLongLongHashMap();
		final TLongLongHashMap sparseUFRanks= new TLongLongHashMap();
		final UnionFindSparse sparseUnionFind = new UnionFindSparse( sparseUFParents, sparseUFRanks, 0 );
		for ( int d = 0; d < wsGrid.numDimensions(); ++d ) {
			final int finalD = d;
			final JavaPairRDD< HashWrapper< long[] >, Tuple2< RandomAccessibleInterval< UnsignedLongType >, RandomAccessibleInterval< UnsignedLongType > > > borders = remapped
					.mapToPair( rai -> new Tuple2<>( HashWrapper.longArray( Intervals.minAsLongArray( rai ) ), rai ) )
					.mapValues( rai -> {
						final long raiMax = rai.max( finalD );
						final RandomAccessibleInterval< UnsignedLongType > lower = Views.zeroMin( Views.hyperSlice( rai, finalD, 0l ) );
						final RandomAccessibleInterval< UnsignedLongType > upperInside = Views.zeroMin( Views.hyperSlice( rai, finalD, raiMax - 1 ) );
						final RandomAccessibleInterval< UnsignedLongType > upperOutside = Views.zeroMin( Views.hyperSlice( rai, finalD, raiMax ) );
						final ArrayImg< UnsignedLongType, LongArray > l = ArrayImgs.unsignedLongs( Intervals.dimensionsAsLongArray( lower ) );
						final ArrayImg< UnsignedLongType, LongArray > upperI = ArrayImgs.unsignedLongs( Intervals.dimensionsAsLongArray( upperInside ) );
						final ArrayImg< UnsignedLongType, LongArray > upperO = ArrayImgs.unsignedLongs( Intervals.dimensionsAsLongArray( upperOutside ) );

						final Cursor< Pair< UnsignedLongType, UnsignedLongType > > c1 = Views.flatIterable( Views.interval( Views.pair( lower, l ), l ) ).cursor();
						final Cursor< Pair< UnsignedLongType, UnsignedLongType > > c2 = Views.flatIterable( Views.interval( Views.pair( upperInside, upperI ), upperI ) ).cursor();
						final Cursor< Pair< UnsignedLongType, UnsignedLongType > > c3 = Views.flatIterable( Views.interval( Views.pair( upperOutside, upperO ), upperO ) ).cursor();

						while ( c1.hasNext() )
						{
							final Pair< UnsignedLongType, UnsignedLongType > p1 = c1.next();
							final Pair< UnsignedLongType, UnsignedLongType > p2 = c2.next();
							final Pair< UnsignedLongType, UnsignedLongType > p3 = c3.next();
							p1.getB().set( p1.getA() );
							p2.getB().set( p2.getA() );
							p3.getB().set( p3.getA() );
						}

						return new Tuple2<>( lower, Views.concatenate( finalD, upperI, upperO ) );
					} );

			final List< TLongLongHashMap > maps = borders
					.cartesian( borders )
					.filter( t -> t._1()._1().getData()[ finalD ] + 1 == t._2()._1().getData()[ finalD ] )
					.map( t -> new Tuple2<>( t._1()._2()._2(), t._2()._2()._1() ) )
					.map( t -> {
						final TLongLongHashMap parents = new TLongLongHashMap();
						final TLongLongHashMap ranks = new TLongLongHashMap();
						final UnionFindSparse localUF = new UnionFindSparse( parents, ranks, 0 );
						final Cursor< UnsignedLongType > c1 = Views.flatIterable( t._1() ).cursor();
						final Cursor< UnsignedLongType > c2 = Views.flatIterable( Views.hyperSlice( t._2(), finalD, 0l ) ).cursor();
						final Cursor< UnsignedLongType > c3 = Views.flatIterable( Views.hyperSlice( t._2(), finalD, 1l ) ).cursor();
						while ( c1.hasNext() )
						{
							final long u1 = c1.next().getIntegerLong();
							final long u2 = c2.next().getIntegerLong();
							final long u3 = c3.next().getIntegerLong();
							if ( u1 != 0 && u2 == u3 && u2 != 0 )
								localUF.join( localUF.findRoot( u1 ), localUF.findRoot( u3 ) );

						}
						return parents;
					} )
					.collect();
			for ( final TLongLongHashMap map : maps )
				for ( final TLongLongIterator it = map.iterator(); it.hasNext(); ) {
					it.advance();
					sparseUnionFind.join( sparseUnionFind.findRoot( it.key() ), sparseUnionFind.findRoot( it.value() ) );
				}
		}

		final int setCount = sparseUnionFind.setCount();
		final Broadcast< TLongLongHashMap > parentsBC = sc.broadcast( sparseUFParents );
		final Broadcast< TLongLongHashMap > ranksBC = sc.broadcast( sparseUFRanks );



		remapped
		.map( rai -> {
			final UnionFindSparse uf = new UnionFindSparse( parentsBC.getValue(), ranksBC.getValue(), setCount );
			for ( final UnsignedLongType t : Views.flatIterable( rai ) )
				if ( t.getIntegerLong() != 0 )
					t.set( uf.findRoot( t.getIntegerLong() ) );
			return rai;
		} )
		.map( new WriteN5<>( sc, writer, n5Target, wsAttrs.getBlockSize() ) )
		.count();



		sc.close();
	}

	public static class RAIFromLoader< T extends RealType< T > & NativeType< T >, A extends ArrayDataAccess< A > > implements PairFunction< HashWrapper< long[] >, HashWrapper< long[] >, RandomAccessibleInterval< T > >
	{

		private final Broadcast< CellGrid > gridBroadcast;

		private final Broadcast< CellLoader< T > > loaderBroadcast;

		private final Broadcast< T > extension;

		private final A access;

		public RAIFromLoader( final JavaSparkContext sc, final Broadcast< CellGrid > gridBroadcast, final Broadcast< CellLoader< T > > loaderBroadcast, final T extension, final A access )
		{
			super();
			this.gridBroadcast = gridBroadcast;
			this.loaderBroadcast = loaderBroadcast;
			this.extension = sc.broadcast( extension );
			this.access = access;
		}

		@Override
		public Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< T > > call( final HashWrapper< long[] > offset ) throws Exception
		{
			final CellGrid grid = gridBroadcast.getValue();
			final CellLoader< T > loader = loaderBroadcast.getValue();
			final Cache< Long, Cell< A > > cache = new SoftRefLoaderCache< Long, Cell< A > >().withLoader( LoadedCellCacheLoader.get( grid, loader, extension.getValue().createVariable() ) );
			final CachedCellImg< T, A > img = new CachedCellImg<>( grid, extension.getValue(), cache, access );
			return new Tuple2<>( offset, img );
		}

	}

	public static class ToInterval< V > implements Function< Tuple2< HashWrapper< long[] >, V >, Tuple2< Interval, V > >
	{

		private final Broadcast< CellGrid > watershedsGrid;

		public ToInterval( final JavaSparkContext sc, final CellGrid watershedsGrid )
		{
			super();
			this.watershedsGrid = sc.broadcast( watershedsGrid );
		}

		@Override
		public Tuple2< Interval, V > call( final Tuple2< HashWrapper< long[] >, V > t ) throws Exception
		{
			final long[] min = t._1().getData();
			final long[] max = IntStream.range( 0, min.length ).mapToLong( d -> Math.min( min[ d ] + watershedsGrid.getValue().cellDimension( d ) + 1, watershedsGrid.getValue().imgDimension( d ) ) - 1 ).toArray();
			final Interval interval = new FinalInterval( min, max );
			return new Tuple2<>( interval, t._2() );
		}

	}

	public static class Extend< T extends Type< T > > implements Function< RandomAccessibleInterval< T >, RandomAccessible< T > >
	{

		private final Broadcast< T > extension;

		public Extend( final Broadcast< T > extension )
		{
			super();
			this.extension = extension;
		}

		@Override
		public RandomAccessible< T > call( final RandomAccessibleInterval< T > rai ) throws Exception
		{
			return Views.extendValue( rai, extension.getValue() );
		}

	}

	public static class WriteN5< T extends NativeType< T > > implements Function< RandomAccessibleInterval< T >, Boolean >
	{

		private final Broadcast< N5FSWriter > writer;

		private final String n5Target;

		private final int[] cellSize;

		public WriteN5( final JavaSparkContext sc, final N5FSWriter writer, final String n5Target, final int[] cellSize )
		{
			super();
			this.writer = sc.broadcast( writer );
			this.n5Target = n5Target;
			this.cellSize = cellSize;
		}

		@Override
		public Boolean call( final RandomAccessibleInterval< T > rai ) throws Exception
		{
			final long[] offset = IntStream.range( 0, 3 ).mapToLong( d -> rai.min( d ) / cellSize[ d ] ).toArray();
			LOG.debug( "Saving block with offset {}", Arrays.toString( offset ) );
			N5Utils.saveBlock( rai, writer.getValue(), n5Target, offset );
			LOG.debug( "Saved block with offset {}", Arrays.toString( offset ) );
			return true;
		}

	}

	public static class Collapse< T > implements Function< RandomAccessible< T >, RandomAccessible< ? extends Composite< T > > >
	{

		@Override
		public RandomAccessible< ? extends Composite< T > > call( final RandomAccessible< T > ra ) throws Exception
		{
			return Views.collapse( ra );
		}

	}

	public static class Registrator implements KryoRegistrator
	{

		@Override
		public void registerClasses( final Kryo kryo )
		{
			kryo.register( FloatType.class, new FloatTypeSerializer() );
		}

	}

	public static class FloatTypeSerializer extends Serializer< FloatType >
	{

		@Override
		public FloatType read( final Kryo kryo, final Input in, final Class< FloatType > type )
		{
			return new FloatType( in.readFloat() );
		}

		@Override
		public void write( final Kryo kryo, final Output out, final FloatType object )
		{
			out.writeFloat( object.get() );
		}

	}

}
