package de.hanslovsky.examples.pipeline;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import scala.Tuple2;

public class RAIFromLoader< T extends RealType< T > & NativeType< T >, A extends ArrayDataAccess< A > > implements PairFunction< HashWrapper< long[] >, HashWrapper< long[] >, RandomAccessibleInterval< T > >
{

	private final String group;

	private final String dataset;
	public RAIFromLoader( final JavaSparkContext sc, final String group, final String dataset, final T t, final A access )
	{
		this( sc, group, dataset, sc.broadcast( t ), sc.broadcast( access ) );
	}

	public RAIFromLoader( final JavaSparkContext sc, final String group, final String dataset, final Broadcast< T > t, final Broadcast< A > access )
	{
		super();
		this.group = group;
		this.dataset = dataset;
	}

	@Override
	public Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< T > > call( final HashWrapper< long[] > offset ) throws Exception
	{
		final N5FSReader n5 = new N5FSReader( this.group );
		final RandomAccessibleInterval< T > img = N5Utils.open( n5, this.dataset );
//		final DatasetAttributes attributes = n5.getDatasetAttributes( this.dataset );
//		final N5CellLoader< T > loader = new N5CellLoader<>( n5, dataset, attributes.getBlockSize() );
//		final CellGrid grid = new CellGrid( attributes.getDimensions(), attributes.getBlockSize() );
//		final Cache< Long, Cell< A > > cache = new SoftRefLoaderCache< Long, Cell< A > >().withLoader( LoadedCellCacheLoader.get( grid, loader, t.getValue().createVariable() ) );
//		final CachedCellImg< T, A > img = new CachedCellImg<>( grid, t.getValue(), cache, access.getValue() );
		return new Tuple2<>( offset, img );
	}

}