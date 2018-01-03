package de.hanslovsky.examples.pipeline;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import scala.Tuple2;

public class RAIFromLoader< T extends RealType< T > & NativeType< T > > implements PairFunction< HashWrapper< long[] >, HashWrapper< long[] >, RandomAccessibleInterval< T > >
{

	private final String group;

	private final String dataset;

	public RAIFromLoader( final JavaSparkContext sc, final String group, final String dataset )
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
		return new Tuple2<>( offset, img );
	}

}