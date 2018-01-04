package org.saalfeldlab.watersheds.pipeline;

import org.apache.spark.api.java.function.Function;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.view.Views;
import scala.Tuple2;

public class Translate< T, F extends RandomAccessibleInterval< T > > implements Function< Tuple2< F, long[] >, RandomAccessibleInterval< T > >
{

	@Override
	public RandomAccessibleInterval< T > call( final Tuple2< F, long[] > t ) throws Exception
	{
		return Views.translate( t._1(), t._2() );
	}

}
