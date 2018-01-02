package de.hanslovsky.examples.pipeline;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.Type;
import net.imglib2.view.Views;

public class Extend< T extends Type< T > > implements Function< RandomAccessibleInterval< T >, RandomAccessible< T > >
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