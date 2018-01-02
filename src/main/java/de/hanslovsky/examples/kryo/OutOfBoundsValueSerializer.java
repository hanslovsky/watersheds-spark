package de.hanslovsky.examples.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.outofbounds.OutOfBoundsConstantValueFactory;
import net.imglib2.type.Type;

public class OutOfBoundsValueSerializer< T extends Type< T > > extends Serializer< OutOfBoundsConstantValueFactory< T, RandomAccessibleInterval< T > > >
{

	@Override
	public void write( final Kryo kryo, final Output output, final OutOfBoundsConstantValueFactory< T, RandomAccessibleInterval< T > > object )
	{
		kryo.writeClassAndObject( output, object.getValue() );
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public OutOfBoundsConstantValueFactory< T, RandomAccessibleInterval< T > > read( final Kryo kryo, final Input input, final Class< OutOfBoundsConstantValueFactory< T, RandomAccessibleInterval< T > > > type )
	{
		return new OutOfBoundsConstantValueFactory<>( ( T ) kryo.readClassAndObject( input ) );
	}

}
