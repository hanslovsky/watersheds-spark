package de.hanslovsky.examples.kryo;

import java.lang.reflect.InvocationTargetException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.type.numeric.integer.GenericLongType;

public class GenericLongTypeSerializer< T extends GenericLongType< T > > extends Serializer< T >
{

	@Override
	public T read( final Kryo kryo, final Input in, final Class< T > type )
	{
		try
		{
			return ( T ) kryo.readClass( in ).getType().getDeclaredConstructor( long.class ).newInstance( in.readLong() );
		}
		catch ( InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException | KryoException e )
		{
			throw new RuntimeException( e );
		}
	}

	@Override
	public void write( final Kryo kryo, final Output out, final T object )
	{
		kryo.writeClass( out, object.getClass() );
		out.writeLong( object.getLong() );
	}

}