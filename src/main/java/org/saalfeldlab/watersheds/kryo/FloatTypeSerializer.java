package org.saalfeldlab.watersheds.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.type.numeric.real.FloatType;

public class FloatTypeSerializer extends Serializer< FloatType >
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