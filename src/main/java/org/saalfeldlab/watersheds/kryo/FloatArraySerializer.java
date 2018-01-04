package org.saalfeldlab.watersheds.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.img.basictypeaccess.array.FloatArray;

public class FloatArraySerializer extends Serializer< FloatArray >
{

	@Override
	public FloatArray read( final Kryo kryo, final Input in, final Class< FloatArray > type )
	{
		return new FloatArray( in.readFloats( in.readInt() ) );
	}

	@Override
	public void write( final Kryo kryo, final Output out, final FloatArray object )
	{
		final float[] arr = object.getCurrentStorageArray();
		out.writeInt( arr.length );
		out.writeFloats( arr );
	}

}