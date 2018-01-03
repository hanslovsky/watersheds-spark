package de.hanslovsky.examples.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.img.basictypeaccess.array.LongArray;

public class LongArraySerializer extends Serializer< LongArray >
{

	@Override
	public LongArray read( final Kryo kryo, final Input in, final Class< LongArray > type )
	{
		return new LongArray( in.readLongs( in.readInt() ) );
	}

	@Override
	public void write( final Kryo kryo, final Output out, final LongArray object )
	{
		final long[] arr = object.getCurrentStorageArray();
		out.writeInt( arr.length );
		out.writeLongs( arr );
	}

}