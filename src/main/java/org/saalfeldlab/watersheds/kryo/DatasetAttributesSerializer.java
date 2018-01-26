package org.saalfeldlab.watersheds.kryo;

import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DatasetAttributesSerializer extends Serializer< DatasetAttributes >
{

	@Override
	public DatasetAttributes read( final Kryo kryo, final Input in, final Class< DatasetAttributes > type )
	{
		final int numDimensions = in.readInt();
		return new DatasetAttributes(
				in.readLongs( numDimensions ),
				in.readInts( numDimensions ),
				kryo.readObject( in, DataType.class ),
				kryo.readObject( in, CompressionType.class ) );
	}

	@Override
	public void write( final Kryo kryo, final Output out, final DatasetAttributes object )
	{
		out.writeInt( object.getNumDimensions() );
		out.writeLongs( object.getDimensions() );
		out.writeInts( object.getBlockSize() );
		kryo.writeObject( out, object.getDataType() );
		kryo.writeObject( out, object.getCompressionType() );
	}

}
