package org.saalfeldlab.watersheds.kryo;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.numeric.real.FloatType;

public class Registrator implements KryoRegistrator
{

	@Override
	public void registerClasses( final Kryo kryo )
	{
		kryo.register( FloatArray.class, new FloatArraySerializer() );
		kryo.register( FloatType.class, new FloatTypeSerializer() );
		kryo.register( RandomAccessibleInterval.class, new RandomAccessibleIntervalSerializer<>() );
		kryo.register( CellGrid.class, new CellGridSerializer() );
//		kryo.register( ArrayImg.class, new ArrayImgSerializer<>() );
//		kryo.register( LongArray.class, new LongArraySerializer() );
//		kryo.register( GenericLongTypeSerializer.class, new GenericLongTypeSerializer<>() );
	}

}