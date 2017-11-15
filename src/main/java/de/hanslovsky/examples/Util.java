package de.hanslovsky.examples;

public class Util
{

	public static String HOME_DIR = System.getProperty( "user.home" );

	public static int[] getFlipPermutation( final int numDimensions )
	{
		final int[] perm = new int[ numDimensions ];
		for ( int d = 0, flip = numDimensions - 1; d < numDimensions; ++d, --flip )
			perm[ d ] = flip;
		return perm;
	}


}
