/******************************************************************* 
FileName: BitHelpers.cpp
Author	: hieunt 
Desc	: function for working with bit shift easier
*******************************************************************/ 
  
#include "stdafx.h" 
#include "BitHelpers.h"
  
 
/// <summary>
/// Bits the extract from a 64 bit value
/// </summary>
/// <param name="myin">64 bit input value</param>
/// <param name="beg">The begin position.</param>
/// <param name="end">The end position.</param>
/// <returns>The value extracted</returns>
UINT64 BitExtractUInt64(UINT64 myin, UINT32 beg, UINT32 end)
{
	UINT64 myll = 0;
	UINT32 beg1, end1;

	// Let the user reverse the order of beg & end.
	if (beg <= end)
	{
		beg1 = beg;
		end1 = end;
	}
	else
	{
		beg1 = end;
		end1 = beg;
	}
	myll = myin >> beg1;
	myll = myll & BitBuildUInt64(beg1, end1);
	return myll;
}

/// <summary>
/// Build 64 bit value from pos to pos
/// </summary>
/// <param name="beg">The begin pos.</param>
/// <param name="end">The end pos.</param>
/// <returns></returns>
UINT64 BitBuildUInt64(UINT32 beg, UINT32 end)
{
	UINT64 myll = 0;
	if (end == 63)
	{
		myll = (UINT64)(-1);
	}
	else
	{
		myll = (1LL << (end + 1)) - 1;
	}
	myll = myll >> beg;
	return myll;
}

/// <summary>
/// Build 32 Bits value.
/// </summary>
/// <param name="beg">The begin pos.</param>
/// <param name="end">The end pos.</param>
/// <returns></returns>
UINT32 BitBuildUInt32(UINT32 beg, UINT32 end)
{
	UINT32 myll = 0;
	if (end == 31)
	{
		myll = (UINT32)(-1);
	}
	else
	{
		myll = (1 << (end + 1)) - 1;
	}
	myll = myll >> beg;
	return myll;
}


/// <summary>
///  Extract 32 bit values
/// </summary>
/// <param name="myin">The input value.</param>
/// <param name="beg">The begin pos.</param>
/// <param name="end">The end pos.</param>
/// <returns></returns>
UINT32 BitExtractUInt32(UINT32 myin, UINT32 beg, UINT32 end)
{
	UINT32 myll = 0;
	UINT32 beg1, end1;

	// Let the user reverse the order of beg & end.
	if (beg <= end)
	{
		beg1 = beg;
		end1 = end;
	}
	else
	{
		beg1 = end;
		end1 = beg;
	}
	myll = myin >> beg1;
	myll = myll & BitBuildUInt32(beg1, end1);
	return myll;
}


/// <summary>
///  Print the bits to screen for debug.
/// </summary>
/// <param name="Value">The value.</param>
VOID BitPrint(const UINT64 Value) 
{     
	printf("%lld = ", Value); 

	// It's better using this solution
	// std::cout << std::bitset<64>(Value);

	for (int i = 63; i >= 0; i--)
	{
		if ( (i+1)%8 == 0) 
			std::cout << " ";   

		if ( (i+1)%32 == 0) 
			std::cout << "\n";  

		std::cout << ((Value >> i) & 1);
	}

	printf("\n"); 
}
 
/// <summary>
/// Bit i of an int value to be set to 1, leaving all other bits unchanged
/// </summary>
/// <param name="num">The number.</param>
/// <param name="index">The index.</param>
VOID BitSet(UINT64 & num, int index) 
{
	UINT64 bitMask = 1 ;  // Set LSb to 1
	bitMask <<= index ;         // Set bitindex to 1, by shifting 
	num |= bitMask ;            // OR in correct value
}

 
/// <summary>
/// Bit i of an int value to be set to 0, leaving all other bits unchanged.
/// </summary>
/// <param name="num">The number.</param>
/// <param name="index">The index.</param>
VOID BitClear(UINT64 & num, int index) 
{
	UINT64 bitMask = 1 ;
	bitMask <<= index ;
	bitMask = ~bitMask ;  // Flip bits
	num &= bitMask ;      // AND in the correct bit
}
 
/// <summary>
/// Determine if at least one bit from index low to high (where low <= high) of a number called num are set to 1. 
/// Return true if so, and false if all are 0's.
/// </summary>
/// <param name="num">The number.</param>
/// <param name="low">The low.</param>
/// <param name="high">The high.</param>
/// <returns></returns>
BOOL BitIsSetInRange(UINT64 & num, int low, int high)
{
	int numOnes = ( high - low ) + 1 ;
	UINT64 bitMask = ~0 ;        // Flip 0 to get all 1's
	bitMask <<= numOnes ;        // Now create numOnes zeroes at the end
	bitMask = ~bitMask ;         // Flip bits to numOnes 1's
	bitMask <<= low ;            // Shift low bits to left

	return num & bitMask ;      // true, if num & bitMask is non-zero
}
 

/// <summary>
/// Leaves all bits low to high (where low <= high) of a number called num alone, and sets all bits outside the range to 0.
/// </summary>
/// <param name="num">The number.</param>
/// <param name="low">The low.</param>
/// <param name="high">The high.</param>
/// <returns></returns>
BOOL BitSelectRange(UINT64 &num, int low, int high) 
{
	int numOnes = ( high - low ) + 1;
	UINT64 bitMask = ~0 ;  // Flip 0 to get all 1's
	bitMask <<= numOnes ;        // Now create numOnes zeroes at the end
	bitMask = ~bitMask ;         // Flip bits to numOnes 1's
	bitMask <<= low ;              // Shift low bits to left

	return num &= bitMask ;      // true, if num &= bitMask is non-zero
}
 
/// <summary>
/// Swaps num1 and num2 without using a temporary variable
/// </summary>
/// <param name="num1">The num1.</param>
/// <param name="num2">The num2.</param>
VOID BitSwap(UINT &num1, UINT &num2) 
{
	num1 = num1 ^ num2;  // num1 holds num1' XOR num2'
	num2 = num1 ^ num2;  // num2 holds num1' XOR num2' XOR num2' == num1'
	num1 = num2 ^ num1;  // num1 holds num1' XOR num1' XOR num2' == num2'
}


UINT32 BitCount(UINT64 n)
{
	UINT32 count = 0;
	while (n)
	{
		count += (UINT32)(n & 0x00000001);
		n >>= 1;
	}

	return count;
}

/// <summary>
/// Gets the bit mask.
/// </summary>
/// <param name="high">The high.</param>
/// <param name="low">The low.</param>
/// <returns></returns>
UINT64 GetBitMask(const int high, const int low)
{
	UINT64 mask = 0 << 63;   
	for(int i = low; i<=high; i++)
	{
		BitSet(mask, i);
	}  
	return mask;
}