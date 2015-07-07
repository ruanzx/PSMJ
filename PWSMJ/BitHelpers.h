#pragma once 
// #include <bitset>

// Helper function when working with bit

VOID BitPrint(UINT64);
VOID BitSet(UINT64 &, int);
UINT64 GetBitMask(const int, const int); 
UINT32 BitCount(UINT64 n);
UINT32 BitBuildUInt32(UINT32, UINT32);
UINT32 BitExtractUInt32(UINT32, UINT32, UINT32);
UINT64 BitBuildUInt64(UINT32, UINT32);
UINT64 BitExtractUInt64(UINT64, UINT32, UINT32);
BOOL BitSelectRange(UINT64 &, int, int);
VOID BitClear(UINT64 & num, int);
BOOL BitIsSetInRange(UINT64 &, int, int);
VOID BitSwap(UINT &, UINT &);
 