package edu.auth.csd.datalab.db.utils.models;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Objects;

import org.apache.commons.lang3.tuple.ImmutablePair;

public class MyBloomFilter implements Cloneable {
    
    // BitSet to act as the storage for bits
    private BitSet bitArray;
    private long bitArraySize;
    private int numberOfHashFunctionsRequired;

    private final int expectedElements;
    private final float fpr;
    
    private static MessageDigest md5 = null;
    private static final float FPR_DEFAULT = 0.01f;

    static {
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
    
    // Initialize a new BitSet with the expected elements that will be inserted
    public MyBloomFilter(int expectedElements) {
        this.expectedElements = expectedElements;
        this.fpr = FPR_DEFAULT;
        ImmutablePair<Long, Integer> tempPair = this.getNumberOfHashFunctionsAndSize();
        this.bitArraySize = tempPair.getKey();
        // Determines the number of hash functions required to apply to each element 
        this.numberOfHashFunctionsRequired = (tempPair.getValue() % 2 == 0 && tempPair.getValue() != 6) ?
                tempPair.getValue() : ((tempPair.getValue() % 2 != 0 && tempPair.getValue() - 1 != 6) ? tempPair.getValue() : 4);
        this.bitArray = new BitSet((int) this.bitArraySize);
    }
    
    // Initialize a new BitSet with the no. of expected elements + accepted fpr 
    public MyBloomFilter(int expectedElements, float fpr) {
        this.expectedElements = expectedElements;
        this.fpr = fpr;
        ImmutablePair<Long, Integer> tempPair = this.getNumberOfHashFunctionsAndSize();
        this.bitArraySize = tempPair.getKey();
        this.numberOfHashFunctionsRequired = (tempPair.getValue() % 2 == 0 && tempPair.getValue() != 6) ?
                tempPair.getValue() : ((tempPair.getValue() % 2 != 0 && tempPair.getValue() - 1 != 6) ? tempPair.getValue() : 4);
        this.bitArray = new BitSet((int) this.bitArraySize);
    }

    public long getBitArraySize() {
        return bitArraySize;
    }

    public int getNumberOfHashFunctionsRequired() {
        return numberOfHashFunctionsRequired;
    }

    public void flipInBitArray(final int index) {
        this.bitArray.set(index);
    }

    public boolean isSet(final int index) {
        return this.bitArray.get(index);
    }
    
    // Calculate the MD5 hash that acts as the hash function (32 bits)
    public static String getMD5Hash(final String key) {
        md5.reset();
        md5.update(StandardCharsets.UTF_8.encode(key));
        return String.format("%032x", new BigInteger(1, md5.digest()));
    }

    /*
        Get the number of hash functions for FPR and the initial size
        using these formulas
        
        m = -n*ln(p) / (ln(2)^2) the number of bits
        k = m/n * ln(2) the number of hash functions
     */
    public ImmutablePair<Long, Integer> getNumberOfHashFunctionsAndSize() {
        final long m = Math.round(- (this.expectedElements) * Math.log(this.fpr) / (Math.log(2) * Math.log(2)));
        return ImmutablePair.of(m, (int) Math.round(m / this.expectedElements * Math.log(2)));
    }
    
    // Split the 32 bit MD5 hash to N chunks, where N is the no. of hash function required
    public String[] getHashChunks(final String key) {
        String hash = getMD5Hash(key);
        final int divisor = hash.length() / this.numberOfHashFunctionsRequired;
        String[] chunks = new String[this.numberOfHashFunctionsRequired];
        int i = -1;
        while (hash.length() > 0) {
            String nextChunk = hash.substring(0, divisor);
            chunks[++i] = nextChunk;
            hash = hash.substring(divisor);
        }

        return chunks;
    }
    
    /*
    Convert MD5 chunks to a position in the BitSet. Each chunk is mapped to
    a position using modulo (BitSetSize).
    
    Returns all the positions in the BitSet that will be flipped (0->1)
    */
    public int[] getBitArrayIndices(final String key) {
        int[] indicesToFlip = new int[this.numberOfHashFunctionsRequired];
        int i = -1;
        for (final String val : this.getHashChunks(key)) {
            indicesToFlip[++i] = (new BigInteger(val, 16)
                    .mod(BigInteger.valueOf(this.bitArraySize)))
                    .intValueExact();
        }

        return indicesToFlip;
    }
    
    // Calculates the positions that will be flipped and applies it to the BitSet
    public void addToBloomFilter(final String key) {
        for (final int index : this.getBitArrayIndices(key)) {
            // System.out.println("Flipping bit at index " + index);
            this.flipInBitArray(index);
        }
    }
    
    /* 
    Test the existence of a key in the BF, if even on of them is 0, then
    we can be certain it is not in, otherwise it may exist in the BF.
    */
    public boolean testKeyInBloomFilter(final String key) {
        for (final int index : this.getBitArrayIndices(key)) {
            if (!this.isSet(index)) {
                return false;
            }
        }
        return true;
    }

    public BitSet getBloomFilter() {
        return bitArray;
    }

    public int getExpectedElements() {
        return expectedElements;
    }

    @Override
    public String toString() {
        return String.format("%d elements set in the BitArray", this.bitArray.cardinality());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyBloomFilter that = (MyBloomFilter) o;
        return Objects.equals(bitArray, that.bitArray);
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return bitArray;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(bitArray);
    }
}
