package edu.auth.csd.datalab.db.utils.models;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Objects;

import org.apache.commons.lang3.tuple.ImmutablePair;

public class MyBloomFilter implements Cloneable {

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

    public MyBloomFilter(int expectedElements) {
        this.expectedElements = expectedElements;
        this.fpr = FPR_DEFAULT;
        ImmutablePair<Long, Integer> tempPair = this.getNumberOfHashFunctionsAndSize();
        this.bitArraySize = tempPair.getKey();
        this.numberOfHashFunctionsRequired = (tempPair.getValue() % 2 == 0 && tempPair.getValue() != 6) ?
                tempPair.getValue() : ((tempPair.getValue() % 2 != 0 && tempPair.getValue() - 1 != 6) ? tempPair.getValue() : 4);
        this.bitArray = new BitSet((int) this.bitArraySize);
    }

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

    public static String getMD5Hash(final String key) {
        md5.reset();
        md5.update(StandardCharsets.UTF_8.encode(key));
        return String.format("%032x", new BigInteger(1, md5.digest()));
    }

    /*
        m = -n*ln(p) / (ln(2)^2) the number of bits
        k = m/n * ln(2) the number of hash functions
     */
    public ImmutablePair<Long, Integer> getNumberOfHashFunctionsAndSize() {
        final long m = Math.round(- (this.expectedElements) * Math.log(this.fpr) / (Math.log(2) * Math.log(2)));
        return ImmutablePair.of(m, (int) Math.round(m / this.expectedElements * Math.log(2)));
    }

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

    public void addToBloomFilter(final String key) {
        for (final int index : this.getBitArrayIndices(key)) {
            // System.out.println("Flipping bit at index " + index);
            this.flipInBitArray(index);
        }
    }

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
