package edu.auth.csd.datalab.db;

public class StringHash {
    
    final static int p1 = 37, m1 = 1000000009;
    private static StringHash sh = null;

    private StringHash() { }

    public static StringHash getInstance() {
        if (sh == null) {
            sh = new StringHash();
        }
        return sh;
    }

    public int getHash(final String s) {
        int hash_so_far = 0;
        final char[] s_array = s.toCharArray();
        long p_pow = 1;
        final int n = s_array.length;
        for (int i = 0; i < n; i++) {
            hash_so_far = (int)((hash_so_far + (s_array[i] - 'a' + 1) * p_pow) % m1);
            p_pow = (p_pow * p1) % m1;
        }    

        return hash_so_far;
    }
}
