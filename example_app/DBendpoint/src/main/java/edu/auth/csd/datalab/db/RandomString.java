package edu.auth.csd.datalab.db;

import java.util.Random;

/*
Creates random strings using upper case English letters
with a predefined length
*/

class RandomString {

    // Create a string of all characters
    final static String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    final static long seed = 4124214L;
    private StringBuilder sb;
    private Random random = new Random(seed);
    // Specify length of random string
    private int length;

    RandomString() {
        sb = new StringBuilder();
        this.length = 2;
    }

    RandomString(int length) {
        sb = new StringBuilder();
        this.length = length;
    }

    String getRandomString() {

        sb.delete(0, sb.length());

        for (int i = 0; i < length; i++) {
            // generate random index number
            int index = random.nextInt(alphabet.length());

            // get character specified by index
            // from the string
            char randomChar = alphabet.charAt(index);

            // append the character to string builder
            sb.append(randomChar);
        }

        return sb.toString();
    }
}