package practice;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        int[] a = {1,2,3};
        int[] b = {1,2,3};
        int[] c = new int[3];
        int[] d = new int[3];
        for (int i = 1; i < 4; i++) {
            c[i - 1] = i;
            d[i - 1] = i;
        }
        String str1 = "string";
        String str2 = "string";

        System.out.println(a.equals(b));
        System.out.println(a==b);
        System.out.println(c.equals(d));
        System.out.println(c==d);
        System.out.println(Arrays.equals(a,b));
        System.out.println(Arrays.equals(c,d));
        System.out.println(str1==str2);
        System.out.println(str1.equals(str2));

        a = b;
        c = d;
        str1 = str2;

        System.out.println(a.equals(b));
        System.out.println(a==b);
        System.out.println(c.equals(d));
        System.out.println(c==d);
        System.out.println(Arrays.equals(a,b));
        System.out.println(str1==str2);
        System.out.println(str1.equals(str2));
    }
}
