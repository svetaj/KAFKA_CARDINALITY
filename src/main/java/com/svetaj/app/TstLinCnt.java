
import com.clearspring.analytics.stream.cardinality.LinearCounting;

/**
 * Test - Linear Counting
 *
 */

public class TstLinCnt {

    public static void main(String[] args) {
        LinearCounting lc = new LinearCounting(6);
        lc.offer(0);
        lc.offer(1);
        lc.offer(2);
        lc.offer(3);
        lc.offer(16);
        lc.offer(17);
        lc.offer(18);
        lc.offer(19);
        lc.offer(0);
        lc.offer(1);
        lc.offer(1111);
        lc.offer(2);
        lc.offer(3);
        lc.offer(16);
        lc.offer(17);
        lc.offer(18);
        lc.offer(19);
        System.out.println("------------------\n");
        System.out.printf("count =%d\n", lc.getCount());
        System.out.printf("cardinality =%d\n", lc.cardinality());
    }
}
