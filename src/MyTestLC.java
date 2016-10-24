
import java.util.Arrays;

import com.clearspring.analytics.stream.cardinality.LinearCounting.Builder;
import com.clearspring.analytics.stream.cardinality.LinearCounting.LinearCountingMergeException;

public class MyTestLC {
    public static void main(String[] args) {
        LinearCounting lc = new LinearCounting(4);
        lc.offer(0);
        lc.offer(1);
        lc.offer(2);
        lc.offer(3);
        lc.offer(16);
        lc.offer(17);
        lc.offer(18);
        lc.offer(19);
        System.out.println("COUNT=%d\n", lc.computeCount());
        System.out.println("COUNT=%d\n", lc.cardinality());
    }
}