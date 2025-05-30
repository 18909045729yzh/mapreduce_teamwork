import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingDoubleComparator extends WritableComparator {
    protected DescendingDoubleComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -super.compare(a, b); // 降序
    }
}
