package ca.uwaterloo.iss4e.hadoop.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xiliu on 02/10/14.
 */
public class LineOffset implements WritableComparable {

    public long offset;
    public String fileName;

    public void readFields(DataInput in) throws IOException {
        this.offset = in.readLong();
        this.fileName = Text.readString(in);
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        Text.writeString(out, fileName);
    }

    public int compareTo(Object o) {
        LineOffset that = (LineOffset)o;

        int f = this.fileName.compareTo(that.fileName);
        if(f == 0) {
            return (int)Math.signum((double)(this.offset - that.offset));
        }
        return f;
    }
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof LineOffset)
            return this.compareTo(obj) == 0;
        return false;
    }
    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; //an arbitrary constant
    }
}