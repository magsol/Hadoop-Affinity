import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class DataPointWritable implements Writable {
	
	private Text value;
	private int I;
	private int J;
	private long i;
	private long j;
	
	public Text getValue() {
		return value;
	}
	
	public long geti() {
		return i;
	}
	
	public int getI() {
		return I;
	}
	
	public long getj() {
		return j;
	}
	
	public int getJ() {
		return J;
	}
	
	public void setValue(Text value) {
		this.value = value;
	}
	
	public void setI(int I) {
		this.I = I;
	}
	
	public void seti(long i) {
		this.i = i;
	}
	
	public void setJ(int J) {
		this.J = J;
	}
	
	public void setj(long j) {
		this.j = j;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
        value.readFields(in);
        I = in.readInt();
        J = in.readInt();
        i = in.readLong();
        j = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
        value.write(out);
        out.writeInt(I);
        out.writeInt(J);
        out.writeLong(i);
        out.writeLong(j);
	}
}
