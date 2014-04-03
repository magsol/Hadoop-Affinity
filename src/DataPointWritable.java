import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class DataPointWritable implements Writable {
	
	private Text value;
	private long id;
	
	public Text getValue() {
		return value;
	}
	
	public long getid() {
		return id;
	}
	
	public void setValue(Text value) {
		this.value = value;
	}
	
	public void setid(long i) {
		this.id = i;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = new Text();
        value.readFields(in);
        id = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
        value.write(out);
        out.writeLong(id);
	}
}
