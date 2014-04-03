import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class DataPointWritable implements Writable {
	
	public static final String ROW = "ROW";
	public static final String COL = "COL";
	
	private Text value;
	private long id;
	private Text type;
	
	public Text getValue() {
		return value;
	}
	
	public long getid() {
		return id;
	}
	
	public Text getType() {
		return type;
	}
	
	public void setValue(Text value) {
		this.value = value;
	}
	
	public void setid(long i) {
		this.id = i;
	}
	
	public void setType(Text type) {
		this.type = type;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = new Text();
		type = new Text();
        value.readFields(in);
        id = in.readLong();
        type.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
        value.write(out);
        out.writeLong(id);
        type.write(out);
	}
}
