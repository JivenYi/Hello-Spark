package core;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 需要无参构造函数,用于反序列化时的反射操作
 */
@Data
@NoArgsConstructor
@Accessors(chain = true)
public class PersonJava implements WritableComparable<PersonJava> {
    private String name;
    private Integer age;



    public PersonJava(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public int compareTo(PersonJava that) {
        int comp = this.name.compareTo(that.name);
        if(comp==0){
            comp = this.age.compareTo(that.age);
        }
        return comp;
    }
    /**
     * 继承Writable接口需要实现的方法-序列化写操作,将对象字段值写入序列化
     * 注意要和readFields的顺序一致
     * @param dataOutput
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeInt(age);
    }
    /**
     * 继承Writable接口需要实现的方法-序列化写操作,将对象字段值写入序列化
     * 注意要和write的顺序一致
     * @param dataInput
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        age = dataInput.readInt();
    }


}
