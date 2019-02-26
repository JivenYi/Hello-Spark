package core

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.WritableComparable

case class PersonScala(var name:String, var age:Int) extends WritableComparable[PersonScala]{
  /**
    * 重写无参构造函数,用于反序列化时的反射操作
    */
  def this(){
    this("",0)
  }
  override def compareTo(o: PersonScala): Int = {
    var comp = this.name compareTo o.name
    if (comp==0){
      comp = this.age compareTo o.age
    }
    comp
  }

  /**
    * 继承Writable接口需要实现的方法-序列化写操作,将对象字段值写入序列化
    * 注意要和readFields的顺序一致
    * @param dataOutput
    */
  override def write(dataOutput: DataOutput): Unit = {
    dataOutput.writeUTF(name)
    dataOutput.writeInt(age)
  }

  /**
    * 继承Writable接口需要实现的方法-序列化写操作,将对象字段值写入序列化
    * 注意要和write的顺序一致
    * @param dataInput
    */
  override def readFields(dataInput: DataInput): Unit = {
    name = dataInput.readUTF()
    age = dataInput.readInt()


  }
}
