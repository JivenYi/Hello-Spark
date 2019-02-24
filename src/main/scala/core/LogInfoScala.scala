package core

class LogInfoScala(val timestamp:Long,val upTraffic:Long,val downTraffic:Long) extends Ordered[LogInfoScala] with Serializable {
  override def compare(that: LogInfoScala): Int = {
    var comp = this.upTraffic.compareTo(that.upTraffic)
    if(comp==0){
      comp = this.downTraffic.compareTo(that.downTraffic)
    }
    comp
  }

  override def toString: String = timestamp+":"+upTraffic+":"+downTraffic
}
