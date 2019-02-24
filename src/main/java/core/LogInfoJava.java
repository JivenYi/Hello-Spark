package core;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@NoArgsConstructor
@Accessors(chain = true)
public class LogInfoJava implements Comparable<LogInfoJava>,Serializable {
    private long timestamp;
    private long upTraffic;
    private long downTraffic;

    public LogInfoJava(long timestamp, long upTraffic, long downTraffic) {
        this.timestamp = timestamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    @Override
    public int compareTo(LogInfoJava that) {
        int comp = Long.valueOf(this.getUpTraffic()).compareTo(that.getUpTraffic());
        if(comp==0){
            comp = Long.valueOf(this.getDownTraffic()).compareTo(that.getDownTraffic());
        }
        return comp;
    }
}
