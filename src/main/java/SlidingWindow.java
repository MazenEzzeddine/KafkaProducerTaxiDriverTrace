import java.util.ArrayDeque;
import java.util.Deque;

public class SlidingWindow {
    double sum = 0.0;
    int winSize;

    Deque<Double> dq = new ArrayDeque<>();


    public SlidingWindow(int winSize) {
        this.winSize = winSize;
    }


    public double average (double val) {
        if(dq.size() == winSize) {
            sum -=dq.poll();
        }
        sum +=val;
        dq.add(val);
        return sum /winSize; //dq.size();
    }



}
