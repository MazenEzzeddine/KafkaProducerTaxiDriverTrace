import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * A TaxiRide consists of
 * - the rideId of the event which is identical for start and end record
 * - the time of the event
 * - the longitude of the start location
 * - the latitude of the start location
 * - the longitude of the end location
 * - the latitude of the end location
 * - the passengerCnt of the ride
 * - the travelDistance which is -1 for start events
 *
 */

//914757,START,2013-01-01 00:00:00,1970-01-01 00:00:00,-73.866131999999993,40.771090000000001,-73.961335000000005,40.764912000000002,6
//914757,END,2013-01-01 00:17:00,2013-01-01 00:00:00,-73.866131999999993,40.771090000000001,-73.961335000000005,40.764912000000002,6

// 917278,END,2013-01-01 00:07:00,2013-01-01 00:00:00,-73.985607000000002,40.744261999999999,-73.969624999999994,40.757371999999997,4
public class TaxiRide {

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    public TaxiRide() {}

    public TaxiRide(long rideId, boolean isStart, DateTime startTime, DateTime endTime,
                    float startLon, float startLat, float endLon, float endLat,
                    short passengerCnt) {

        this.rideId = rideId;
        this.isStart = isStart;
        this.startTime = startTime;
        this.endTime = endTime;
        this.startLon = startLon;
        this.startLat = startLat;
        this.endLon = endLon;
        this.endLat = endLat;
        this.passengerCnt = passengerCnt;
    }

    public long rideId;
    public boolean isStart;
    public DateTime startTime;
    public DateTime endTime;
    public float startLon;
    public float startLat;
    public float endLon;
    public float endLat;
    public short passengerCnt;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rideId).append(",");
        sb.append(isStart ? "START" : "END").append(",");
        if (isStart) {
            sb.append(startTime.toString(timeFormatter)).append(",");
            sb.append(endTime.toString(timeFormatter)).append(",");
        } else {
            sb.append(endTime.toString(timeFormatter)).append(",");
            sb.append(startTime.toString(timeFormatter)).append(",");
        }
        sb.append(startLon).append(",");
        sb.append(startLat).append(",");
        sb.append(endLon).append(",");
        sb.append(endLat).append(",");
        sb.append(passengerCnt);
        return sb.toString();
    }

    public static TaxiRide fromString(String line) {
        String[] tokens = line.split(",");
        if (tokens.length != 9) {
            throw new RuntimeException("Invalid record: " + line);
        }
        TaxiRide ride = new TaxiRide();
        try {
            ride.rideId = Long.parseLong(tokens[0]);

            switch (tokens[1]) {
                case "START":
                    ride.isStart = true;
                    ride.startTime = DateTime.parse(tokens[2], timeFormatter);
                    ride.endTime = DateTime.parse(tokens[3], timeFormatter);
                    break;
                case "END":
                    ride.isStart = false;
                    ride.endTime = DateTime.parse(tokens[2], timeFormatter);
                    ride.startTime = DateTime.parse(tokens[3], timeFormatter);
                    break;
                default:
                    throw new RuntimeException("Invalid record: " + line);
            }

            ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
            ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
            ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
            ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
            ride.passengerCnt = Short.parseShort(tokens[8]);
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }
        return ride;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaxiRide &&
                this.rideId == ((TaxiRide) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int)this.rideId;
    }

}
