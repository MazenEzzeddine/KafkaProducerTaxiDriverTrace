import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Reader {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);
    private transient BufferedReader reader;
    private transient InputStream gzipStream;

    public int servingSpeed = 20;
    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();
            ArrayList<TaxiRide> rides = new ArrayList<TaxiRide>();

     void read() throws IOException, InterruptedException {
         long counter = 0;
         long servingStartTime = Calendar.getInstance().getTimeInMillis();
         long dataStartTime=0;

        gzipStream = new GZIPInputStream(getClass().getResourceAsStream("nycTaxiRides.gz"));
        reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));
         String line;
         TaxiRide ride;
         Random rnd = new Random();
         if ( (line = reader.readLine()) != null) {
             // read first ride
             ride = TaxiRide.fromString(line);
             //System.out.println(ride);
             dataStartTime = getEventTime(ride);
             counter++;
             rides.add(ride);
         }

         while ((line = reader.readLine()) != null) {
             long rideEventTime;
             ride = TaxiRide.fromString(line);
             //System.out.println(ride);
             rides.add(ride);
             counter++;
             rideEventTime = getEventTime(ride);
             long now = Calendar.getInstance().getTimeInMillis();
             long servingTime = toServingTime(servingStartTime, dataStartTime, rideEventTime);
             long waitTime = servingTime - now;
             //1 hour = 24344

             if(waitTime> 0){
                 System.out.println("sleeping for " + waitTime);
             }
             Thread.sleep( (waitTime > 0) ? waitTime : 0);
             Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
             KafkaProducerExample.
                     producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                             null, null, UUID.randomUUID().toString(), custm));
             //System.out.println("serving ride " +  ride.toString());
             log.info("sending event {}", ride.toString());
             if(counter==1134709 /*168873*//*24344*//*1000*/)
                 break;
         }
         this.reader.close();
         this.reader = null;
         this.gzipStream.close();
         this.gzipStream = null;
    }


    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    public long getEventTime(TaxiRide ride) {
        if (ride.isStart) {
            return ride.startTime.getMillis();
        }
        else {
            return ride.endTime.getMillis();
        }
    }


}
