import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Reader {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);
    private static final transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();
    public int servingSpeed = 40;
    ArrayList<TaxiRide> rides = new ArrayList<TaxiRide>();
    int[] fiveSecondsWindow = {0, 0, 0, 0, 0};
    private transient BufferedReader reader;
    private transient InputStream gzipStream;


    void read() throws IOException, InterruptedException {
        long counter = 0;
        long servingStartTime = Calendar.getInstance().getTimeInMillis();
        long dataStartTime = 0;

        gzipStream = new GZIPInputStream(getClass().getResourceAsStream("nycTaxiRides.gz"));
        reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));
        String line;
        TaxiRide ride;
        if ((line = reader.readLine()) != null) {
            // read first ride
            ride = TaxiRide.fromString(line);
            //System.out.println(ride);
            dataStartTime = getEventTime(ride);
            counter++;
            rides.add(ride);
        }

        int seconds = 1000;

        List<Integer> events = new ArrayList<Integer>();
        int numPerSec = 0;
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



            if((rideEventTime - dataStartTime)/40 < seconds) {
                numPerSec++;
            }else {
                 if (numPerSec > 5 ) {
                events.add(numPerSec);
                }
                seconds =  seconds + 1000;
                numPerSec=0;
            }

            /* if(waitTime> 0){
                 System.out.println("sleeping for " + waitTime);
             }*/

            //TODO uncomment if not a batch and rather serve by timestamp
         /*   Thread.sleep((waitTime > 0) ? waitTime : 0);
            Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
            KafkaProducerExample.
                    producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                            null, null, UUID.randomUUID().toString(), custm));*/
            //System.out.println("serving ride " +  ride.toString());
            //log.info("sending event {}", ride.toString());

            //  log.info("sending event");

            if (counter == 2199999) {
                System.out.println(events);
                break;
            }

        }
        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;

        System.out.println(events);
        System.out.println(events.size());
        printTheEvents(events);
        //saveTheEvents(events);
        //sendTheEvents(events);

    }



    private void saveTheEvents(List<Integer> events) throws InterruptedException, IOException {
        FileOutputStream fout = new FileOutputStream("wrzeros1001h.csv");
        String strtowrite;
        int index =0;

        for (int i = 0; i < events.size(); i++) {
            strtowrite = String.valueOf(index) +","+ events.get(i) + "\n";
            // if(events.get(i)> 10) {
            System.out.println(events.get(i));
            fout.write(strtowrite.getBytes(Charset.forName("UTF-8")));
            System.out.println(events.get(i));
            index++;
            // }
            if(index > 3600 /*7200*/) break;
        }
    }



    private void printTheEvents(List<Integer> events) throws InterruptedException {
        for (int i = 0; i < events.size(); i++) {
            System.out.println(events.get(i));
            if (i> 7250) break;
        }
    }


        private void sendTheEvents(List<Integer> events) throws InterruptedException {

        Random rnd = new Random();
        for (int n = 0; n < events.size() ; n++) {
           // long sleep = (long) (1000.0/events.get(n));
            int e = (int) (events.get(n)/*/1.3*/);
            for (int i = 0; i < e ; i++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));
                //Thread.sleep(sleep);
            }

            if (n> 7250) break;
            System.out.println("seding events :" + e);
            System.out.println("sleeping 1 sec");
            Thread.sleep(1000);
        }
    }



   /* private void sendTheEvents(List<Integer> events) throws InterruptedException {

        Random rnd = new Random();
        for (int n = 0; n < events.size() ; n++) {
            for (int i = 0; i < events.get(n) ; i++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                null, null, UUID.randomUUID().toString(), custm));
            }
            System.out.println("seding events :" + events.get(n));
            System.out.println("sleeping 1 sec");
            Thread.sleep(1000);
        }


    }*/




    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    public long getEventTime(TaxiRide ride) {
        if (ride.isStart) {
            return ride.startTime.getMillis();
        } else {
            return ride.endTime.getMillis();
        }
    }



}
