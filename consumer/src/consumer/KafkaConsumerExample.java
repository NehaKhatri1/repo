package consumer;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.StringReader;
import java.security.Key;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
public class KafkaConsumerExample {
    private final static String TOPIC = "test1";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
           // "localhost:9092,localhost:9093,localhost:9094";

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                                    "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // Create the consumer using props.
        final Consumer<Long, String> consumer =
                                    new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        System.out.println("returning consumer "+consumer);
        return consumer;
    }
    
    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        final int giveUp = 100;   int noRecordsCount = 0;
        System.out.println("consumer value "+consumer);
        
List<JsonNumber> listInMain=new ArrayList<JsonNumber>();
        
        System.out.println("listInMain ***"+listInMain);
        
        while (true) { 
        	List<JsonNumber> listForEachRecord=new ArrayList<JsonNumber>();
            
            System.out.println("listForEachRecord ***"+listForEachRecord);
        	
        	System.out.println("inside while loop ");
        	
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count()==0) {
            	
            	System.out.println("record counts "+consumerRecords.count());
                noRecordsCount++;
                System.out.println("in if block");
                if (noRecordsCount > giveUp) break;
                else {
                	System.out.println("else case ");
                //	System.out.println(consumerRecords.getClass().getName());
                	//consumerRecords.get(Key);
                	
                /*	 for (ConsumerRecord<Long, String> consumerRecord : consumerRecords){
                     	
                     	System.out.println("receiving records  ***** ");
                         System.out.printf("offset = %d, key = %s, value = %s%n", consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                 }*/
                	continue;
                }
            }
          /*  consumerRecords.forEach(record -> {
            	System.out.println("i m here");
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });  */  
            
            for (ConsumerRecord<Long, String> consumerRecord : consumerRecords){
            	
            	
            
            	
            	//System.out.println("receiving records  ***** ");
                //System.out.printf("offset = %d, key = %s, value = %s%n", consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                   
                
            //  String jsonString = "{\"table\":\"trade\",\"action\":\"insert\",\"data\":[{\"timestamp\":\"2020-02-26T11:40:42.425Z\",\"symbol\":\"XBTUSD\",\"side\":\"Buy\",\"size\":860,\"price\":9794,\"tickDirection\":\"PlusTick\",\"trdMatchID\":\"5e279a15-7516-48b9-600d-b78a2b74933d\",\"grossValue\":8780600,\"homeNotional\":0.087806,\"foreignNotional\":860}]}";
               String jsonString =consumerRecord.value();
            //    System.out.println("**** jsonString is   **** "+jsonString);
            
              
                JsonParser parser = Json.createParser(new StringReader(jsonString));

        		while(parser.hasNext()) {

        			//   Event event = parser.next();

        			Event event = parser.next(); // START_OBJECT

        			if(event == Event.KEY_NAME) {

        				switch(parser.getString()) {

        				/*case "table":

        					parser.next();

        					System.out.println("table: " + parser.getString());

        					break;  */


        				case "data":

        					parser.next();
        					JsonArray dataArray=parser.getArray();
        					System.out.println("array *** " +dataArray);
        					//System.out.println("parsing further: " +dataArray.size ());
        					System.out.println(" *********** starting ******** ");
        					System.out.println("Getting timestamp ***** "+dataArray.getJsonObject(0).getString("timestamp"));
        					System.out.println("Getting Price ****** "+dataArray.getJsonObject(0).getJsonNumber("price"));
        					System.out.println(" *********** ending ******** ");
            				
        					String timestamp=dataArray.getJsonObject(0).getString("timestamp");
                            JsonNumber price=dataArray.getJsonObject(0).getJsonNumber("price");
                         
                            listForEachRecord=breakTheTimestampIntoHourMinuteDate(timestamp,price);
                            listForEachRecord.add(price);
        					  System.out.println("listForEachRecord cases  ***"+listForEachRecord);
                              
        					  System.out.println("**** this is going to be the last line *****");
        					break;

        				}

        			}  
        		}
                
                
            } 
          //  return listInMain;
            System.out.println("********** before commit *************");
        	
            listInMain.addAll(listForEachRecord);
            System.out.println("******** list is *********"+listInMain);
            consumer.commitAsync();
        }
      
      
        consumer.close();
        System.out.println(" ******** DONE *******");
    }

    private static String getCurrentUtcTime() throws ParseException {

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

		//Here you say to java the initial timezone. This is the secret
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		//Will print in UTC
		System.out.println(sdf.format(calendar.getTime())); 
		SimpleDateFormat localDateFormat = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
		String dateTimeNow=sdf.format(new Date());
		// System.out.println(dateTimeNow);
		return dateTimeNow;

	}

    
    private static boolean compare1MinTimeFrame(String date,String hour,String minute,String second,String dateNow,String hourNow,String minuteNow,String secondNow,JsonNumber price) throws ParseException {
		List<JsonNumber> listOfPrice=new ArrayList<JsonNumber>();
		String dateRetrievedFromRecord=date;
		String hourRetrievedFromRecord=hour;
		String minuteRetrievedFromRecord=minute;
		String secondRetrievedFromRecord=second;
		String dateNowValue=dateNow;
		String hourNowValue=hourNow;
		String minuteNowValue=minuteNow;
		String secondNowValue=secondNow;
		String lowerBound="00";
		String upperBound="59";
		boolean check2=false;

		// Get the timedate retrieved from record
		System.out.println("dateRetrievedFromRecord "+dateRetrievedFromRecord);
	    System.out.println("hourRetrievedFromRecord "+hourRetrievedFromRecord);
		System.out.println("minuteRetrievedFromRecord "+minuteRetrievedFromRecord);
		System.out.println("secondRetrievedFromRecord "+secondRetrievedFromRecord);
		System.out.println("dateNowValue "+dateNowValue);
		System.out.println("hourNowValue "+hourNowValue);
		System.out.println("minuteNowValue "+minuteNowValue);
		System.out.println("secondNowValue "+secondNowValue);

		
	
			
	
		// Accumulate all the prices falling under 1 min window.
		if (dateRetrievedFromRecord.equalsIgnoreCase(dateNowValue) &&  hourRetrievedFromRecord.equalsIgnoreCase(hourNowValue) && minuteRetrievedFromRecord.equalsIgnoreCase(minuteNowValue) && (secondRetrievedFromRecord.compareToIgnoreCase(lowerBound) >= 0 && secondRetrievedFromRecord.compareToIgnoreCase(upperBound) <= 0 )  ) {
			System.out.println("yes i am in if");
			System.out.println("price"+price);
			
		/*	int i=1;
			 while(i== 1){
				 //keep adding price value to the list 
				 listOfPrice.add(price);
				 System.out.println("listOs Price"+listOfPrice);
			  i--;
			 }*/
			check2=true;
			System.out.println("check2 in if case "+check2);
		}
		else{
			System.out.println("do nothing *******");
			check2=false;
		} 
		
		
	//	System.out.println("returning price"+price);
		return check2;
	}
	
    
    
  
    private static List<JsonNumber> breakTheTimestampIntoHourMinuteDate(String timestampRetrievedFromRecord,JsonNumber price){

    	List<JsonNumber> listOfPrices=new ArrayList<JsonNumber>();
    	
		
		System.out.println("timestampRetrievedFromRecord "+timestampRetrievedFromRecord);
		String[] dateTimeArray=timestampRetrievedFromRecord.split("T");

		String date=dateTimeArray[0];
		String time=dateTimeArray[1];

		System.out.println("date "+date);
		System.out.println("time "+time);

		String[] hourMinSecondsSplited=time.split(":");
		String hour=hourMinSecondsSplited[0];
		String minute=hourMinSecondsSplited[1];
		String secondMillisecondUnsplited=hourMinSecondsSplited[2];
		String secondMillisecond=secondMillisecondUnsplited;
		String[] secondMillisecondSplited=secondMillisecond.split("\\.");
		String second=secondMillisecondSplited[0];
		String millisecond=secondMillisecondSplited[1];
		

		System.out.println("hour "+hour);
		System.out.println("minute "+minute);
		System.out.println("second "+second);
		System.out.println("millisecond "+millisecond);
		
		
		try {
			// Get the currenttimeDate
			String dateTimeNowReturned=getCurrentUtcTime();
			System.out.println("dateTimeNowReturned "+dateTimeNowReturned);
			String[] dateTimeNow=dateTimeNowReturned.split(" ");
			String dateNow=dateTimeNow[0];
			String timeNow=dateTimeNow[1];
			System.out.println("dateNow "+dateNow);
			System.out.println("timeNow "+timeNow);
			
			String[] hourMinuteSecondTimeNow=timeNow.split(":");
			String hourNow=hourMinuteSecondTimeNow[0];
			String minuteNow=hourMinuteSecondTimeNow[1];
			String secondNow=hourMinuteSecondTimeNow[2];
			
			System.out.println("hourNow "+hourNow);
			System.out.println("minuteNow "+minuteNow);
			System.out.println("secondNow "+secondNow);
			
			/*JsonNumber returnedPrice=compare1MinTimeFrame(date,hour,minute,second,dateNow,hourNow,minuteNow,secondNow,price);
              listOfPrices.add(returnedPrice);
			*/
			
			boolean returnedFlagCheck2=compare1MinTimeFrame(date,hour,minute,second,dateNow,hourNow,minuteNow,secondNow,price);
			if (returnedFlagCheck2==true) {
				listOfPrices.add(price);
				System.out.println("returnedFlagCheck2  "+returnedFlagCheck2);
			}
           
			//System.out.println(" listOfPrices values **** "+listOfPrices);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		


		
 return listOfPrices;
	}  


    
    
    
  
public static void main(String[] args) throws Exception {
      runConsumer();
  }
}
    
    
    