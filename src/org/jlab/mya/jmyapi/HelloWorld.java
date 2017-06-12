package org.jlab.mya.jmyapi;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;

/**
 *
 * @author ryans
 */
public class HelloWorld {

    /**
     * @param args the command line arguments
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        ArchiverQueryService service = new ArchiverQueryService(ArchiverDeployment.dev);

        String pv = "DCPHP2ADC10";
        Instant begin = ZonedDateTime.parse("2017-01-01T00:00:00.00-05:00").toInstant();
        Instant end = ZonedDateTime.parse("2017-01-01T00:00:05.00-05:00").toInstant();
        //Instant begin = Instant.parse("2017-01-01T05:00:00.00Z"); // UTC
        //Instant end = Instant.parse("2017-01-01T05:00:05.00Z"); // UTC

        List<PvRecord> recordList = service.find(pv, begin, end);

        for (PvRecord record : recordList) {
            System.out.println(record);
        }

        /*DateTimeFormatter formatter
                    = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(
                    ZoneId.systemDefault());
            System.out.println(formatter.format(begin));*/
    }

}
