package org.jlab.mya.jmyapi;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
        Instant begin = LocalDateTime.parse("2017-01-01T00:00:00").atZone(ZoneId.systemDefault()).toInstant();
        Instant end = LocalDateTime.parse("2017-01-01T00:00:05").atZone(ZoneId.systemDefault()).toInstant();

        List<PvRecord> recordList = service.find(pv, begin, end);

        for (PvRecord record : recordList) {
            System.out.println(record.toColumnString());
        }
    }

}
