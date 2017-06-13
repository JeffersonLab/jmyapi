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

        //System.out.println(MyaUtil.fromMyaTimestamp(6276360291443298313L).atZone(ZoneId.systemDefault()));
        ArchiverQueryService service = new ArchiverQueryService(ArchiverDeployment.ops);

        //String pv = "DCPHP2ADC10";
        String pv = "IPM3F09.XPOS";
        Instant begin
                = LocalDateTime.parse("2016-09-22T08:43:00").atZone(ZoneId.systemDefault()).toInstant();
        Instant end
                = LocalDateTime.parse("2017-04-22T08:43:28").atZone(ZoneId.systemDefault()).toInstant();

        List<PvRecord<Float>> recordList = service.find(pv, Float.class, begin, end);

        for (PvRecord<Float> record : recordList) {
            System.out.println(record.toColumnString());
        }
    }

}
