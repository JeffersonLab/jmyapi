package org.jlab.mya.jmyapi;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author ryans
 */
public class MyGetTest {

    public static final String TEST_PV = "DCPHP2ADC10";
    public static final int TEST_ID = 13;
    public static final Class TEST_CLASS = Float.class;
    public static final int TEST_SIZE = 1;
    public static final Instant TEST_BEGIN = Instant.parse("2017-06-09T10:15:00.00Z");
    public static final Instant TEST_END = Instant.now();
    
    private Connection con;

    public MyGetTest() {

    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
        ArchiverQueryService service = new ArchiverQueryService(ArchiverDeployment.dev);
        con = service.open("devmya0", 3306);
    }

    @After
    public void tearDown() throws SQLException {
        con.close();
    }

    /**
     * Test of fetchMetadata method, of class MyGet.
     */
    @Test
    public void testFetchMetadata_Connection_String() throws Exception {
        System.out.println("fetchMetadata");
        MyGet instance = new MyGet();
        PvMetadata expResult = new PvMetadata(TEST_ID, TEST_PV, "devmya0", PvDataType.DBR_DOUBLE, 1);
        PvMetadata result = instance.fetchMetadata(con, TEST_PV);
        assertEquals(expResult, result);
    }

    /**
     * Test of countRecords method, of class MyGet.
     */
    @Test
    public void testCountRecords() throws Exception {
        System.out.println("countRecords");
        MyGet instance = new MyGet();
        long expResult = 0L;
        long result = instance.countRecords(con, TEST_ID, TEST_BEGIN, TEST_END);
        assertEquals(expResult, result);
    }

    /**
     * Test of fetchList method, of class MyGet.
     */
    @Test
    public void testFetchList() throws Exception {
        System.out.println("fetchList");
        MyGet instance = new MyGet();
        long expSize = 0;
        List<PvRecord<Float>> result = instance.fetchList(con, TEST_ID, TEST_CLASS, TEST_SIZE, TEST_BEGIN, TEST_END);
        if(result.size() != expSize) {
            fail("List size does not match expected");
        }
    }

}
