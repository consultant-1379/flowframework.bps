package com.ericsson.component.aia.bps.engine.service.spark;

import java.io.FileNotFoundException;

import com.ericsson.component.aia.bps.core.pipe.BpsPipe;
import com.ericsson.component.aia.bps.engine.service.BPSPipeLineExecuter;

public class BPSKafkaTest {

    //@Test
    public void test() throws FileNotFoundException {
        try {
            System.setProperty("hadoop.home.dir", "C:\\Development\\bps\\spark-1.6.2-bin-hadoop2.6\\");

            final String flowXML = "C:\\git\\ashish\\flowframework.bps\\bps-integration\\bps-spark-integration\\src\\test\\resources\\analysis.xml";
            //final String flowXML = "/mnt/hgfs/gitrepo/ashish/flowframework.bps/bps-integration/bps-spark-integration/src/test/resources/analysis.xml";

            final BPSPipeLineExecuter instance = new BPSPipeLineExecuter();
            final BpsPipe pipe = instance.init(flowXML);
            pipe.execute();
            pipe.cleanUp();
        } catch (final Throwable e) {
            e.printStackTrace();
        }
    }

}
