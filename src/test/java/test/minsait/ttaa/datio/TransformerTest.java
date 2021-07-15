package test.minsait.ttaa.datio;

import minsait.ttaa.datio.common.pathfile.PathFile;
import minsait.ttaa.datio.engine.Transformer;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TransformerTest implements  SparkSessionTestWrapper{

    @Test
    public void testTransformer() {

        PathFile path= new PathFile();
        Transformer engine = new Transformer(spark,path);

        try {
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }
    }

}
