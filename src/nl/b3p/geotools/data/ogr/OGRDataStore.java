package nl.b3p.geotools.data.ogr;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geotools.data.AbstractFileDataStore;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureReader;
import org.geotools.data.postgis.PostgisDataStoreFactory;
import org.geotools.feature.FeatureType;

/**
 *
 * @author Gertjan Al, B3Partners
 */
public class OGRDataStore extends AbstractFileDataStore {

    private static final Log log = LogFactory.getLog(OGRDataStore.class);
    private URL url;
    private String strippedFile;
    private String srs;
    private DataStore postgisDataStore;
    private OGRProcessor processor;
    private Map tmp_params;

    public OGRDataStore(URL url, String srs, Map tmp_params, boolean skipFailures) throws IOException {
        this.url = url;
        this.srs = srs;
        this.tmp_params = tmp_params;

        // File to tmp postgis
        processor = new OGRProcessor(url, tmp_params, srs, skipFailures);

        // Open tmp postgis
        postgisDataStore = new PostgisDataStoreFactory().createDataStore(tmp_params);
    }

    public String[] getTypeNames() throws IOException {
        strippedFile = getTypeName(url);
        return new String[]{strippedFile};
    }

    public static String getTypeName(URL url) throws IOException {
        String strippedFile = url.getFile();
        strippedFile = strippedFile.substring(strippedFile.indexOf("."));
        return strippedFile;
    }

    public FeatureType getSchema(String typename) throws IOException {
        return postgisDataStore.getSchema(typename);
    }

    public FeatureType getSchema() throws IOException {
        return postgisDataStore.getSchema(strippedFile);
    }

    public FeatureReader getFeatureReader(Query query, Transaction trans) throws IOException {
        return postgisDataStore.getFeatureReader(query, trans);
    }

    public FeatureReader getFeatureReader() throws IOException {
        return (FeatureReader) postgisDataStore.getFeatureSource(strippedFile);
    }

    public FeatureReader getFeatureReader(String typename) throws IOException {
        return (FeatureReader) postgisDataStore.getFeatureSource(typename);
    }

    @Override
    public void dispose() {
        processor.close(postgisDataStore);
        postgisDataStore.dispose();
        super.dispose();
    }
}
