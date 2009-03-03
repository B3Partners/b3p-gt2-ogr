package nl.b3p.geotools.data.ogr;

import java.io.File;
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
    private Map ogr_tmp_db;

    public OGRDataStore(URL url, String srs, Map ogr_tmp_db, boolean skipFailures, Map ogr_settings) throws IOException {
        this.url = url;
        this.srs = srs;
        this.ogr_tmp_db = ogr_tmp_db;
        strippedFile = getTypeName(url);

        // File to tmp postgis
        processor = new OGRProcessor(url, ogr_tmp_db, srs, skipFailures, strippedFile, ogr_settings);
        processor.process();

        // Open tmp postgis
        postgisDataStore = new PostgisDataStoreFactory().createDataStore(ogr_tmp_db);
    }

    public String[] getTypeNames() throws IOException {
        return new String[]{strippedFile};
    }

    public static String getTypeName(URL url) throws IOException {
        File file = new File(url.getFile());
        String strippedFile = file.getName();

        strippedFile = strippedFile.substring(0, strippedFile.indexOf("."));
        return strippedFile;
    }

    public FeatureType getSchema(String typename) throws IOException {
        return postgisDataStore.getSchema(typename);
    }

    public FeatureType getSchema() throws IOException {
        return getSchema(strippedFile);
    }

    @Override
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
