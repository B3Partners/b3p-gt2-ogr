package nl.b3p.geotools.data.ogr;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.DataStore;
import org.geotools.data.DefaultServiceInfo;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.FileDataStore;
import org.geotools.data.LockingManager;
import org.geotools.data.ServiceInfo;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;

/**
 *
 * @author Gertjan Al, B3Partners
 * @mprins
 */
public class OGRDataStore implements FileDataStore {

    public static String getTypeName(URL url) throws IOException {
        File file = new File(url.getFile());
        String strippedFile = file.getName();

        strippedFile = strippedFile.substring(0, strippedFile.indexOf("."));
        return strippedFile;
    }

    private final URL url;
    private final String strippedFile;
    private final DataStore postgisDataStore;
    private final OGRProcessor processor;

    public OGRDataStore(URL url, String srs, Map ogr_tmp_db, boolean skipFailures, Map ogr_settings, boolean noDrop) throws IOException {
        this.url = url;
        strippedFile = getTypeName(url);

        // File to tmp postgis
        processor = new OGRProcessor(url, ogr_tmp_db, srs, skipFailures, strippedFile, ogr_settings, noDrop);
        processor.process();

        // Open tmp postgis
        postgisDataStore = new PostgisNGDataStoreFactory().createDataStore(ogr_tmp_db);
    }

    public String[] getTypeNames() throws IOException {
        return new String[]{strippedFile};
    }

    @Override
    public List<Name> getNames() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SimpleFeatureType getSchema(Name name) throws IOException {
        return postgisDataStore.getSchema(name);
    }

    @Override
    public SimpleFeatureType getSchema(String typename) throws IOException {
        return postgisDataStore.getSchema(typename);
    }

    @Override
    public SimpleFeatureType getSchema() throws IOException {
        return getSchema(strippedFile);
    }

    @Override
    public FeatureReader getFeatureReader(Query query, Transaction trans) throws IOException {
        return postgisDataStore.getFeatureReader(query, trans);
    }

    @Override
    public FeatureReader getFeatureReader() throws IOException {
        return (FeatureReader) postgisDataStore.getFeatureSource(strippedFile);
    }

    public FeatureReader getFeatureReader(String typename) throws IOException {
        return (FeatureReader) postgisDataStore.getFeatureSource(typename);
    }

    @Override
    public SimpleFeatureSource getFeatureSource() throws IOException {
        return postgisDataStore.getFeatureSource(strippedFile);
    }

    @Override
    public SimpleFeatureSource getFeatureSource(String string) throws IOException {
        return postgisDataStore.getFeatureSource(string);
    }

    @Override
    public SimpleFeatureSource getFeatureSource(Name name) throws IOException {
        return postgisDataStore.getFeatureSource(name);
    }

    @Override
    public void updateSchema(SimpleFeatureType sft) throws IOException {
        postgisDataStore.updateSchema(strippedFile, sft);
    }

    @Override
    public void updateSchema(String string, SimpleFeatureType sft) throws IOException {
        postgisDataStore.updateSchema(string, sft);
    }

    @Override
    public void updateSchema(Name name, SimpleFeatureType t) throws IOException {
        postgisDataStore.updateSchema(name, t);
    }

    @Override
    public void createSchema(SimpleFeatureType t) throws IOException {
        postgisDataStore.createSchema(t);
    }

    @Override
    public void removeSchema(String string) throws IOException {
        postgisDataStore.removeSchema(string);
    }

    @Override
    public void removeSchema(Name name) throws IOException {
        postgisDataStore.removeSchema(name);
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(Filter filter, Transaction t) throws IOException {
        return getFeatureWriter(strippedFile, filter, t);
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(Transaction t) throws IOException {
        return getFeatureWriter(strippedFile, t);
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(String string, Filter filter, Transaction t) throws IOException {
        return postgisDataStore.getFeatureWriter(string, filter, t);
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(String string, Transaction t) throws IOException {
        return postgisDataStore.getFeatureWriter(string, t);
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(String string, Transaction t) throws IOException {
        return postgisDataStore.getFeatureWriterAppend(string, t);
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(Transaction t) throws IOException {
        return getFeatureWriterAppend(strippedFile, t);
    }

    @Override
    public LockingManager getLockingManager() {
        return postgisDataStore.getLockingManager();
    }

    @Override
    public ServiceInfo getInfo() {
        DefaultServiceInfo serviceInfo = new DefaultServiceInfo();
        serviceInfo.setTitle("OGR DataStore");
        try {
            serviceInfo.setSource(this.url.toURI());
        } catch (URISyntaxException ex) {

        }
        return serviceInfo;
    }

    @Override
    public void dispose() {
        this.processor.close(this.postgisDataStore);
        this.postgisDataStore.dispose();
    }
}
