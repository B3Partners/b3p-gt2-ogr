package nl.b3p.geotools.data.ogr;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFactorySpi;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

/**

 * @author Gertjan Al, B3Partners
 */
public class OGRDataStoreFactory implements FileDataStoreFactorySpi {

    public static final DataStoreFactorySpi.Param PARAM_URL = new Param("url", URL.class, "url to a file supported by ogr2ogr");
    public static final DataStoreFactorySpi.Param PARAM_SRS = new Param("srs", String.class, "override srs");
    public static final DataStoreFactorySpi.Param PARAM_OGR_TMP_DB = new Param("ogr_tmp_db", Map.class, "PostGIS temp database");
    public static final DataStoreFactorySpi.Param PARAM_SKIPFAILURES = new Param("skip_failures", Boolean.class, "skip ogr2ogr failures");
    public static final DataStoreFactorySpi.Param PARAM_OGR_SETTINGS = new Param("ogr_settings", Map.class, "Map containing os specific values for FWTools dirs");
    public static final DataStoreFactorySpi.Param PARAM_NO_TEMP_DROP = new Param("no_tmp_drop", Boolean.class, "Disable drop of temp table");

    private static final String[] SUPPORTED = new String[]{
        "tab",
        "gml",
        "kml"
    };

    public String getDisplayName() {
        return "OGR File";
    }

    public String getDescription() {
        return "Load files using ogr2ogr";
    }

    public String[] getFileExtensions() {
        return SUPPORTED;
    }

    /**
     * @return true if the file of the f parameter exists
     */
    public boolean canProcess(URL f) {
        for (int i = 0; i < SUPPORTED.length; i++) {
            if (f.getFile().toLowerCase().endsWith("." + SUPPORTED[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if srs can be resolved
     */
    public boolean canProcess(String srs) throws NoSuchAuthorityCodeException, FactoryException {
        return CRS.decode(srs) != null;
    }

    /**
     * @return true if the file in the url param exists
     */
    public boolean canProcess(Map params) {
        boolean result = false;
        if (params.containsKey(PARAM_URL.key) && params.containsKey(PARAM_OGR_TMP_DB.key) && params.containsKey(PARAM_SKIPFAILURES.key) && params.containsKey(PARAM_OGR_SETTINGS.key)) {
            try {
                URL url = (URL) PARAM_URL.lookUp(params);
                result = canProcess(url);
            } catch (IOException ioe) {
                /* return false on any exception */
            }
        }
        if (result && params.containsKey(PARAM_SRS.key)) {
            try {
                String srs = (String) PARAM_SRS.lookUp(params);
                result = canProcess(srs);
            } catch (NoSuchAuthorityCodeException ex) {
                /* return false on any exception */
            } catch (FactoryException ex) {
                /* return false on any exception */
            } catch (IOException ioe) {
                /* return false on any exception */
            }
        }
        return result;
    }

    /*
     * Always returns true, no additional libraries needed
     */
    public boolean isAvailable() {
        return true;
    }

    public Param[] getParametersInfo() {
        return new Param[]{PARAM_URL};
    }

    public Map getImplementationHints() {
        /* XXX do we need to put something in this map? */
        return Collections.EMPTY_MAP;
    }

    public String getTypeName(URL url) throws IOException {
        return OGRDataStore.getTypeName(url);
    }

    public FileDataStore createDataStore(URL url) throws IOException {
        Map params = new HashMap();
        params.put(PARAM_URL.key, url);

        boolean isLocal = url.getProtocol().equalsIgnoreCase("file");
        if (isLocal && !(new File(url.getFile()).exists())) {
            throw new UnsupportedOperationException("Specified file \"" + url + "\" does not exist, this plugin is read-only so no new file will be created");
        } else {
            return createDataStore(params);
        }
    }

    public FileDataStore createDataStore(Map params) throws IOException {
        if (!canProcess(params)) {
            throw new FileNotFoundException("File not found: " + params);
        }

        boolean noDrop = false;
        if(params.containsKey(PARAM_NO_TEMP_DROP.key)){
            noDrop = (Boolean) params.get(PARAM_NO_TEMP_DROP.key);
        }

        return new OGRDataStore((URL) params.get(PARAM_URL.key), (String) params.get(PARAM_SRS.key), (Map) params.get(PARAM_OGR_TMP_DB.key), (Boolean) params.get(PARAM_SKIPFAILURES.key), (Map) params.get(PARAM_OGR_SETTINGS.key), noDrop);
    }

    public DataStore createNewDataStore(Map params) throws IOException {
        throw new UnsupportedOperationException("This plugin is read-only");
    }
}