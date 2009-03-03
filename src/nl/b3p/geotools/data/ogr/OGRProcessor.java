package nl.b3p.geotools.data.ogr;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.geotools.data.DataStore;
import org.geotools.data.Transaction;
import org.geotools.data.jdbc.JDBCDataStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Gertjan Al, B3Partners
 */
public class OGRProcessor {

    private static final Log log = LogFactory.getLog(OGRDataStore.class);
    //  public static final String TEMP_TABLE = "_temp_ogr";
    private Properties osProperties;
    private String file_in;
    private Map db_out;
    private String srs;
    private boolean skipFailures;
    private String typename;

    public OGRProcessor(URL url, Map db_tmp, String srs, boolean skipFailures, String typename) {
        this.file_in = (new File(url.getFile())).getAbsolutePath();
        this.typename = typename;
        this.db_out = translateParams(db_tmp);
        this.srs = srs;
        this.skipFailures = skipFailures;
    }

    public void process() throws IOException {
        osProperties = getOsSpecificProperties();

        if (!osProperties.containsKey("ogr.dir")) {
            throw new IOException("Property 'ogr.dir' not found");
        }

        ArrayList<String> commandList = new ArrayList();
        commandList.add(osProperties.getProperty("ogr.dir") + "bin/ogr2ogr");
        commandList.add("-f");
        commandList.add("PostgreSQL");
        commandList.add("-a_srs");
        commandList.add(srs);
        commandList.add(mapToDBString(db_out));
        commandList.add(file_in);
        commandList.add("-nln");
        //commandList.add(TEMP_TABLE);
        commandList.add(typename);
        commandList.add("-overwrite");

        if (skipFailures) {
            commandList.add("-skipfailures");
        }

        String[] commands = commandList.toArray(new String[commandList.size()]);

        ProcessBuilder pb = new ProcessBuilder(commands);
        setEnvironment(pb.environment(), "ogr.env.");

        Process child = pb.start();

        BufferedReader errorReader = new BufferedReader(new InputStreamReader(child.getErrorStream()));
        String errorLine;
        String errorText = "";
        while ((errorLine = errorReader.readLine()) != null) {
            errorText += errorLine + "\n";
        }

        if (!errorText.equals("")) {
            log.error(errorText);
        }

        try {
            int result = child.waitFor();
            if (result != 0) {
                if (errorText.equals("")) {
                    throw new IOException("Loading file '" + file_in + "' failed. No error available");
                } else {
                    throw new IOException("Loading file '" + file_in + "' failed. " + errorText);
                }
            }
        } catch (InterruptedException ex) {
            throw new IOException(ex.getMessage(), ex.getCause());
        }
    }

    private String mapToDBString(Map params) {
        String connect = "PG: ";

        Iterator iter = params.keySet().iterator();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            String value = (String) params.get(key);
            connect += key + "=" + value + " ";
        }

        return connect.substring(0, connect.length() - 1);
    }

    public void close(DataStore dataStore2Read) {
        try {
            // Drop temptable
            JDBCDataStore database = (JDBCDataStore) dataStore2Read;
            Connection con = database.getConnection(Transaction.AUTO_COMMIT);
            con.setAutoCommit(true);

            // TODO make this function work with all databases
            PreparedStatement ps = con.prepareStatement("DROP TABLE \"" + typename + "\"; DELETE FROM \"geometry_columns\" WHERE f_table_name = '" + typename + "'");
            ps.execute();

            con.close();
        } catch (Exception ex) {
            // Drop table failed no biggie
            log.error(ex.getLocalizedMessage());
        }
    }

    private void setEnvironment(Map<String, String> environment, String prefix) {
        try {

            for (String prop : osProperties.stringPropertyNames()) {
                if (prop.toLowerCase().startsWith(prefix)) {
                    String key = prop.substring(prefix.length());
                    String value = osProperties.getProperty(prop);
                    environment.put(key, value);
                }
            }
        } catch (Exception ex) {
        }
    }

    public static Properties getOsSpecificProperties() {
        String os = System.getProperty("os.name");
        Properties p = new Properties();

        try {
            if (os.toLowerCase().contains("windows")) {
                os = "windows";
            } else if (os.toLowerCase().contains("linux")) {
                os = "linux";
            }

            Class c = OGRProcessor.class;
            URL envProperties = c.getResource("pref_" + os + ".properties");
            p.load(envProperties.openStream());

        } catch (Exception ex) {
            log.warn("Unable to load environment settings from pref_" + os + ".properties;" + ex.getLocalizedMessage());
        }
        return p;
    }

    public static Map translateParams(Map map) {
        Map newMap = new HashMap();
        String[][] translate = new String[][]{
            {"passwd", "password"},
            {"host", "host"},
            {"user", "user"},
            {"database", "dbname"}
        };

        for(int i = 0; i < translate.length; i++){
            if(map.containsKey(translate[i][0])){
                newMap.put(translate[i][1], map.get(translate[i][0]));
            }
        }

        return newMap;
    }
}
