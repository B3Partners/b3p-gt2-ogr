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
import org.geotools.data.DataStore;
import org.geotools.data.Transaction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geotools.jdbc.JDBCDataStore;

/**
 *
 * @author Gertjan Al, B3Partners
 */
public class OGRProcessor {

    private static final Log log = LogFactory.getLog(OGRDataStore.class);
    //  public static final String TEMP_TABLE = "_temp_ogr";
    //private Properties osParams;
    private String file_in;
    private Map db_out;
    private String srs;
    private boolean skipFailures;
    private String typename;
    //private Map osParams;
    private String fwtools_dir;
    private Map<String, String> envirionment = new HashMap();
    private boolean noDrop;

    public OGRProcessor(URL url, Map db_tmp, String srs, boolean skipFailures, String typename, Map osFWTools, boolean noDrop) throws IOException {
        this.file_in = (new File(url.getFile())).getAbsolutePath();
        this.typename = typename;
        this.db_out = translateParams(db_tmp);
        this.srs = srs;
        this.skipFailures = skipFailures;
        this.noDrop = noDrop;
        setDirAndSetEnv(osFWTools);
    }

    public void process() throws IOException {
        ArrayList<String> commandList = new ArrayList();
        commandList.add(fwtools_dir + "bin/ogr2ogr");
        commandList.add("-f");
        commandList.add("PostgreSQL");
        commandList.add("-a_srs");
        commandList.add(srs);
        commandList.add(mapToDBString(db_out));
        commandList.add(file_in);
        commandList.add("-nln");
        commandList.add(typename);
        commandList.add("-overwrite");

        if (skipFailures) {
            commandList.add("-skipfailures");
        }

        String[] commands = commandList.toArray(new String[commandList.size()]);

        ProcessBuilder pb = new ProcessBuilder(commands);
        // Add own Java Envirionment Settings
        pb.environment().putAll(envirionment);

        log.info("Starting OGR2OGR process");
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

    public void setDirAndSetEnv(Map osParams) throws IOException {
        String os = System.getProperty("os.name");

        if (os.toLowerCase().contains("windows")) {
            os = "windows";
        } else if (os.toLowerCase().contains("linux")) {
            os = "linux";
        } else if (os.toLowerCase().contains("mac")) {
            os = "mac";
        }

        if (osParams.containsKey(os)) {
            if (osParams.get(os) instanceof Map) {
                osParams = (Map) osParams.get(os);
                if (osParams.containsKey("dir")) {
                    if (osParams.get("dir") instanceof String) {
                        fwtools_dir = (String) osParams.get("dir");
                    } else {
                        throw new IOException("Expected " + os + ".dir to contain single value");
                    }
                } else {
                    throw new IOException("Property " + os + ".dir not found");
                }
            } else {
                throw new IOException("Expected " + os + " to contain subdirs .dir and .env");
            }
        }

        if (osParams.containsKey("env")) {
            if (osParams.get("env") instanceof Map) {
                Map envirionmentParameters = (Map) osParams.get("env");
                Iterator iter = envirionmentParameters.keySet().iterator();
                while (iter.hasNext()) {
                    String key = (String) iter.next();
                    if (envirionmentParameters.get(key) instanceof String) {
                        String value = (String) envirionmentParameters.get(key);
                        envirionment.put(key, fwtools_dir + value);
                    } else {
                        throw new IOException("Expected " + os + ".env." + key + " to contain single value");
                    }
                }
            }
        }
    }

    public static Map translateParams(Map map) {
        Map newMap = new HashMap();
        String[][] translate = new String[][]{
            {"passwd", "password"},
            {"host", "host"},
            {"user", "user"},
            {"database", "dbname"}
        };

        for (int i = 0; i < translate.length; i++) {
            if (map.containsKey(translate[i][0])) {
                newMap.put(translate[i][1], map.get(translate[i][0]));
            }
        }

        return newMap;
    }

    public void close(DataStore dataStore2Read) {
        if(!noDrop){
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
    }
}
