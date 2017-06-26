package pctcube.database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class JDBCConfig {

    protected static final String m_jdbcClassName = "com.vertica.jdbc.Driver";
    protected static final String m_urlFormat = "jdbc:vertica://%s:%s/%s";

    private String m_serverAddress = "10.10.182.43";
    private int m_portNumber = 5433;
    private String m_userName = "dbadmin";
    private String m_password = "";
    private String m_database = "pctcube";

    public String getDatabaseURL() {
        return String.format(m_urlFormat, getServerAddress(), getPortNumber(), getDatabaseName());
    }

    public String getServerAddress() {
        return m_serverAddress;
    }

    public int getPortNumber() {
        return m_portNumber;
    }

    public String getUserName() {
        return m_userName;
    }

    public String getPassword() {
        return m_password;
    }

    public String getDatabaseName() {
        return m_database;
    }

    public void setServerAddress(String serverAddress) {
        m_serverAddress = serverAddress;
    }

    public void setPortNumber(int portNumber) {
        m_portNumber = portNumber;
    }

    public void setUserName(String userName) {
        m_userName = userName;
    }

    public void setPassword(String password) {
        m_password = password;
    }

    public void setDatabaseName(String database) {
        m_database = database;
    }

    public static JDBCConfig getJDBCConfigFromFile(String filePath) {
        File configFile = new File(filePath);
        JDBCConfig config = null;
        try {
            BufferedReader in = new BufferedReader(new FileReader(configFile));
            String line;
            config = new JDBCConfig();
            while ((line = in.readLine()) != null) {
                String[] seg = line.split("=");
                if (seg.length != 2) {
                    System.err.println("ERROR: Unable to parse " + line);
                }
                else {
                    switch(seg[0]) {
                    case "host":
                        config.setServerAddress(seg[1]);
                        break;
                    case "port":
                        config.setPortNumber(Integer.parseInt(seg[1]));
                        break;
                    case "user":
                        config.setUserName(seg[1]);
                        break;
                    case "password":
                        config.setPassword(seg[1]);
                        break;
                    case "database":
                        config.setDatabaseName(seg[1]);
                        break;
                    }
                }
            }
            in.close();
        }
        catch (FileNotFoundException e) {
            System.err.println("Cannot find file " + configFile.getAbsolutePath());
        }
        catch (NumberFormatException | IOException e) {
            e.printStackTrace();
        }
        return config;
    }
}
