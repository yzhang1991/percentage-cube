package pctcube.database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Config {

    protected static final String m_jdbcClassName = "com.vertica.jdbc.Driver";
    protected static final String m_urlFormat = "jdbc:vertica://%s:%s/%s";

    private String m_serverAddress = "10.10.182.43";
    private int m_portNumber = 5433;
    private String m_userName = "dbadmin";
    private String m_password = "";
    private String m_database = "pctcube";
    private PrintStream m_sqlStream = null;
    private int m_dataSize = 10000;
    private double m_deltaRatio = 0.2;
    private int m_dStart = 2;
    private int m_dEnd = 5;
    private boolean m_datagen = true;

    public boolean needToGenerateData() {
        return m_datagen;
    }

    public int getDStart() {
        return m_dStart;
    }

    public int getDEnd() {
        return m_dEnd;
    }

    public PrintStream getSQLStream() {
        return m_sqlStream;
    }

    public int getDataSize() {
        return m_dataSize;
    }

    public int getDeltaDataSize() {
        return (int) (m_dataSize * m_deltaRatio);
    }

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

    public static Config getConfigFromFile(String filePath) {
        File configFile = new File(filePath);
        Config config = null;
        try {
            BufferedReader in = new BufferedReader(new FileReader(configFile));
            String line;
            config = new Config();
            while ((line = in.readLine()) != null) {
                String[] seg = line.split("=");
                if (seg.length != 2) {
                    System.err.println("ERROR: Unable to parse " + line);
                }
                else {
                    switch(seg[0].trim()) {
                    case "host":
                        config.m_serverAddress = seg[1].trim();
                        break;
                    case "port":
                        config.m_portNumber = Integer.parseInt(seg[1].trim());
                        break;
                    case "user":
                        config.m_userName = seg[1].trim();
                        break;
                    case "password":
                        config.m_password = seg[1].trim();
                        break;
                    case "database":
                        config.m_database = seg[1].trim();
                        break;
                    case "print":
                        if (seg[1].trim().equals("file")) {
                            config.m_sqlStream = new PrintStream(new FileOutputStream(String.format("%s.sql",
                                    ZonedDateTime.now().format(DT_FORMAT))));
                        }
                        else if (seg[1].trim().equals("screen")) {
                            config.m_sqlStream = System.out;
                        }
                        break;
                    case "datasize":
                        config.m_dataSize = Integer.parseInt(seg[1].trim());
                        break;
                    case "delta":
                        config.m_deltaRatio = Double.parseDouble(seg[1].trim());
                        break;
                    case "dstart":
                        config.m_dStart = Integer.parseInt(seg[1].trim());
                        break;
                    case "dend":
                        config.m_dEnd = Integer.parseInt(seg[1].trim());
                        break;
                    case "datagen":
                        if (seg[1].trim().equals("true")) {
                            config.m_datagen = true;
                        }
                        else {
                            config.m_datagen = false;
                        }
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

    private static final DateTimeFormatter DT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss");
}
