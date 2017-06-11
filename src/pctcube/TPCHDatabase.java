package pctcube;

import pctcube.database.Database;
import pctcube.database.Table;

public final class TPCHDatabase {

    // This class provides the Java modeled schema information for the TPC-H database
    public static final Database TPCH = new Database();

    static {
        TPCH.addTables(getNationTable(),
                       getRegionTable(),
                       getPartTable(),
                       getSupplierTable(),
                       getPartSuppTable(),
                       getCustomerTable(),
                       getOrdersTable(),
                       getLineItemTable());
    }

    private static Table getNationTable() {
        Table NATION = new Table("NATION");
        return NATION;
    }

    private static Table getRegionTable() {
        Table REGION = new Table("REGION");
        return REGION;
    }

    private static Table getPartTable() {
        Table PART = new Table("PART");
        return PART;
    }

    private static Table getSupplierTable() {
        Table SUPPLIER = new Table("SUPPLIER");
        return SUPPLIER;
    }

    private static Table getPartSuppTable() {
        Table PARTSUPP = new Table("PARTSUPP");
        return PARTSUPP;
    }

    private static Table getCustomerTable() {
        Table CUSTOMER = new Table("CUSTOMER");
        return CUSTOMER;
    }

    private static Table getOrdersTable() {
        Table ORDERS = new Table("ORDERS");
        return ORDERS;
    }

    private static Table getLineItemTable() {
        Table LINEITEM = new Table("LINEITEM");
        return LINEITEM;
    }
}
