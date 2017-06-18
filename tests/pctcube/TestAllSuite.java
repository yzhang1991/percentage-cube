package pctcube;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import pctcube.database.TestColumn;
import pctcube.database.TestDatabase;
import pctcube.database.TestTable;
import pctcube.utils.TestArgumentParser;
import pctcube.utils.TestPermutationGenerator;

// Run all test cases

@RunWith(Suite.class)
@SuiteClasses({ TestPercentageCube.class,
                TestPercentageAggregation.class,
                TestArgumentParser.class,
                TestColumn.class,
                TestDatabase.class,
                TestTable.class,
                TestPermutationGenerator.class})
public class TestAllSuite {

}
