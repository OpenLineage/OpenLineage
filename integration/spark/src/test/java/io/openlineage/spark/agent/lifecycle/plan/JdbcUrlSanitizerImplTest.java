package io.openlineage.spark.agent.lifecycle.plan;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class JdbcUrlSanitizerImplTest {

  @ParameterizedTest
  @MethodSource("provideJdbcUrls")
  void testJdbcUrlSanitizer(String jdbcUrl, String expectedResult) {
    JdbcUrlSanitizer jdbcUrlSanitizer = new JdbcUrlSanitizerImpl();

    String actualResult = jdbcUrlSanitizer.sanitize(jdbcUrl);

    assertEquals(expectedResult, actualResult);
  }

  private static Stream<Arguments> provideJdbcUrls() {
    return Stream.of(
        Arguments.of("jdbc:sqlserver://localhost;user=MyUserName;password=*****", "sqlserver://localhost/"),
        Arguments.of("jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;integratedSecurity=true", "sqlserver://localhost:1433/AdventureWorks"),
        Arguments.of("jdbc:sqlserver://localhost\\instance1;databaseName=AdventureWorks;integratedSecurity=true", "sqlserver://localhost\\instance1/AdventureWorks"),
        Arguments.of("jdbc:sqlserver://;serverName=3ffe:8311:eeee:f70f:0:5eae:10.203.31.9\\instance1;integratedSecurity=true;", "sqlserver://3ffe:8311:eeee:f70f:0:5eae:10.203.31.9\\instance1/"),
        Arguments.of("jdbc:sqlserver://;serverName=3ffe:8311:eeee:f70f:0:5eae:10.203.31.9\\instance1;databaseName=AdventureWorks;integratedSecurity=true;", "sqlserver://3ffe:8311:eeee:f70f:0:5eae:10.203.31.9\\instance1/AdventureWorks"),

        Arguments.of("jdbc:db2://test.host.com:5021/test:user=dbadm;password=dbadm;", "db2://test.host.com:5021/test"),

        Arguments.of("jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true", "postgresql://localhost/test"),
        Arguments.of("jdbc:postgresql://localhost:5432/test?user=fred&password=secret&ssl=true", "postgresql://localhost:5432/test"),
        Arguments.of("jdbc:postgresql://192.168.1.1:5432/test?user=fred&password=secret&ssl=true", "postgresql://192.168.1.1:5432/test"),
        Arguments.of("jdbc:postgresql://192.168.1.1:5432", "postgresql://192.168.1.1:5432"),

        Arguments.of("jdbc:oracle:thin:@10.253.102.122:1521:dg01?key=value", "oracle:thin:@10.253.102.122:1521:dg01"),
        Arguments.of("jdbc:oracle:thin:user/password@some.host:1521:orcl", "oracle:thin:@some.host:1521/orcl"),
        Arguments.of("jdbc:oracle:thin:@localhost:1521:orcl", "oracle:thin:@localhost:1521/orcl"),
        Arguments.of("jdbc:oracle:thin:@//localhost:1521/serviceName", "oracle:thin:@//localhost:1521/serviceName"),
        Arguments.of("jdbc:oracle:thin:@dbname_high?TNS_ADMIN=/Users/test/wallet_dbname", "oracle:thin:@dbname_high"),


        Arguments.of("jdbc:mysql://10.253.102.122:1521/dbname", "mysql://10.253.102.122:1521/dbname"),
        Arguments.of("jdbc:mysql://10.253.102.122:1521/dbname;user=user;pawword=pwd", "mysql://10.253.102.122:1521/dbname"),
        Arguments.of("jdbc:mysql://host.name.com/dbname", "mysql://host.name.com/dbname"),
        Arguments.of("jdbc:mysql://username:pwd@host.name.com/dbname", "mysql://host.name.com/dbname"),
        Arguments.of("jdbc:mysql://username:pwd@host.name.com/dbname?key=value;key2=value2", "mysql://host.name.com/dbname"),
        Arguments.of("jdbc:mysql://username:pwd@myhost1:1111,username:pwd@myhost2:2222/db", "mysql://myhost1:1111,myhost2:2222/db"),
        Arguments.of("jdbc:mysql://address=(host=myhost1)(port=1111)(user=sandy)(password=secret),address=(host=myhost2)(port=2222)(user=finn)(password=secret)/db", "oracle:thin:@10.253.102.122:1521:dg01")
    );
  }

}