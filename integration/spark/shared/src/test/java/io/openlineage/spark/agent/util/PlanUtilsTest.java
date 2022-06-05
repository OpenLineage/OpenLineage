/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PlanUtilsTest {

  @ParameterizedTest
  @MethodSource("provideJdbcUrls")
  void testJdbcUrlSanitizer(String jdbcUrl, String expectedResult) {

    String actualResult = JdbcUtils.sanitizeJdbcUrl(jdbcUrl);

    assertEquals(expectedResult, actualResult);
  }

  private static Stream<Arguments> provideJdbcUrls() {
    return Stream.of(
        Arguments.of(
            "jdbc:sqlserver://localhost;user=MyUserName;password=P%20%4021",
            "sqlserver://localhost;"),
        Arguments.of(
            "jdbc:sqlserver://localhost;USER=MyUserName;paSSword=P%20%4021",
            "sqlserver://localhost;"),
        Arguments.of(
            "jdbc:sqlserver://localhost\\instance1;databaseName=AdventureWorks;integratedSecurity=true",
            "sqlserver://localhost\\instance1;databaseName=AdventureWorks;integratedSecurity=true"),
        Arguments.of(
            "jdbc:sqlserver://;serverName=3ffe:8311:eeee:f70f:0:5eae:10.203.31.9\\instance1;integratedSecurity=true;",
            "sqlserver://;serverName=3ffe:8311:eeee:f70f:0:5eae:10.203.31.9\\instance1;integratedSecurity=true;"),
        Arguments.of(
            "jdbc:db2://test.host.com:5021/test:user=dbadm;password=dbadm;",
            "db2://test.host.com:5021/test:"),
        Arguments.of("jdbc:db2://server:user=dbadm", "db2://server:"),
        Arguments.of(
            "jdbc:db2://[2001:DB8:0:0:8:800:200C:417A]:5021/test",
            "db2://[2001:DB8:0:0:8:800:200C:417A]:5021/test"),
        Arguments.of(
            "jdbc:db2://[2001:DB8:0:0:8:800:200C:417A]:5021/test:user=dba%20adm;password=db%40dm;",
            "db2://[2001:DB8:0:0:8:800:200C:417A]:5021/test:"),
        Arguments.of(
            "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true",
            "postgresql://localhost/test?ssl=true"),
        Arguments.of(
            "jdbc:postgresql://localhost:5432/test?user=fred&password=secret&ssl=true",
            "postgresql://localhost:5432/test?ssl=true"),
        Arguments.of(
            "jdbc:postgresql://192.168.1.1:5432/test?user=fred&password=secret&ssl=true",
            "postgresql://192.168.1.1:5432/test?ssl=true"),
        Arguments.of("jdbc:postgresql://192.168.1.1:5432", "postgresql://192.168.1.1:5432"),
        Arguments.of(
            "jdbc:oracle:thin:us%2Ar/p%40%24%24w0rd@some.host:1521:orcl",
            "oracle:thin:@some.host:1521:orcl"),
        Arguments.of(
            "jdbc:oracle:thin:@10.253.102.122:1521:dg01?key=value",
            "oracle:thin:@10.253.102.122:1521:dg01?key=value"),
        Arguments.of(
            "jdbc:oracle:thin:user/password@some.host:1521:orcl",
            "oracle:thin:@some.host:1521:orcl"),
        Arguments.of(
            "jdbc:oracle:thin:user/password@//localhost:1521/serviceName",
            "oracle:thin:@//localhost:1521/serviceName"),
        Arguments.of(
            "jdbc:oracle:thin:@(description=(address=(protocol=tcp)(port=1521)(host=prodHost)))(connect_data=(INSTANCE_NAME=ORCL)))",
            "oracle:thin:@(description=(address=(protocol=tcp)(port=1521)(host=prodHost)))(connect_data=(INSTANCE_NAME=ORCL)))"),
        Arguments.of(
            "jdbc:oracle:thin:@(DESCRIPTION= (LOAD_BALANCE=on) (ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host1) (PORT=1521)) (ADDRESS=(PROTOCOL=TCP)(HOST=host2)(PORT=5221))) (CONNECT_DATA=(SERVICE_NAME=orcl)))",
            "oracle:thin:@(DESCRIPTION= (LOAD_BALANCE=on) (ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host1) (PORT=1521)) (ADDRESS=(PROTOCOL=TCP)(HOST=host2)(PORT=5221))) (CONNECT_DATA=(SERVICE_NAME=orcl)))"),
        Arguments.of("jdbc:oracle:thin:@localhost:1521:orcl", "oracle:thin:@localhost:1521:orcl"),
        Arguments.of(
            "jdbc:oracle:thin:@//localhost:1521/serviceName",
            "oracle:thin:@//localhost:1521/serviceName"),
        Arguments.of(
            "jdbc:oracle:thin:@dbname_high?TNS_ADMIN=/Users/test/wallet_dbname",
            "oracle:thin:@dbname_high?TNS_ADMIN=/Users/test/wallet_dbname"),
        Arguments.of(
            "jdbc:mysql://10.253.102.122:1521/dbname;key=value",
            "mysql://10.253.102.122:1521/dbname;key=value"),
        Arguments.of(
            "jdbc:mysql://10.253.102.122:1521/dbname;user=user;password=pwd;key=value",
            "mysql://10.253.102.122:1521/dbname;key=value"),
        Arguments.of("jdbc:mysql://host.name.com/dbname", "mysql://host.name.com/dbname"),
        Arguments.of(
            "jdbc:mysql://username:pwd@host.name.com/dbname", "mysql://host.name.com/dbname"),
        Arguments.of(
            "jdbc:mysql://username:pwd@host.name.com/dbname?key=value;key2=value2",
            "mysql://host.name.com/dbname?key=value;key2=value2"),
        Arguments.of(
            "jdbc:mysql://username:pwd@myhost1:1111,username:pwd@myhost2:2222/db",
            "mysql://myhost1:1111,myhost2:2222/db"),
        Arguments.of(
            "jdbc:mysql://address=(host=myhost1)(port=1111)(user=sandy)(password=secret),address=(host=myhost2)(port=2222)(user=finn)(password=secret)/db",
            "mysql://address=(host=myhost1)(port=1111),address=(host=myhost2)(port=2222)/db"));
  }
}
