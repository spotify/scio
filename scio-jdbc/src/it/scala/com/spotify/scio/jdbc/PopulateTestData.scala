package com.spotify.scio.jdbc

import com.spotify.scio.jdbc.sharded.JdbcUtils

import java.sql.DriverManager

object PopulateTestData {

  import JdbcIOIT._

  // See https://learn.microsoft.com/en-us/sql/connect/ado-net/sql/compare-guid-uniqueidentifier-values?view=sql-server-ver16
  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.createConnection(connection)
    try {
      val stmt = conn.createStatement()
      val query = s"""DROP TABLE IF EXISTS $tableId;
                     |CREATE TABLE $tableId
                     |(
                     |    guid UNIQUEIDENTIFIER
                     |        CONSTRAINT guid_default DEFAULT
                     |        NEWSEQUENTIALID() ROWGUIDCOL,
                     |    name VARCHAR(60),
                     |
                     |    CONSTRAINT guid_pk PRIMARY KEY (guid)
                     |);
                     |INSERT INTO $tableId (guid, name)
                     |VALUES
                     | (CAST('3AAAAAAA-BBBB-CCCC-DDDD-2EEEEEEEEEEE' AS UNIQUEIDENTIFIER), 'Bob'),
                     | (CAST('2AAAAAAA-BBBB-CCCC-DDDD-1EEEEEEEEEEE' AS UNIQUEIDENTIFIER), 'Alice'),
                     | (CAST('1AAAAAAA-BBBB-CCCC-DDDD-3EEEEEEEEEEE' AS UNIQUEIDENTIFIER), 'Carol');
                     |""".stripMargin
      try {
        stmt.execute(query)
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }
  }
}
