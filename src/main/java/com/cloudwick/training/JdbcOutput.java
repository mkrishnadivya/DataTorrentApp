package com.cloudwick.training;

import com.datatorrent.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;
import com.datatorrent.lib.util.KeyValPair;

import javax.validation.constraints.NotNull;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by divya on 8/16/16.
 */
public class JdbcOutput extends AbstractJdbcTransactionableOutputOperator<KeyValPair<String, Integer>> {

    @NotNull
    String tableName;

    @Override
    protected String getUpdateCommand() {
        return "insert into  " + tableName + " (word, count) values (?, ?) on duplicate key update count=values(count)";
    }

    @Override
    protected void setStatementParameters(PreparedStatement statement, KeyValPair<String, Integer> tuple) throws SQLException {
        statement.setString(1, tuple.getKey());
        statement.setInt(2, tuple.getValue());
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
