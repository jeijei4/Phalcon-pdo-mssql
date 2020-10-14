<?php

/**
 * Class MssqlPdo, MssqlDialect
 * Connecting to Microsoft SQL Server PDO driver With Phalcon Framework, via 'ODBC' or 'DBLIB' or 'SQLSRV'
 * Original work: https://github.com/fishjerky/phalcon-mssql
 * By Davide Airaghi (www.airaghi.net)
 * Modified by JeiHO
 * Version: 2019.2.7
 * PhalconPHP: 'Phalcon V3.4.2'
 * Tested with: 'CentOS 7.5', 'PHP V5.6.38', 'PHP V7.2.15', 'Sql server 2008'
 */

use Phalcon\Db\Dialect;
//use Phalcon\Db\DialectInterface;
use Phalcon\Db\Column;

/*
*Adapter: https://github.com/phalcon/cphalcon/blob/v3.4.5/phalcon/db/adapter.zep#L32
*Pdo extends Adapter: https://github.com/phalcon/cphalcon/blob/v3.4.5/phalcon/db/adapter/pdo.zep#L48
*/
use Phalcon\Db\Adapter\Pdo as AdapterPdo; 

use Phalcon\Events\EventsAwareInterface;
use Phalcon\Db\AdapterInterface;


class MssqlDialect extends Dialect //implements DialectInterface
{
    /*
     * list of "search&replace" to make "happy" the PhalconPHP query analyzer and to use SQL Server Functions ...
     * key=>fake function name , value=>replacement and call to the real sql server function
     * @var array
     */
    protected $functions_translate = array(
        '_sqlsrv_datediff_d(' => 'datediff(d,',
        '_sqlsrv_right(' => 'right(',
        '_sqlsrv_left(' => 'left(',
        '_sqlsrv_ltrim(' => 'ltrim(',
        '_sqlsrv_rtrim(' => 'rtrim('
    );

    public function forUpdate($sqlQuery)
    {
        $sql = $sqlQuery . ' WITH (UPDLOCK) ';
        return $sql;
    }

    public function shareLock($sqlQuery)
    {
        $sql = $sqlQuery . ' WITH (NOLOCK) ';
        return $sql;
    }

    public function select(array $definition) // 2.x
    {
        $tables;
        $columns;
        $escapeChar;
        $columnItem;
        $column;
        $selectedColumns;
        $columnSql;
        $columnDomainSql;
        $columnAlias;
        $selectedTables;    //global
        $sqlJoin;
        $joinExpressions;
        $joinCondition;
        $joinConditionsArray;
        $tablesSql;
        $columnDomain;
        $columnAliasSql;
        $columnsSql;
        $table;
        $sql;
        $joins;
        $join;
        $sqlTable;
        $whereConditions;
        $groupFields;
        $groupField;
        $groupItems;
        $havingConditions;
        $orderFields;
        $orderItem;
        $orderItems;
        $orderSqlItem;
        $sqlOrderType;
        $orderSqlItemType;
        $limitValue;
        $number;
        $offset;
        if (!is_array($definition)) {
            throw new Phalcon\Db\Exception("Invalid SELECT definition");
        }
        if (isset($definition['tables'])) {
            $tables = $definition["tables"];
        } else {
            throw new Phalcon\Db\Exception("The index 'tables' is required in the definition array");
        }
        if (isset($definition['columns'])) {
            $columns = $definition["columns"];
        } else {
            throw new Phalcon\Db\Exception("The index 'columns' is required in the definition array");
        }
        /*      if globals_get("db.escape_identifiers") {
                let escapeChar = this->_escapeChar;
                } else {
                let escapeChar = null;
                }*/
        //$escapeChar = array('[',']');
        $escapeChar = "\"";
        if (is_array($columns)) {
            $selectedColumns = array();
            foreach ($columns as $column) {
                /**
                 * Escape column name
                 */
                $columnItem = $column[0];
                if (is_array($columnItem)) {
                    $columnSql = $this->getSqlExpression($columnItem, $escapeChar);
                } else {
                    if ($columnItem == "*") {
                        $columnSql = $columnItem;
                    } else {
                        /*if globals_get("db.escape_identifiers") {
                          let columnSql = escapeChar . columnItem . escapeChar;
                          } else {
                          let columnSql = columnItem;
                          }*/
                        $columnSql = $columnItem;
                    }
                }
                /**
                 * Escape column domain
                 */
                if (isset($column[1])) {
                    $columnDomain = $column[1];
                    if ($columnDomain) {
                        /*if globals_get("db.escape_identifiers") {
                          let columnDomainSql = escapeChar . columnDomain . escapeChar . "." . columnSql;
                          } else {
                          let columnDomainSql = columnDomain . "." . columnSql;
                          }*/
                        $columnDomainSql = $columnDomain . "." . $columnSql;
                    } else {
                        $columnDomainSql = $columnSql;
                    }
                } else {
                    $columnDomainSql = $columnSql;
                }
                /**
                 * Escape column alias
                 */
                if (isset($column[2])) {
                    $columnAlias = $column[2];
                    if ($columnAlias) {
                        /*if globals_get("db.escape_identifiers") {
                          let columnAliasSql = columnDomainSql . " AS " . escapeChar . columnAlias . escapeChar;
                          } else {
                          let columnAliasSql = columnDomainSql . " AS " . columnAlias;
                          }*/
                        $columnAliasSql = $columnDomainSql . " AS " . $columnAlias;
                    } else {
                        $columnAliasSql = $columnDomainSql;
                    }
                } else {
                    $columnAliasSql = $columnDomainSql;
                }
                $selectedColumns[] = $columnAliasSql;
            }
            $columnsSql = join(", ", $selectedColumns);
        } else {
            $columnsSql = $columns;
        }
        /**
         * Check and escape tables
         */
        if (is_array($tables)) {
            $selectedTables = array();
            foreach ($tables as $table) {
                $selectedTables[] = $this->getSqlTable($table, $escapeChar);
            }
            $tablesSql = join(", ", $selectedTables);
        } else {
            $tablesSql = $tables;
        }
        $sql = "SELECT $columnsSql FROM /*tbl*/ $tablesSql ";
        /**
         * Check for joins
         */
        $sqlJoins = '';
        if (isset($definition['joins'])) {
            $joins = $definition['joins'];
            foreach ($joins as $join) {
                $sqlTable = $this->getSqlTable($join["source"], $escapeChar);
                $selectedTables[] = $sqlTable;
                $sqlJoin = " " . $join["type"] . " JOIN " . $sqlTable;
                /**
                 * Check if the join has conditions
                 */
                $joinConditionsArray = $join['conditions'];
                if (isset($joinConditionsArray)) {
                    if (count($joinConditionsArray)) {
                        $joinExpressions = array();
                        foreach ($joinConditionsArray as $joinCondition) {
                            $joinExpressions[] = $this->getSqlExpression($joinCondition, $escapeChar);
                        }
                        $sqlJoin .= " ON " . join(" AND ", $joinExpressions) . " ";
                    }
                }
                $sqlJoins .= $sqlJoin;
            }
        }
        /**
         * Check for a WHERE clause
         */
        $sqlWhere = '';
        if (isset($definition['where'])) {
            $whereConditions = $definition['where'];
            if (is_array($whereConditions)) {
                $sqlWhere .= " WHERE " . $this->getSqlExpression($whereConditions, $escapeChar);
            } else {
                $sqlWhere .= " WHERE " . $whereConditions;
            }
        }
        /**
         * Check for a GROUP clause
         */
        $sqlGroup = '';
        if (isset($definition['group'])) {
            $groupFields = $definition['group'];
            $groupItems = array();
            foreach ($groupFields as $groupField) {
                $groupItems[] = $this->getSqlExpression($groupField, $escapeChar);
            }
            $sqlGroup = " GROUP BY " . join(", ", $groupItems);
            /**
             * Check for a HAVING clause
             */
            if (isset($definition['having'])) {
                $havingConditions = $definition['having'];
                $sqlGroup .= " HAVING " . $this->getSqlExpression($havingConditions, $escapeChar);
            }
        }
        /**
         * Check for a ORDER clause
         */
        $sqlOrder = '';
        $nolockTokens = array('id');    //token to trigger nolock hint
        if (isset($definition['order'])) {
            $nolock = false;
            $orderFields = $definition['order'];
            $orderItems = array();
            foreach ($orderFields as $orderItem) {
                $orderSqlItem = $this->getSqlExpression($orderItem[0], $escapeChar);
                /**
                 * In the numeric 1 position could be a ASC/DESC clause
                 */
                if (isset($orderItem[1])) {
                    $sqlOrderType = $orderItem[1];
                    $orderSqlItemType = $orderSqlItem . " " . $sqlOrderType;
                } else {
                    $orderSqlItemType = $orderSqlItem;
                }
                //check nolock
                if (!isset($orderItem[0]['name'])) {
                    $orderItem[0]['name'] = '';
                }
                if (in_array(strtolower($orderItem[0]['name']), $nolockTokens)) {
                    $nolock = true;
                } else {
                    $orderItems[] = $orderSqlItemType;
                }
            }
            if (count($orderItems)) {
                $sqlOrder = " ORDER BY /*rdr*/ " . join(", ", $orderItems);
            }
            if ($nolock) {
                $sql .= " with (nolock) ";
            }
        }
        $sql .= $sqlJoins . $sqlWhere . $sqlGroup . $sqlOrder;
        if (empty($sqlOrder)) {
            $sqlOrder == null;  //side effect, limit clause need =>  if (isset($sqlOrder) && !empty($sqlOrder))
        }
        /**
         * Check for a LIMIT condition - OLD
         */
        $limitValue = isset($definition["limit"]) ? $definition["limit"] : null;
        if (isset($limitValue)) {
            if (is_array($limitValue)) {
                $number = $limitValue["number"]['value'];
                $order = 'ORDER BY id';
                if (preg_match('/\ ORDER\ BY\ \/\*rdr\*\/\ (.*)$/i', $sql, $orx)) {
                    $orx = $orx[1];
                    $order = 'ORDER BY ' . $orx;
                } else {
                    $order = 'Order By ( SELECT COL_NAME(OBJECT_ID(\'' . $selectedTables[0] . '\'), 1) )';
                }
                // Check for a OFFSET condition
                if (isset($limitValue['offset'])) {
                    $offset = intval($limitValue['offset']['value']) + 1;
                    $number = intval($number);
                    if ($number < 1) {
                        $number++;
                    } // fix PhalconPHP 2.0.4+ ...
                    $sql = preg_replace('#\ ORDER\ BY\ .*#i', '', $sql);
                    $sql = preg_replace('#\ FROM\ \/\*tbl\*\/\ #', ', ROW_NUMBER() OVER (' . $order . ') AS RowNum FROM ', $sql);
                    // $sql = 'WITH Results_CTE AS ( '.$sql.'  ) SELECT * FROM Results_CTE WHERE RowNum >= '.$offset.' AND RowNum < '.$offset.' + '.$number.' ';
                    $sql = 'SELECT * FROM ( ' . $sql . ' ) subq WHERE RowNum >= ' . $offset . ' AND RowNum < ' . $offset . ' + ' . $number . ' ';
                } else {
                    $sql = $this->limit($sql, $number);
                }
            } else {
                $sql = $this->limit($sql, $number);
            }
        }
        /**
         * Check for a LIMIT condition - NEW
         */
        /*
		if (isset($definition['limit'])) {
            $limitValue = $definition["limit"];
            if (is_array($limitValue)) {
                $number = $limitValue["number"]['value'];
                if (isset($limitValue['offset'])) {
                    $sql = $this->limit($sql, '100 PERCENT');
                    $startIndex = $limitValue['offset']['value'] + 1;//index start from 1
					$endIndex = $startIndex + $number - 1;
                    $pos = strpos($sql, 'FROM');
                    $table = substr($sql, $pos + 4); //4 = FROM
                    $countPos = strpos($sql, 'COUNT');
                    if ($countPos) {
                        //if COUNT, take 'id' as default column, unless you have 'order'
                        if (isset($sqlOrder) && !empty($sqlOrder)) {
                            $sql = substr($sql, 0, $countPos) .  " *, ROW_NUMBER() OVER ($sqlOrder) AS rownum FROM $table";
                        } else {
                            $sql = substr($sql, 0, $countPos) . " *, ROW_NUMBER() OVER (Order By (SELECT COL_NAME(OBJECT_ID('{$selectedTables[0]}'), 1))) AS rownum FROM $table";
                        }
                    } else {
                        if (isset($sqlOrder) && !empty($sqlOrder)) {
                            $sql = substr($sql, 0, $pos) .  ", ROW_NUMBER() OVER ($sqlOrder) AS rownum FROM $table";
                        } else {
                            //if order is not giving, it will take first selected column for order.
                            $sql = substr($sql, 0, $pos) .  ", ROW_NUMBER() OVER (ORDER BY {$selectedColumns[0]}) AS rownum FROM $table";
                        }
                    }
                    //remove all column domain
                    $pureColumns = array();
                    foreach ($selectedColumns as $column) {
                        $pureColumn = substr($column, ($pos = strpos($column, '.')) !== false ? $pos + 1 : 0);
                        $pureColumns[] = $pureColumn;
                    }
                    $pureColumns = join(", ", $pureColumns);
                    $sql = "SELECT $pureColumns FROM ( $sql ) AS t WHERE t.rownum BETWEEN $startIndex AND $endIndex"; //don't break line
                } else {
                    $sql = $this->limit($sql, $number);
                }
            } else {
                $sql = $this->limit($sql, $number);
            }
        }
		*/
        // at this point we can do some "magic" ...
        $sql = $this->do_translate($sql);
        // echo $sql.'<br><br>';
        // echo '<pre>'; print_r($definition); echo '</pre><br><br>';
        return $sql;
    }

    public function limit($sqlQuery, $number)
    {
        $sql = preg_replace('/^SELECT\s/i', 'SELECT TOP ' . $number . ' ', $sqlQuery);
        return $sql;
    }

    /*
    public function getSqlTable($tables, $escapeChar = "\"")
    {
        if (!is_array($tables))
            return  $this->escaping($tables, $escapeChar);
        $result = array();
        foreach ($tables as $table) {
            $result[] = $this->escaping($table, $escapeChar);
        }
        return $result;
    }
    public function getSqlExpression($expressions, $escapeChar = "\"")
    {
        $domain = $this->escaping($expressions['domain'], $escapeChar);
        $name = $this->escaping($expressions['name'], $escapeChar);
        $result = "$domain.$name";
        return $result;
    }
    */
    protected function do_translate($sql)
    {
        foreach ($this->functions_translate as $find => $replace) {
            $sql = str_ireplace($find, $replace, $sql);
        }
        return $sql;
    }

    public function addColumn($tableName, $schemaName, \Phalcon\Db\ColumnInterface $column) // 2.x
    {
        $afterPosition;
        $sql;
        if (!is_object($column)) {
            throw new \Phalcon\Db\Exception("Column definition must be an object compatible with Phalcon\\Db\\ColumnInterface");
        }
        if ($schemaName) {
            $sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] ADD ";
        } else {
            $sql = "ALTER TABLE [" . $tableName . "] ADD ";
        }
        $sql .= "[" . $column->getName() . "] " . $this->getColumnDefinition($column);
        /* NOT NULL  alter with not ll is not allowed in mssql
           if ($column->isNotNull()) {
           $sql .= " NOT NULL";
           }
         */
        if ($column->isFirst()) {
            $sql .= " FIRST";
        } else {
            $afterPosition = $column->getAfterPosition();
            if ($afterPosition) {
                $sql .= " AFTER " . $afterPosition;
            }
        }
        return $sql;
    }
    // {
    //    //exec sp_columns 'table name'
    // }
    /**
     * Gets the column name in MsSQL
     *
     * @param Phalcon\Db\ColumnInterface column
     * @return string
     */
    public function getColumnDefinition(\Phalcon\Db\ColumnInterface $column)  // 2.x
    {
        $columnSql;
        $size;
        $scale;
        if (!is_object($column)) {
            throw new \Phalcon\Db\Exception("Column definition must be an object compatible with Phalcon\\Db\\ColumnInterface");
        }
        switch ((int)$column->getType()) {
            case \Phalcon\Db\Column::TYPE_INTEGER:
                $columnSql = "INT";
                break;
            case \Phalcon\Db\Column::TYPE_DATE:
                $columnSql = "DATE";
                break;
            case \Phalcon\Db\Column::TYPE_VARCHAR:
                $columnSql = "NCHAR(" . $column->getSize() . ")";
                break;
            case \Phalcon\Db\Column::TYPE_DECIMAL:
                $columnSql = "DECIMAL(" . $column->getSize() . "," . $column->getScale() . ")";
                break;
            case \Phalcon\Db\Column::TYPE_DATETIME:
                $columnSql = "DATETIME";
                break;
            case \Phalcon\Db\Column::TYPE_CHAR:
                $columnSql = "CHAR(" . $column->getSize() . ")";
                break;
            case \Phalcon\Db\Column::TYPE_TEXT:
                $columnSql = "TEXT";
                break;
            case \Phalcon\Db\Column::TYPE_FLOAT:
                $columnSql = "NUMERIC"; //FLOAT can't have range
                $size = $column->getSize();
                if ($size) {
                    $scale = $column->getScale();
                    $columnSql .= "(" . $size;
                    if ($scale) {
                        $columnSql .= "," . $scale . ")";
                    } else {
                        $columnSql .= ")";
                    }
                }
                break;
            default:
                throw new \Phalcon\Db\Exception("Unrecognized Mssql data type: " . $column->getType());
        }
        return $columnSql;
    }

    public function modifyColumn($tableName, $schemaName, \Phalcon\Db\ColumnInterface $column, \Phalcon\Db\ColumnInterface $currentColumn = NULL) // 2.x
    {
        $sql;
        if (!is_object($column)) {
            throw new \Phalcon\Db\Exception("Column definition must be an object compatible with Phalcon\\Db\\ColumnInterface");
        }
        if ($schemaName) {
            $sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] ALTER COLUMN ";
        } else {
            $sql = "ALTER TABLE [" . $tableName . "] ALTER COLUMN ";
        }
        $sql .= "[" . $column->getName() . "] " . $this->getColumnDefinition($column);
        /* NOT NULL  alter with not ll is not allowed in mssql
           if ($column->isNotNull()) {
           $sql .= " NOT NULL";
           }
         */
        return $sql;
    }

    public function dropColumn($tableName, $schemaName, $columnName)
    {
        $sql;
        if ($schemaName) {
            $sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] DROP COLUMN ";
        } else {
            $sql = "ALTER TABLE [" . $tableName . "] DROP COLUMN ";
        }
        $sql .= "[$columnName]";
        return $sql;
    }

    public function addIndex($tableName, $schemaName, \Phalcon\Db\IndexInterface $index) // 2.x
    {
        $sql;
        if (!is_object($index)) {
            throw new Phalcon\Db\Exception("Index parameter must be an object compatible with Phalcon\\Db\\IndexInterface");
        }
        if ($schemaName) {
            $sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] ADD INDEX ";
        } else {
            $sql = "ALTER TABLE [" . $tableName . "] ADD INDEX ";
        }
        $sql .= "[" . $index->getName() . "] " . $this->getColumnDefinition($index->getColumns());
        return $sql;
    }

    /*
     * not done yet
     CREATE UNIQUE NONCLUSTERED INDEX (indexname)
     ON dbo.YourTableName(columns to include)
     */
    public function dropIndex($tableName, $schemaName, $indexName)
    {
        $sql;
        if ($schemaName) {
            $sql = "DROP INDEX ($indexName) on [" . $schemaName . "].[" . $tableName . "] ";
        } else {
            $sql = "DROP INDEX ($indexName) on  [" . $tableName . "] ";
        }
        return $sql;
    }

    /*
     * not done yet
     */
    public function addPrimaryKey($tableName, $schemaName, \Phalcon\Db\IndexInterface $index) // 2.x
    {
        $sql;
        if (!is_object($index)) {
            throw new Phalcon\Db\Exception("Index parameter must be an object compatible with Phalcon\\Db\\IndexInterface");
        }
        if ($schemaName) {
            $sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] ADD PRIMARY KEY ";
        } else {
            $sql = "ALTER TABLE [" . $tableName . "] ADD PRIMARY KEY ";
        }
        $sql .= "(" . $this->getColumnList($index->getColumns()) . ")";
        return $sql;
    }

    public function dropPrimaryKey($tableName, $schemaName)
    {
        $sql;
        if ($schemaName) {
            $sql = "ALTER TABLE [" . $schemaName . "].[" . $tableName . "] DROP PRIMARY KEY ";
        } else {
            $sql = "ALTER TABLE [" . $tableName . "] DROP PRIMARY KEY ";
        }
        return $sql;
    }

    public function tableExists($tableName, $schemaName = null)
    {
        $sql = "SELECT COUNT(*) FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_NAME] = '$tableName' ";
        if ($schemaName) {
            $sql = $sql . "AND TABLE_SCHEMA = '$schemaName'";
        }
        return $sql;
    }

    /**
     * Generates SQL checking for the existence of a schema.view
     *
     * @param string viewName
     * @param string schemaName
     * @return string
     */
    public function viewExists($viewName, $schemaName = null)
    {
        if ($schemaName) {
            return "SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE table_name = '$viewName' and table_schema = '$schemaName'";
        }
        return "SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE table_name = '$viewName'";
    }

    public function describeColumns($table, $schema = null)
    {
        /* missing information for auto increment
           $sql = "select * from [INFORMATION_SCHEMA].[COLUMNS] where [TABLE]_NAME='$table' ";
           if ($schemaName) {
           $sql = $sql . "AND TABLE_SCHEMA = '$schemaName'";
           }
         */
        $sql = "exec sp_columns [$table], [$schema]";
        return $sql;
    }

    /**
     * Returns a list of the tables in the database.
     *
     * @return array
     */
    public function listTables($schemaName = null)
    {
        //$sql =  "SELECT name FROM sysobjects WHERE type = 'U' ORDER BY name";
        $sql = "SELECT table_name FROM [INFORMATION_SCHEMA].[TABLES] ";
        if ($schemaName) {
            $sql = $sql . " WHERE TABLE_SCHEMA = '$schemaName'";
        }
        return $sql;
    }

    /**
     * Generates the SQL to list all views of a schema or user
     *
     * @param string schemaName
     * @return array
     */
    public function listViews($schemaName = null)
    {
        if ($schemaName) {
            return "SELECT [TABLE_NAME] AS view_name FROM [INFORMATION_SCHEMA].[VIEWS] WHERE `TABLE_SCHEMA` = '" . $schemaName . "' ORDER BY view_name";
        }
        return "SELECT [TABLE_NAME] AS view_name FROM [INFORMATION_SCHEMA].[VIEWS] ORDER BY view_name";
    }

    public function createView($viewName, array $definition, $schemaName = NULL)  // 2.x
    {
        $view;
        $viewSql;
        if (!isset($definition['sql'])) {
            throw new Phalcon\Db\Exception("The index 'sql' is required in the definition array");
        }
        $viewSql = $definition['sql'];
        if ($schemaName) {
            $view = "[$schemaName].[$viewName]";
        } else {
            $view = "[$viewName]";
        }
        return "CREATE VIEW $view AS $viewSql";
    }

    /**
     * Generates SQL to create a view
     *
     * @param string viewName
     * @param array definition
     * @param string schemaName
     * @return string
     */
    public function dropView($viewName, $schemaName = NULL, $ifExists = NULL) // 2.x
    {
        $sql = "";
        $view;
        if ($schemaName) {
            $view = "$schemaName.$viewName";
        } else {
            $view = "$viewName";
        }
        if ($ifExists) {
            if ($schemaName) {
                $sql = "IF EXISTS ( SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = '$viewName' AND TABLE_SCHEMA = '$schemaName' ) ";
            } else {
                $sql = "IF EXISTS ( SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = '$view' ) ";
            }
        }
        $sql .= "DROP VIEW " . $view;
        return $sql;
    }
    /**
     * Generates SQL to drop a view
     *
     * @param string viewName
     * @param string schemaName
     * @param boolean ifExists
     * @return string
     */
    /**
     * Generates SQL to query indexes on a table
     *
     * @param   string table
     * @param   string schema
     * @return  string
     * TODO schema not finish yet
     */
    public function describeIndexes($table, $schema = null)
    {
        $sql = "SELECT * FROM sys.indexes ind INNER JOIN sys.tables t ON ind.object_id = t.object_id WHERE t.name = '$table' ";
        if ($schema) {
            //$sql .= "AND t."
        }
        return $sql;
    }

    /**
     * Generates SQL to query foreign keys on a table
     *
     * @param   string table
     * @param   string schema
     * @return  string
     */
    public function describeReferences($table, $schema = null)
    {
        $sql = "SELECT TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME,REFERENCED_TABLE_SCHEMA,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME IS NOT NULL AND ";
        if ($schema) {
            $sql .= "CONSTRAINT_SCHEMA = '" . $schema . "' AND TABLE_NAME = '" . $table . "'";
        } else {
            $sql .= "TABLE_NAME = '" . $table . "'";
        }
        return $sql;
    }

    /**
     * Generates the SQL to describe the table creation options
     *
     * @param   string table
     * @param   string schema
     * @return  string
     */
    public function tableOptions($table, $schema = null)
    {
        $sql = "SELECT TABLES.TABLE_TYPE AS table_type,TABLES.AUTO_INCREMENT AS auto_increment,TABLES.ENGINE AS engine,TABLES.TABLE_COLLATION AS table_collation FROM INFORMATION_SCHEMA.TABLES WHERE ";
        if ($schema) {
            $sql .= "TABLES.TABLE_SCHEMA = '" . $schema . "' AND TABLES.TABLE_NAME = '" . $table . "'";
        } else {
            $sql .= "TABLES.TABLE_NAME = '" . $table . "'";
        }
        return $sql;
    }

    public function addForeignKey($tableName, $schemaName, \Phalcon\Db\ReferenceInterface $reference)
    {
    }

    public function dropForeignKey($tableName, $schemaName, $referenceName)
    {
    }

    public function createTable($tableName, $schemaName, array $definition)
    {
    }

    public function dropTable($tableName, $schemaName)
    {
    } // 2.x

    public function supportsSavepoints()
    {
    }

    public function supportsReleseSavepoints()
    {
    }

    public function createSavepoint($name)
    {
    }

    public function releaseSavepoint($name)
    {
    }

    public function rollbackSavepoint($name)
    {
    }

    public function sharedLock($sqlQuery)
    {
    }

    protected function escaping($item, $escapeChar)
    {
        if (is_array($escapeChar)) {
            return $escapeChar[0] . $item . $escapeChar[1];
        } else {
            return $escapeChar . $item . $escapeChar;
        }
    }
}

class MssqlPdo extends AdapterPdo implements EventsAwareInterface, AdapterInterface
{
    protected $instance;
    protected $_lastID = false;
    protected $_type = 'mssql';
    protected $_dialectType = 'mssql';

    public function __construct(array $descriptor)
    {
        $this->connect($descriptor);
        $this->instance = microtime();
    }

    /**
     * @param array $descriptor
     */
    public function connect(array $descriptor = null)
    {
        try {
            switch (strtoupper(trim($descriptor['pdoType']))) {
                case 'ODBC' :
                    $dns = 'odbc:Driver={' . $descriptor['driver'] . '};Server=' . $descriptor['host'] . ((!empty($descriptor['port'])) ? (';Port=' . $descriptor['port']) : '') . ';Database=' . $descriptor['dbname'] . ';Uid=' . $descriptor['username'] . ';Pwd=' . $descriptor['password'] . ';Trusted_Connection=No;Encrypt=Yes;TrustServerCertificate=Yes;MultipleActiveResultSets=Yes;IntegratedSecurity=Yes';
                    if (!empty($descriptor['failover'])) {
                        $dns .= ';Failover_Partner=' . $descriptor['failover'];
                    }

                    $this->_pdo = new \PDO($dns);
                    $this->_pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
                    $this->_pdo->setAttribute(\PDO::ATTR_STRINGIFY_FETCHES, true);
                    $this->_pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);
                    break;

                case 'DBLIB' :

                    $portDB = '';
                    if (isset($descriptor['port'])) {
                        $seperator = ':';
                        if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
                            $seperator = ',';
                        }
                        $portDB = $seperator . $descriptor['port'];
                    }

                    $dns = 'dblib:host=' . $descriptor['host'] . $portDB . ';dbname=' . $descriptor['dbname'] . ';MultipleActiveResultSets=1;Encrypt=true;TrustServerCertificate=true;charset=UTF-8' . ((!empty($descriptor['failover'])) ? (';Failover_Partner=' . $descriptor['failover']) : '');

                    $this->_pdo = new \PDO($dns, $descriptor['username'], $descriptor['password']);
                    $this->_pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
                    $this->_pdo->setAttribute(\PDO::ATTR_STRINGIFY_FETCHES, true);
                    break;

                case 'SQLSRV' :

                    $dns = 'sqlsrv:Server=' . $descriptor['host'] . ((!empty($descriptor['port'])) ? (',' . $descriptor['port']) : '') . ';Database=' . $descriptor['dbname'] . '';

                    $this->_pdo = new \PDO($dns, $descriptor['username'], $descriptor['password']);
                    $this->_pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
                    $this->_pdo->setAttribute(\PDO::ATTR_STRINGIFY_FETCHES, true);
                    $this->_pdo->setAttribute(\PDO::SQLSRV_ATTR_DIRECT_QUERY, true);
                    break;

                default :
                    throw new \Phalcon\Db\Exception('The ' . $descriptor['pdoType'] . ' driver is not configured.');
                    break;
            }

            $this->execute('SET QUOTED_IDENTIFIER ON');
            $this->_dialect = new MssqlDialect();
        } catch (PDOException $e) {
            throw new \Phalcon\Db\Exception('PDO error: ' . $e->getMessage());
        } catch (Exception $e) {
            throw new \Phalcon\Db\Exception($e->getMessage());
        }
    }

    public function describeColumns($table, $schema = null)
    {
        $columns = array();
        $oldColumn = null;

        //1. get PK
        $primaryKeys = array();
        $describeKeys = $this->fetchAll("exec sp_pkeys @table_name = [$table]", \Phalcon\Db::FETCH_ASSOC);
        foreach ($describeKeys as $field) {
            $primaryKeys[$field['COLUMN_NAME']] = true;
        }

        //2.get column description
        $describe = $this->fetchAll("exec sp_columns [$table], [$schema]", \Phalcon\Db::FETCH_ASSOC);

        /**
         * Field Indexes: 0:name, 1:type, 2:not null, 3:key, 4:default, 5:extra
         */
        foreach ($describe as $field) {
            /**
             * By default the bind types is two
             */
            $definition = array(
                "bindType" => 2,
                "unsigned" => false,
            );
            /**
             * By checking every column type we convert it to a Phalcon\Db\Column
             */
            $autoIncrement = false;
            switch ($field['TYPE_NAME']) {
                case 'int identity':
                    $definition['type'] = Column::TYPE_INTEGER;
                    $definition["isNumeric"] = true;
                    $definition['bindType'] = Column::BIND_PARAM_INT;
                    $autoIncrement = true;
                    break;
                case 'int':
                    $definition['type'] = Column::TYPE_INTEGER;
                    $definition["isNumeric"] = true;
                    $definition['bindType'] = Column::BIND_PARAM_INT;
                    break;
                case 'nchar':
                    $definition['type'] = Column::TYPE_VARCHAR;
                    break;
                case 'char':
                    $definition['type'] = Column::TYPE_CHAR;
                    break;
                case 'smallint':
                    $definition['type'] = Column::TYPE_INTEGER;
                    $definition["isNumeric"] = true;
                    $definition['bindType'] = Column::BIND_PARAM_INT;
                    break;
                case 'float':
                    $definition['type'] = Column::TYPE_DECIMAL;
                    $definition["isNumeric"] = true;
                    $definition['bindType'] = Column::BIND_SKIP;
                    break;
                case 'datetime':
                    $definition["type"] = Column::TYPE_DATETIME;
                    break;
                case 'date':
                    $definition["type"] = Column::TYPE_DATE;
                    break;
                case 'decimal':
                    $definition["type"] = Column::TYPE_DECIMAL;
                    $definition["isNumeric"] = true;
                    $definition["bindType"] = Column::BIND_PARAM_DECIMAL;
                    break;
                case 'text':
                    $definition["type"] = Column::TYPE_TEXT;
                    break;
                case 'numeric':
                    $definition["type"] = Column::TYPE_FLOAT;
                    $definition["isNumeric"] = true;
                    $definition["bindType"] = Column::TYPE_DECIMAL;
                    break;
                default:
                    //echo $field['COLUMN_NAME'] . 'has no match type: ' .  $field['TYPE_NAME'] . PHP_EOL;
                    $definition['type'] = Column::TYPE_VARCHAR;
                //$definition['bindType'] = Column::BIND_PARAM_STR;
            }

            /**
             * If the column type has a parentheses we try to get the column size from it
             */
            $definition["size"] = (int)$field['LENGTH'];
            $definition["precision"] = (int)$field['PRECISION'];
            /**
             * Positions
             */
            if (!$oldColumn) {
                $definition["first"] = true;
            } else {
                $definition["after"] = $oldColumn;
            }
            /**
             * Check if the field is primary key
             */
            if (isset($primaryKeys[$field['COLUMN_NAME']])) {
                $definition["primary"] = true;
            }
            /**
             * Check if the column allows null values
             */
            $definition["notNull"] = ($field['NULLABLE'] == 0);

            if ($field['SCALE'] || $field['SCALE'] == '0') {

                /*
                 //Phalcon/Db/Column type does not support scale parameter
                 $definition["scale"] = (int)$field['SCALE'];
                 */

                $definition["size"] = $definition['precision'];
            }
            /**
             * Check if the column is auto increment
             */
            if ($autoIncrement) {
                $definition["autoIncrement"] = true;
            }
            /**
             * Every route is stored as a Phalcon\Db\Column
             */
            $columnName = $field['COLUMN_NAME'];
            $columns[] = new \Phalcon\Db\Column($columnName, $definition);
            $oldColumn = $columnName;
        }

        return $columns;
    }

    /**
     * Appends a LIMIT clause to $sqlQuery argument
     *
     * <code>
     *    echo $connection->limit("SELECT * FROM robots", 5);
     * </code>
     *
     * @param    string sqlQuery
     * @param    int number
     * @return    string
     */
    public function limit($sqlQuery, $number)
    {
        $dialect = $this->_dialect;
        return $dialect->limit($sqlQuery, $number);
    }

    public function executePrepared(\PDOStatement $statement, array $placeholders, $dataTypes)  // 2.x
    {
        /*
        $sql = ($statement->queryString);
        if (substr($sql,0,6)=='UPDATE' || substr($sql,0,6)=='INSERT') {
            echo $sql."<br>";
            print_r($placeholders);
            die;
        }
        */
        // fix PhalconPHP 2.0.4+ ...
        $used_pp = false;
        $used_qm = false;
        $ph_counter = 0;
        $_placeholders = array();
        foreach ($placeholders as $pk => $pv) {
            if ($pk == '?') {
                $pk = ':' . ($ph_counter++);
                $used_qm = true;
            }
            if (is_int(($pk))) {
                $pk = ':' . $pk;
            }
            if (substr($pk, 0, 1) == ':') {
                $used_pp = true;
                $_placeholders[intval(substr($pk, 1))] = $pv;
            }
        }
        $placeholders = $_placeholders;
        if (!is_array($dataTypes)) {
            if ($dataTypes) {
                $dataTypes = array($dataTypes);
            } else {
                $dataTypes = array();
            }
        }
        $_datatypes = array();
        foreach ($dataTypes as $pk => $pv) {
            $_pk = substr($pk, 0, 1);
            if (in_array($_pk, array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9'))) {
                $_datatypes[$pk] = $pv;
            }
        }
        $dataTypes = $_datatypes;
        // fine fix PhalconPHP 2.0.4+ ...
        // $placeholders = array( ':0' => '/' ); $dataTypes    = array( ':0' => \Phalcon\Db\Column::BIND_PARAM_STR );
        if (count($placeholders) != count($dataTypes)) {
            if (count($dataTypes) > count($placeholders)) {
                array_splice($dataTypes, count($placeholders));
            }
            // fix PhalconPHP 2.0.4+ ...
            // $index = count($placeholders)-1 ;
            $last_index = count($dataTypes);
            $first_index = count($placeholders);
            // echo $first_index.' '.$last_index.''.PHP_EOL;
            if ($last_index <= 0) {
                // dataTypes ? vuoto ....
                $first_index = 0;
                $last_index = count($placeholders);
            }
            for ($index = $first_index; $index < $last_index; $index++) {
                // print_r($placeholders);die;
                if (isset($placeholders[$index]) || array_key_exists($index, $placeholders)) {
                    $val = $placeholders[$index];
                    $val = strtolower(gettype($val));
                    switch ($val) {
                        case 'integer':
                            $newval = \Phalcon\Db\Column::BIND_PARAM_INT;
                            break;
                        case 'float':
                        case 'double':
                            $newval = \Phalcon\Db\Column::BIND_PARAM_DECIMAL;
                            break;
                        case 'null':
                            $newval = \Phalcon\Db\Column::BIND_PARAM_NULL;
                            break;
                        case 'string':
                        default:
                            $newval = \Phalcon\Db\Column::BIND_PARAM_STR;
                            break;
                    }
                    $dataTypes[] = $newval;
                } else {
                }
            }
            // fine fix PhalconPHP 2.0.4+ ...
        }
        // fix PhalconPHP 2.0.4+ ...
        if ($used_pp) {
            $_placeholders = $placeholders;
            $_datatypes = $dataTypes;
            $dataTypes = array();
            $placeholders = array();
            foreach ($_placeholders as $pk => $pv) {
                $placeholders[str_replace('::', ':', ':' . $pk)] = $pv;
            }
            foreach ($_datatypes as $pk => $pv) {
                $dataTypes[str_replace('::', ':', ':' . $pk)] = $pv;
            }
        }
        if ($used_qm) {
            $_placeholders = $placeholders;
            $_datatypes = $dataTypes;
            $dataTypes = array();
            $placeholders = array();
            foreach ($_placeholders as $pk => $pv) {
                $placeholders[str_replace(':', '', $pk)] = $pv;
            }
            foreach ($_datatypes as $pk => $pv) {
                $dataTypes[str_replace(':', '', $pk)] = $pv;
            }
        }
        // fine fix PhalconPHP 2.0.4+ ...
        if (defined('BLOCKSQL')) {
            echo 'STATEMENT: ';
            print_r($statement);
            echo '<br>';
            echo 'PLACEHOLDERS: ';
            print_r($placeholders);
            echo '<br>';
            echo 'DATATYPES: ';
            print_r($dataTypes);
            echo '<br>';
            die;
        }
        //return $this->_pdo->prepare($statement->queryString, $placeholders);//not working
        if (!is_array($placeholders)) {
            throw new \Phalcon\Db\Exception("Placeholders must be an array");
        }
        foreach ($placeholders as $wildcard => $value) {
            $parameter = '';
            if (is_int($wildcard)) {
                $parameter = $wildcard + 1;
            } else {
                if (is_string($wildcard)) {
                    $parameter = $wildcard;
                } else {
                    throw new \Phalcon\Db\Exception("Invalid bind parameter (#1)");
                }
            }
            if (is_array($dataTypes) && !empty($dataTypes)) {
                if (!isset($dataTypes[$wildcard])) {
                    throw new \Phalcon\Db\Exception("Invalid bind type parameter (#2)");
                }
                $type = $dataTypes[$wildcard];
                /**
                 * The bind type is double so we try to get the double value
                 */
                $castValue;
                if ($type == \Phalcon\Db\Column::BIND_PARAM_DECIMAL) {
                    $castValue = doubleval($value);
                    $type = \Phalcon\Db\Column::BIND_SKIP;
                    // fix PhalconPHP 2.0.4+ ...
                } elseif ($value === 'DEFAULT') {
                    $type = \Phalcon\Db\Column::BIND_SKIP;
                    $castValue = null;
                    // fine fix PhalconPHP 2.0.4+ ...
                } else {
                    $castValue = $value;
                }
                /**
                 * 1024 is ignore the bind type
                 */
                if ($type == \Phalcon\Db\Column::BIND_SKIP) {
                    $statement->bindParam($parameter, $castValue);
                    $statement->bindValue($parameter, $castValue);
                } else {
                    $statement->bindParam($parameter, $castValue, $type);
                    $statement->bindParam($parameter, $castValue, $type);
                    $statement->bindValue($parameter, $castValue, $type);
                }
            } else {
                $statement->bindParam($parameter, $value);        //TODO: works for model, but not pdo - all column with the latest parameter value
                $statement->bindValue($parameter, $value);    //works for pdo , but not model
            }
        }
        //echo PHP_EOL . $statement->queryString . PHP_EOL;
        // echo '<br><br>';print_r($statement).'<br><br>';
        $statement->execute();
        return $statement;
    }

    public function insert($table, array $values, $fields = NULL, $dataTypes = NULL) // 2.x
    {
        $placeholders;
        $insertValues;
        $bindDataTypes;
        $bindType;
        $position;
        $value;
        $escapedTable;
        $joinedValues;
        $escapedFields;
        $field;
        $insertSql;
        if (!is_array($values)) {
            throw new \Phalcon\Db\Exception("The second parameter for insert isn't an Array");
        }
        /**
         * A valid array with more than one element is required
         */
        if (!count($values)) {
            throw new \Phalcon\Db\Exception("Unable to insert into " . $table . " without data");
        }
        $placeholders = array();
        $insertValues = array();
        if (!is_array($dataTypes)) {
            $bindDataTypes = array();
        } else {
            $bindDataTypes = $dataTypes;
        }
        /**
         * Objects are casted using __toString, null values are converted to string "null", everything else is passed as "?"
         */
        //echo PHP_EOL;	var_dump($dataTypes);
        foreach ($values as $position => $value) {
            if (is_object($value)) {
                $placeholders[] = '?'; // (string) $value;
                $insertValues[] = (string)$value;
            } else {
                if ($value === null) { // (0 ==) null is true
                    $placeholders[] = '?';  // "default";
                    $insertValues[] = null; // "default";
                } else {
                    $placeholders[] = "?";
                    $insertValues[] = $value;
                    if (is_array($dataTypes)) {
                        if (!isset($dataTypes[$position])) {
                            throw new \Phalcon\Db\Exception("Incomplete number of bind types");
                        }
                        $bindType = $dataTypes[$position];
                        $bindDataTypes[] = $bindType;
                    }
                }
            }
        }
        // if (defined('DEBUG')) { var_dump($placeholders); die; }
        if (false) { //globals_get("db.escape_identifiers") {
            $escapedTable = $this->escapeIdentifier($table);
        } else {
            $escapedTable = $table;
        }
        /**
         * Build the final SQL INSERT statement
         */
        $joinedValues = join(", ", $placeholders);
        if (is_array($fields)) {
            if (false) {//globals_get("db.escape_identifiers") {
                $escapedFields = array();
                foreach ($fields as $field) {
                    $escapedFields[] = $this->escapeIdentifier($field);
                }
            } else {
                $escapedFields = $fields;
            }
            $insertSql = "INSERT INTO " . $escapedTable . " (" . join(", ", $escapedFields) . ") VALUES (" . $joinedValues . ")";
        } else {
            $insertSql = "INSERT INTO " . $escapedTable . " VALUES (" . $joinedValues . ")";
        }
        $insertSql = 'SET NOCOUNT ON; ' . $insertSql . '; SELECT CAST(SCOPE_IDENTITY() as int) as newid';
        /**
         * Perform the execution via PDO::execute
         */
        $obj = $this->query($insertSql, $insertValues, $bindDataTypes);
        $ret = $obj->fetchAll();
        if ($ret && isset($ret[0]) && isset($ret[0]['newid'])) {
            $this->_lastID = $ret[0]['newid'];
            if ($this->_lastID > 0) {
                return true;
            } else {
                $this->_lastID = null;
                return false;
            }
        } else {
            $this->_lastID = null;
            return false;
        }
    }
    //insert miss parameters, need to do this

    /**
     * Escapes a column/table/schema name
     *
     *<code>
     *    $escapedTable = $connection->escapeIdentifier('robots');
     *    $escapedTable = $connection->escapeIdentifier(array('store', 'robots'));
     *</code>
     *
     * @param string identifier
     * @return string
     */
    public function escapeIdentifier($identifier)
    {
        if (is_array($identifier)) {
            return "[" . $identifier[0] . "].[" . $identifier[1] . "]";
        }
        return "[" . $identifier . "]";
    }

    public function query($sql, $bindParams = null, $bindTypes = null)
    {
        // echo '---- ---- ---- ---- ----<br><br>';
        if (is_string($sql)) {
            //check sql server keyword
            if (!strpos($sql, '[rowcount]')) {
                $sql = str_replace('rowcount', '[rowcount]', $sql);    //sql server keywords
            }
            //case 1. select count(query builder)
            $countString = 'SELECT COUNT(*)';
            if (strpos($sql, $countString)) {
                $sql = str_replace('"', '', $sql);
                return parent::query($sql, $bindParams, $bindTypes);
            }
            //case 2. subquery need alais name (model find)
            $countString = 'SELECT COUNT(*) "numrows" ';
            if (strpos($sql, $countString) !== false) {
                $sql .= ' dt ';
                // $sql = preg_replace('/ORDER\sBY.*\)\ dt/i',') dt',$sql);
                //subquery need TOP
                if (strpos($sql, 'TOP') === false) {
                    if (strpos($sql, 'ORDER') !== false) {
                        $offset = count($countString);
                        $pos = strpos($sql, 'SELECT', $offset) + 7; //'SELECT ';
                        if (stripos($sql, 'SELECT DISTINCT') === false) {
                            $sql = substr($sql, 0, $pos) . 'TOP 100 PERCENT ' . substr($sql, $pos);
                        }
                    }
                }
            }
            // echo $sql."<br><br>";
            //sql server(dblib) does not accept " as escaper
            $sql = str_replace('"', '', $sql);
        }
        // echo $sql.'<br><br>------ --------- ----------';
        return parent::query($sql, $bindParams, $bindTypes);
    }

    public function update($table, $fields, $values, $whereCondition = null, $dataTypes = null)
    {
        $placeholders = array();
        $updateValues = array();
        if (is_array($dataTypes)) {
            $bindDataTypes = array();
        } else {
            $bindDataTypes = $dataTypes;
        }
        /**
         * Objects are casted using __toString, null values are converted to string 'null', everything else is passed as '?'
         */
        foreach ($values as $position => $value) {
            if (!isset($fields[$position])) {
                throw new \Phalcon\Db\Exception("The number of values in the update is not the same as fields");
            }
            $field = $fields[$position];
            if (false) {//globals_get("db.escape_identifiers") {
                $escapedField = $this->escapeIdentifier($field);
            } else {
                $escapedField = $field;
            }
            if (is_object($value)) {
                // $placeholders[] = $escapedField . " = " . $value;
                $placeholders[] = $escapedField . ' = ? ';
                $updateValues[] = (string)$value;
            } else {
                if ($value === null) { // (0 ==) null is true
                    $placeholders[] = $escapedField . " = null";
                    // $placeholders[] = $escapedField . ' = ? ';
                    // $updateValues[] = null;
                } else {
                    $updateValues[] = $value;
                    if (is_array($dataTypes)) {
                        if (!isset($dataTypes[$position])) {
                            throw new \Phalcon\Db\Exception("Incomplete number of bind types");
                        }
                        $bindType = $dataTypes[$position];
                        $bindDataTypes[] = $bindType;
                    }
                    $placeholders[] = $escapedField . " = ?";
                }
            }
        }
        if (false) {//globals_get("db.escape_identifiers") {
            $escapedTable = $this->escapeIdentifier($table);
        } else {
            $escapedTable = $table;
        }
        $setClause = join(", ", $placeholders);
        if ($whereCondition !== null) {
            $updateSql = "UPDATE " . $escapedTable . " SET " . $setClause . " WHERE ";
            /**
             * String conditions are simply appended to the SQL
             */
            if (!is_array($whereCondition)) {
                $updateSql .= $whereCondition;
            } else {
                /**
                 * Array conditions may have bound params and bound types
                 */
                if (!is_array($whereCondition)) {
                    throw new \Phalcon\Db\Exception("Invalid WHERE clause conditions");
                }
                /**
                 * If an index 'conditions' is present it contains string where conditions that are appended to the UPDATE sql
                 */
                if (isset($whereCondition["conditions"])) {
                    $conditions = $whereCondition['conditions'];
                    $updateSql .= $conditions;
                }
                /**
                 * Bound parameters are arbitrary values that are passed by separate
                 */
                if (isset($whereCondition["bind"])) {
                    $whereBind = $whereCondition["bind"];
                    $updateValues = array_merge($updateValues, $whereBind);
                }
                /**
                 * Bind types is how the bound parameters must be casted before be sent to the database system
                 */
                if (isset($whereCondition["bindTypes"])) {
                    $whereTypes = $whereCondition['bindTypes'];
                    $bindDataTypes = array_merge($bindDataTypes, $whereTypes);
                }
            }
        } else {
            $updateSql = "UPDATE " . $escapedTable . " SET " . $setClause;
        }
        /**
         * Perform the update via PDO::execute
         */
        //					echo PHP_EOL . $updateSql;
        //					var_dump($updateValues);
        return $this->execute($updateSql, $updateValues, $bindDataTypes);
    }

    public function lastInsertId($tableName = null, $primaryKey = null)
    {
        // $sql = 'SET NOCOUNT ON; SELECT CAST(SCOPE_IDENTITY() as int) as id';
        // echo __FUNCTION__.': '.$this->instance.'<br>'; die;
        return $this->_lastID;
        // return (int)$this->fetchOne($sql);
    }

    public function delete($table, $whereCondition = null, $placeholders = null, $dataTypes = null)
    {
        $sql;
        $escapedTable;
        if (false) { // globals_get("db.escape_identifiers") {
            $escapedTable = $this->escapeIdentifier($table);
        } else {
            $escapedTable = $table;
        }
        if (!empty($whereCondition)) {
            $sql = "DELETE FROM " . $escapedTable . " WHERE " . $whereCondition;
        } else {
            $sql = "DELETE FROM " . $escapedTable;
        }
        /**
         * Perform the update via PDO::execute
         */
        return $this->execute($sql, $placeholders, $dataTypes);
    }

    /**
     * Lists table indexes
     *
     *<code>
     *    print_r($connection->describeIndexes('robots_parts'));
     *</code>
     *
     * @param    string table
     * @param    string schema
     * @return    Phalcon\Db\Index[]
     */
    public function describeIndexes($table, $schema = null)
    {
        $dialect = $this->_dialect;
        $indexes = array();
        $temps = $this->fetchAll($dialect->describeIndexes($table, $schema), \Phalcon\Db::FETCH_ASSOC);
        foreach ($temps as $index) {
            $keyName = $index['index_id'];
            if (!isset($indexes[$keyName])) {
                $indexes[$keyName] = array();
            }
            //let indexes[keyName][] = index[4];
        }
        $indexObjects = array();
        foreach ($indexes as $name => $indexColumns) {
            /**
             * Every index is abstracted using a Phalcon\Db\Index instance
             */
            $indexObjects[$name] = new \Phalcon\Db\Index($name, $indexColumns);
        }
        return $indexObjects;
    }

    /**
     * Lists table references
     *
     *<code>
     * print_r($connection->describeReferences('robots_parts'));
     *</code>
     *
     * @param    string table
     * @param    string schema
     * @return    Phalcon\Db\Reference[]
     */
    public function describeReferences($table, $schema = null)
    {
        $dialect = $this->_dialect;
        $emptyArr = array();
        $references = array();
        $temps = $this->fetchAll($dialect->describeReferences($table, $schema), \Phalcon\Db::FETCH_NUM);
        foreach ($temps as $reference) {
            $constraintName = $reference[2];
            if (!isset($references[$constraintName])) {
                $references[$constraintName] = array(
                    "referencedSchema" => $reference[3],
                    "referencedTable" => $reference[4],
                    "columns" => $emptyArr,
                    "referencedColumns" => $emptyArr
                );
            }
            //let references[constraintName]["columns"][] = reference[1],
            //	references[constraintName]["referencedColumns"][] = reference[5];
        }
        $referenceObjects = array();
        foreach ($references as $name => $arrayReference) {
            $referenceObjects[$name] = new \Phalcon\Db\Reference($name, array(
                "referencedSchema" => $arrayReference["referencedSchema"],
                "referencedTable" => $arrayReference["referencedTable"],
                "columns" => $arrayReference["columns"],
                "referencedColumns" => $arrayReference["referencedColumns"]
            ));
        }
        return $referenceObjects;
    }

    /**
     * Gets creation options from a table
     *
     *<code>
     * print_r($connection->tableOptions('robots'));
     *</code>
     *
     * @param    string tableName
     * @param    string schemaName
     * @return    array
     */
    public function tableOptions($tableName, $schemaName = null)
    {
        $dialect = $this->_dialect;
        $sql = $dialect->tableOptions($tableName, $schemaName);
        if ($sql) {
            $describe = $this->fetchAll($sql, \Phalcon\DB::FETCH_NUM);
            return $describe[0];
        }
        return array();
    }

    /**
     * Begin a transaction.
     *
     * It is necessary to override the abstract PDO transaction functions here, as
     * the PDO driver for MSSQL does not support transactions.
     */
    public function begin($nesting = false)
    {
        //						$this->execute('SET QUOTED_IDENTIFIER OFF');
        //						$this->execute('SET NOCOUNT OFF');
        $this->execute('BEGIN TRANSACTION;');
        return true;
    }

    /**
     * Commit a transaction.
     *
     * It is necessary to override the abstract PDO transaction functions here, as
     * the PDO driver for MSSQL does not support transactions.
     */
    public function commit($nesting = false)
    {
        $this->execute('COMMIT TRANSACTION');
        return true;
    }

    /**
     * Roll-back a transaction.
     *
     * It is necessary to override the abstract PDO transaction functions here, as
     * the PDO driver for MSSQL does not support transactions.
     */
    public function rollBack($nesting = false)
    {
        $this->execute('ROLLBACK TRANSACTION');
        return true;
    }

    public function getTransactionLevel()
    {
        return (int)$this->fetchOne('SELECT @@TRANCOUNT as level');
    }

    /**
     * Creates a PDO DSN for the adapter from $this->_config settings.
     *
     * @return string
     */
    protected function _dsn()
    {
        // baseline of DSN parts
        $dsn = $this->_config;
        // don't pass the username and password in the DSN
        unset($dsn['username']);
        unset($dsn['password']);
        unset($dsn['options']);
        unset($dsn['persistent']);
        unset($dsn['driver_options']);
        if (isset($dsn['port'])) {
            $seperator = ':';
            if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
                $seperator = ',';
            }
            $dsn['host'] .= $seperator . $dsn['port'];
            unset($dsn['port']);
        }
        // this driver supports multiple DSN prefixes
        // @see http://www.php.net/manual/en/ref.pdo-dblib.connection.php
        if (isset($dsn['pdoType'])) {
            switch (strtolower($dsn['pdoType'])) {
                case 'freetds':
                case 'sybase':
                    $this->_pdoType = 'sybase';
                    break;
                case 'mssql':
                    $this->_pdoType = 'mssql';
                    break;
                case 'dblib':
                default:
                    $this->_pdoType = 'dblib';
                    break;
            }
            unset($dsn['pdoType']);
        }
        // use all remaining parts in the DSN
        foreach ($dsn as $key => $val) {
            $dsn[$key] = "$key=$val";
        }
        $dsn = $this->_pdoType . ':' . implode(';', $dsn);
        return $dsn;
    }
}

?>
