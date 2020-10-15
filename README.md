# Phalcon-pdo-mssql
Connecting to Microsoft SQL Server PDO driver With Phalcon Framework v3 https://github.com/phalcon/cphalcon/tree/3.4.x

Supports: 'ODBC' - 'DBLIB' - 'SQLSRV'

# Example
```php
<?php
require('MssqlPdo.class.php');

use Phalcon\Db\Exception;

try {
    $connection = new MssqlPdo(array(
        'pdoType' => 'ODBC',
        'driver' => 'ODBC Driver 17 for SQL Server', // Optional - for when 'pdoType' is 'ODBC'
        'host' => 'your_server', // update me
        'failover' => 'your_server_failover', // Optional
        'port' => '1433',
        'username' => 'your_username', // update me
        'password' => 'your_password', // update me
        'dbname' => 'your_database' // update me
    ));

    $arrayResult = $connection->fetchAll("SELECT @@VERSION", Phalcon\Db::FETCH_NUM);
    var_dump($arrayResult);
    die();
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage() . '. Line:' . $e->getLine() . '. File:' . $e->getFile(), PHP_EOL;
}


```
