# Phalcon-pdo-mssql
Connecting to Microsoft SQL Server PDO driver With Phalcon Framework v3 https://github.com/phalcon/cphalcon

# Example
```
<?php
require('MssqlAdapter.class.php');

try {
    $connection = new Mssql(array(
        'pdoType' => 'dblib',
        'host' => '',
        'username' => '',
        'password' => '',
        'dbname' => '',
        'port' => '1433'
    ));

    $result = $connection->query("SELECT @@VERSION");
    $result->setFetchMode(Phalcon\Db::FETCH_NUM);

    $robots = $result->fetchAll();
    unset($result);
    var_dump($robots);

} catch (Phalcon\Db\Exception $e) {
    echo 'Phalcon error: ' . $e->getMessage(), PHP_EOL;
} catch (Exception $e) {
    echo $e->getMessage(), PHP_EOL;
}

```
