<?php
require 'vendor/autoload.php';

use Aws\DynamoDb\Marshaler;
use Aws\Sdk;
use League\Csv\Reader;
use League\Csv\Statement;
use \Aws\DynamoDb\Exception\DynamoDbException;

$sdk = new Sdk(
    [
        'region' => 'us-west-2',
        'version' => 'latest',
        'endpoint' => 'http://localhost:8000',
        'credentials' => [
            'key' => 'not-a-real-key',
            'secret' => 'not-a-real-secret',
        ],
    ]
);

$dynamodb = $sdk->createDynamoDb();

$marshaler = new Marshaler();

//
$delete = $dynamodb->deleteTable(['TableName' => 'perceel',]);
try {
    $result = $dynamodb->createTable(
        [
            'TableName' => 'perceel',
            'AttributeDefinitions' => [
                ['AttributeName' => 'perceelId', 'AttributeType' => 'S'],
            ],
            'KeySchema' => [
                ['AttributeName' => 'perceelId', 'KeyType' => 'HASH'],
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 6,
            ],
        ]
    );
    //print_r($result->getPath('TableDescription'));
} catch (DynamoDbException $e) {
    echo "Table already exists \n";
}
echo PHP_EOL . (new DateTime())->format('H:i:s') . PHP_EOL;
$dateStarted = new \DateTime();
$reader = Reader::createFromPath('perceel.csv', 'r');
$recordCount = $reader->count();
$limit = 26;
for ($i = 0, $chunks = 0; $i <= $recordCount; $i += $limit, ++$chunks) {
    $statement = (new Statement())
        ->offset($i)
        ->limit($limit);
    $records = $statement->process($reader);
    $batchRequests = [];

    foreach ($records as $record) {

        $data['PutRequest'] = [
            'Item' => $marshaler->marshalJson(
                json_encode(
                    [
                        'perceelId' => $record[0] ?: "null",
                        'perceeltype' => $record[1] ?: "null",
                        'huisnr' => $record[2] ?: "null",
                        'huisnr_bag_letter' => $record[3] ?: "null",
                        'huisnr_bag_toevoeging' => $record[4] ?: "null",
                        'bag_nummeraanduidingid' => $record[5] ?: "null",
                        'bag_adresseerbaarobjectid' => $record[6] ?: "null",
                        'breedtegraad' => $record[7] ?: "null",
                        'lengtegraad' => $record[8] ?: "null",
                        'rdx' => $record[9] ?: "null",
                        'rdy' => $record[10] ?: "null",
                        'oppervlakte' => $record[11] ?: "null",
                        'gebruiksdoel' => $record[12] ?: "null",
                        'reeksid' => $record[13] ?: "null",

                    ]
                )
            ),
        ];

        array_push($batchRequests, $data);
    }

    try {
        $response = $dynamodb->batchWriteItem(
            ['RequestItems' => ['perceel' => $batchRequests], 'ReturnConsumedCapacity' => 'TOTAL', 'ReturnItemCollectionMetrics' => 'SIZE']
        );
    } catch
    (DynamoDbException $e) {
        echo "Unable to add perceel:\n";
        echo $e->getMessage() . "\n";
        break;
    }

    echo "Chunk {$chunks} has been proceeded\n";
}
$timeEnded = (new DateTime())->diff($dateStarted);

echo PHP_EOL . (new DateTime())->format('H:i:s') . PHP_EOL;
var_dump($timeEnded);



